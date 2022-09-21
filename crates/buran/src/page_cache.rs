use std::{
    mem::MaybeUninit,
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use dashmap::DashMap;
use flume::{Receiver, Sender};
use once_cell::sync::OnceCell;
use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use sharded_slab::Slab;
use tracing::{error, warn};

use crate::{
    buffer_cache::{self, Buffer, BufferCache, BufferId},
    types::{DiskAddr, HeapAddr, Lsn, PageId},
    util::FutureCell,
    Error, Result,
};

use self::{
    heap::Heap,
    wal::Wal,
    wal_index::{PageState, WalIndex},
};

mod heap;
mod wal;
mod wal_index;

#[derive(Debug, Error)]
pub enum PageGetError {
    #[error("page not found")]
    NotFound,
    #[error("requested page was broken")]
    Broken,
    #[error(transparent)]
    Other(#[from] crate::Error),
}

pub type PageGetResult<T> = result::Result<T, PageGetError>;

pub struct PageCache {
    buffers: BufferCache,
    wal: Wal,
    wal_index: WalIndex,
    heap: Heap,
    // Use a dashmap for maximum performance. We only hold a mapref for very short
    // critical sections to maximize throughput.
    table: DashMap<DiskAddr, usize>,
    buffers_table: DashMap<BufferId, DiskAddr>,
    /// Isolate disk entries in a separate slab for a few reasons
    /// 1. Ensure we hold a dashmap ref for as little time as possible
    /// 2. Stable indexing for a potential asynchronous ioe ngine
    entries: Slab<DiskEntry>,
    fetches_tx: Sender<usize>,
    fetches_rx: Receiver<usize>,
    error_fuse: OnceCell<Error>,

    pending_flushes: Mutex<usize>,
    pending_flushes_cvar: Condvar,
}

impl PageCache {
    pub fn get(&self, page: PageId, lsn: Option<Lsn>) -> PageGetResult<PageRef<'_>> {
        todo!()
    }

    pub fn get_mut(&self, page: PageId) -> PageGetResult<PageMut<'_>> {
        todo!()
    }

    fn read(&self, page: PageId, lsn: Option<Lsn>) -> PageGetResult<Buffer<'_>> {
        let addr = self.lookup(page, lsn).ok_or(PageGetError::NotFound)?;
        self.read_addr(addr)
    }

    fn lookup(&self, page: PageId, lsn: Option<Lsn>) -> Option<DiskAddr> {
        match self.wal_index.lookup(page, lsn) {
            Some((lsn, PageState::Write(_))) => Some(DiskAddr::Wal(lsn)),
            Some(_) => None,
            None => Some(DiskAddr::Heap(HeapAddr(page.0))),
        }
    }

    fn read_addr(&self, addr: DiskAddr) -> PageGetResult<Buffer<'_>> {
        let entry = self.entry(addr);

        let mut read_guard = entry.buffer_fetch_result.read();

        loop {
            let bufid = *read_guard.wait();
            self.check_for_error()?;

            if let Some(buf) = self.buffers.get(bufid) {
                return Ok(buf);
            }

            let (guard, _) = reset_future_cell(|cur| *cur == bufid, read_guard);
            read_guard = guard;
            self.submit_fetch_request(entry.key(), &entry);
        }
    }

    /// Initiate a fetch request. If one is already in progress for the given index,
    /// nothing is done.
    fn submit_fetch_request(&self, index: usize, entry: &DiskEntry) {
        if entry
            .fetching
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.fetches_tx.send(index).expect("send error");
    }

    fn try_flush_one(&self) -> Result<()> {
        if let Some(flush) = self.buffers.next_flush() {
            let buffer = flush.data();
            let addr = flush.key();
            let addr = DiskAddr::from_u64(addr).unwrap();
            self.write_by_addr(addr, buffer)?;
            flush.finish();
        }
        Ok(())
    }

    fn entry(&self, addr: DiskAddr) -> sharded_slab::Entry<'_, DiskEntry> {
        use dashmap::mapref::entry::Entry::*;

        if let Some(entry) = self.table.get(&addr).and_then(|e| self.entries.get(*e)) {
            return entry;
        }

        match self.table.entry(addr) {
            Occupied(entry) => self.entries.get(*entry.get()).unwrap(),
            Vacant(entry) => {
                let disk_entry = DiskEntry {
                    addr,
                    buffer_fetch_result: Default::default(),
                    fetching: Default::default(),
                };
                let key = self.entries.insert(disk_entry).expect("slab full");
                entry.insert(key);

                let e = self.entries.get(key).unwrap();
                self.submit_fetch_request(key, &e);
                e
            }
        }
    }

    fn read_by_addr(&self, addr: DiskAddr, buffer: &mut [MaybeUninit<u8>]) -> Result<()> {
        todo!()
    }

    fn write_by_addr(&self, addr: DiskAddr, buffer: &[u8]) -> Result<()> {
        todo!()
    }

    /// If a flush failed, the page cache is considered poisoned. (Note that
    /// on many systems a flush error will cause the filesystem to remount itself. This
    /// is the *least* severe action to take).
    fn check_for_error(&self) -> Result<()> {
        match self.error_fuse.get() {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }

    fn start_flush_thread(self: &Arc<Self>) -> Result<()> {
        let this = self.clone();

        thread::spawn(move || {
            let rx = this.buffers.flushes();
            for bufid in rx {
                let buf = match this.buffers.get(bufid) {
                    Some(buf) => buf,
                    None => continue,
                };
                let buf = buf.read();
                let addr = match this.buffers_table.get(&bufid) {
                    Some(v) => *v,
                    None => continue,
                };
                if let Err(e) = this.write_by_addr(addr, &buf) {
                    let _ = this.error_fuse.try_insert(e);
                    return;
                }
            }
        });
        Ok(())
    }

    fn start_fetch_thread(self: &Arc<Self>) {
        let this = self.clone();

        thread::spawn(move || {
            let rx = this.fetches_rx.clone();
            for key in rx {
                let entry = match this.entries.get(key) {
                    Some(e) => e,
                    None => todo!(),
                };
                let guard = entry.buffer_fetch_result.read();
                let mut slot = this
                    .buffers
                    .slot(entry.addr.to_u64())
                    .expect("failed to acquire empty buffer");

                if let Err(err) = this.read_by_addr(entry.addr, &mut slot) {
                    error!(
                        "error occurred while reading page: {:?}, {:?}",
                        err, entry.addr
                    );
                    let _ = this.error_fuse.try_insert(err);
                    let _ = guard.try_insert(BufferId::DANGLING);
                    continue;
                }

                let buf = unsafe { slot.assume_init() };
                let id = buf.id();
                if guard.try_insert(id).is_err() {
                    warn!("buffer fetch cell not empty");
                }
            }
        });
    }
}

pub struct PageRef<'a> {
    buffer: buffer_cache::ReadGuard<'a>,
}

pub struct PageMut<'a> {
    buffer: buffer_cache::WriteGuard<'a>,
}

#[derive(Debug)]
struct DiskEntry {
    addr: DiskAddr,
    buffer_fetch_result: RwLock<FutureCell<BufferId>>,
    fetching: AtomicBool,
}

fn reset_future_cell<T>(
    validate: impl FnOnce(&T) -> bool,
    guard: RwLockReadGuard<FutureCell<T>>,
) -> (RwLockReadGuard<FutureCell<T>>, bool) {
    let lock = RwLockReadGuard::rwlock(&guard);
    drop(guard);
    let mut guard = lock.write();

    let mut was_reset = false;
    if let Some(current) = guard.get() {
        if validate(current) {
            *guard = FutureCell::new();
            was_reset = true;
        }
    }
    (RwLockWriteGuard::downgrade(guard), was_reset)
}
