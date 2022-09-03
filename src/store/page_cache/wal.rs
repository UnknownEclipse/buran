use std::{
    cmp,
    collections::BTreeMap,
    hash::Hasher,
    io,
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use moka::sync::Cache;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use parking_lot::{Mutex, MutexGuard, RwLock};
use twox_hash::XxHash64;

use crate::{device::Device, ErrorKind, Result};

use super::{Epoch, PageId};

pub struct Wal {
    device: Device,
    writer: Mutex<WriterInner>,
    index: WalIndex,
    epoch: AtomicU64,
    disk_size: AtomicU64,
    logical_size: AtomicU64,
    cached_segments: Cache<u64, Arc<[u8]>>,
}

impl Wal {
    pub fn clear(&self) -> Result<()> {
        let mut w = self.writer.lock();
        w.buffer.clear();
        self.device.set_len(0)?;
        self.index.clear();
        self.epoch.store(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn reader(&self) -> Reader<'_> {
        let epoch = self.epoch.load(Ordering::Relaxed);
        Reader {
            epoch: Epoch(NonZeroU64::new(epoch).unwrap()),
            wal: self,
        }
    }

    pub fn writer(&self) -> Writer<'_> {
        let inner = self.writer.lock();
        Writer { inner, wal: self }
    }

    fn append(&self, data: &[u8]) -> Result<()> {
        unsafe { self.device.append(data)? };
        self.disk_size
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

pub struct Writer<'wal> {
    inner: MutexGuard<'wal, WriterInner>,
    wal: &'wal Wal,
}

impl<'wal> Writer<'wal> {
    pub fn flush(&mut self) -> Result<()> {
        self.wal.device.sync()?;
        todo!()
    }

    pub fn append_raw(&mut self, data: &[u8]) -> Result<()> {
        todo!()
    }

    fn flush_if_full(&mut self) -> Result<()> {
        let buf = &mut self.inner.buffer;
        if buf.len() != buf.capacity() {
            return Ok(());
        }
        let hash = {
            let mut state = XxHash64::default();
            state.write(&buf[8..]);
            state.finish()
        };
        buf[..8].copy_from_slice(&hash.to_le_bytes());
        unsafe { self.wal.device.append(buf)? };
        self.wal
            .disk_size
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        buf.truncate(8);
        Ok(())
    }
}

impl<'wal> Drop for Writer<'wal> {
    fn drop(&mut self) {
        todo!()
    }
}

pub struct Reader<'wal> {
    wal: &'wal Wal,
    epoch: Epoch,
}

pub struct Records<'wal> {
    wal: &'wal Wal,
    buffer: Arc<Vec<u8>>,
    pos: usize,
    segment: u64,
    segment_size: usize,
    epoch: Option<Epoch>,
}

impl<'wal> Records<'wal> {
    pub fn next(&mut self) -> Result<Option<Record<'_>>> {
        if self.buffer.is_empty() && !self.next_segment()? {
            return Ok(None);
        }
        let slice = &self.buffer[self.pos..];
        let kind = RecordKind::try_from(slice[0]).map_err(|_| ErrorKind::BrokenWalSegment)?;
        self.pos += 1;

        let record = match kind {
            RecordKind::Commit => Record::Commit,
            RecordKind::Rollback => Record::Rollback,
            RecordKind::Free => {
                let mut buf = [0; 8];
                self.read(&mut buf)?;
                let page = PageId::from_bytes(buf).ok_or(ErrorKind::BrokenWalSegment)?;
                Record::Free(page)
            }
            RecordKind::Alloc => {
                let mut buf = [0; 8];
                self.read(&mut buf)?;
                let page = PageId::from_bytes(buf).ok_or(ErrorKind::BrokenWalSegment)?;
                Record::Free(page)
            }
            RecordKind::Write => todo!(),
        };
        Ok(Some(record))
    }

    fn next_segment(&mut self) -> Result<bool> {
        todo!()
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<()> {
        while !buf.is_empty() {
            let remain = self.buffer.len() - self.pos;
            if remain == 0 {
                if !self.next_segment()? {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                continue;
            }
            let n = cmp::min(buf.len(), remain);
            buf[..n].copy_from_slice(&self.buffer[self.pos..self.pos + n]);
            self.pos += n;
        }
        todo!()
    }
}

pub struct WalIndex {
    map: RwLock<BTreeMap<(PageId, Epoch), WalIndexEntry>>,
}

impl WalIndex {
    /// A rollback operation.
    fn remove_epoch(&self, epoch: Epoch) {
        let mut guard = self.map.write();

        // This collect *is* needed to satisfy the borrow checker
        #[allow(clippy::needless_collect)]
        let to_remove: Vec<_> = guard
            .iter()
            .filter_map(|((p, e), _)| epoch.eq(e).then_some(*p))
            .collect();
        to_remove.into_iter().for_each(|p| {
            guard.remove(&(p, epoch));
        })
    }

    pub(super) fn get(&self, (epoch, page): (Epoch, PageId)) -> Option<WalIndexEntry> {
        let range = (page, Epoch(NonZeroU64::new(1).unwrap()))..=(page, epoch);
        let guard = self.map.read();
        guard.range(range).rev().next().map(|(_, entry)| *entry)
    }

    fn insert(&self, (epoch, page): (Epoch, PageId), entry: WalIndexEntry) {
        self.map.write().insert((page, epoch), entry);
    }

    pub fn clear(&self) {
        self.map.write().clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalIndexEntry {
    Alloc,
    Free,
    Write(u64),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive)]
pub enum RecordKind {
    Commit,
    Rollback,
    Free,
    Alloc,
    Write,
}

pub enum Record<'a> {
    Commit,
    Rollback,
    Free(PageId),
    Alloc(PageId),
    Write(PageId, &'a [u8]),
}

impl<'a> Record<'a> {
    pub fn kind(&self) -> RecordKind {
        match self {
            Record::Commit => RecordKind::Commit,
            Record::Rollback => RecordKind::Rollback,
            Record::Free(_) => RecordKind::Free,
            Record::Alloc(_) => RecordKind::Alloc,
            Record::Write(_, _) => RecordKind::Write,
        }
    }
}

struct WriterInner {
    buffer: Vec<u8>,
}
