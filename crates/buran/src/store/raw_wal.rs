use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    convert::TryInto,
    io::{self, BufReader, BufWriter, Read, Write},
    mem::{self, MaybeUninit},
    num::NonZeroU64,
    result,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use nonmax::NonMaxU64;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    sys::{self, StdIo, Tracked},
    types::Lsn,
    util::{assume_init_slice_mut, slice_as_uninit},
    Result,
};

use super::{Epoch, PageId};

pub struct RawWal {
    file: Arc<sys::File>,
    disk_size: AtomicU64,
    lsn: AtomicU64,
    appender_buffer_size: usize,
    appender: RwLock<Tracked<BufWriter<StdIo<'static>>>>,
    index: WalIndex,
}

impl RawWal {
    pub fn page(&self, epoch: Epoch, page: PageId) -> Result<Option<PageUpdate>> {
        Ok(self.index.page(Some(epoch), page))
    }

    pub fn write(&self, record: &Record<'_>) -> Result<Lsn> {
        let mut w = self.appender.write();
        let lsn = self.lsn();

        match record {
            Record::Commit => {
                let buf = [RecordKind::Commit.into()];
                w.write_all(&buf)?;
            }
            Record::Rollback => {
                let buf = [RecordKind::Rollback.into()];
                w.write_all(&buf)?;
            }
            Record::PageWrite { page, data } => {
                let buf = [RecordKind::PageWrite.into()];
                w.write_all(&buf)?;
                w.write_all(&page.0.get().to_le_bytes())?;
                leb128::write::unsigned(&mut *w, data.len() as u64)?;
                w.write_all(data)?;
            }
        }

        w.debug_ensure_position()?;

        self.lsn.store(w.position(), Ordering::Relaxed);
        self.index.append(lsn, record);
        Ok(lsn)
    }

    pub fn read<'a>(&self, lsn: Lsn, buffer: &'a mut [MaybeUninit<u8>]) -> Result<&'a mut [u8]> {
        let start = lsn.0.get();
        let end = start + buffer.len() as u64;

        // If a region of the requested data is buffered, we take a lock and split the read,
        // otherwise avoid taking the lock and do a single read_at() call.
        if self.lsn().0.get() < (end + self.appender_buffer_size as u64) {
            // Note: It is possible for a race condition to occur here that causes the
            // buffer to be flushed, however this is harmless as we take the read guard
            // before doing any of the calculations, so the only cost is an unnecessary
            // lock.
            let guard = self.appender.read();
            let hot = guard.get_ref().buffer();
            let disk_size = self.lsn().0.get() - hot.len() as u64;

            let on_disk = cmp::min(disk_size, end)
                .checked_sub(start)
                .unwrap()
                .try_into()
                .unwrap();

            self.file.read_exact_at(&mut buffer[..on_disk], start)?;

            let in_memory = buffer.len() - on_disk;
            buffer[on_disk..].copy_from_slice(slice_as_uninit(&hot[..in_memory]));

            Ok(unsafe { assume_init_slice_mut(buffer) })
        } else {
            self.file.read_exact_at(buffer, start).map_err(Into::into)
        }
    }

    fn lsn(&self) -> Lsn {
        NonMaxU64::new(self.lsn.load(Ordering::Relaxed))
            .map(Lsn)
            .unwrap()
    }
}

struct RawWalIter<'a> {
    force_done: bool,
    inner: Tracked<BufReader<StdIo<'a>>>,
    temp_buffer: Vec<u8>,
}

impl<'a> RawWalIter<'a> {
    pub fn next(&mut self) -> result::Result<Option<Record<'_>>, WalRecordError> {
        if self.force_done {
            return Ok(None);
        }
        // Instead of setting this if we hit an error, and greatly complicating control
        // flow, set it here and unset it before a successful return.
        self.force_done = true;

        let mut kind = [0];
        match self.inner.read_exact(&mut kind) {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err.into()),
        }
        let kind = kind[0].try_into().map_err(|_| WalRecordError::Broken)?;

        match kind {
            RecordKind::Commit => {
                self.force_done = false;
                Ok(Some(Record::Commit))
            }
            RecordKind::Rollback => {
                self.force_done = false;
                Ok(Some(Record::Rollback))
            }
            RecordKind::PageWrite => {
                let len = leb128::read::unsigned(&mut self.inner)?;
                let len = len.try_into().map_err(|_| WalRecordError::Broken)?;

                let mut page = [0; 8];
                self.inner.read_exact(&mut page)?;
                let page = NonZeroU64::new(u64::from_le_bytes(page))
                    .map(PageId)
                    .ok_or(WalRecordError::Broken)?;

                self.temp_buffer.resize(len, 0);
                self.inner.read_exact(&mut self.temp_buffer)?;
                self.force_done = false;
                let data = &self.temp_buffer;

                Ok(Some(Record::PageWrite { page, data }))
            }
        }
    }

    pub fn lsn(&self) -> Lsn {
        NonMaxU64::new(self.inner.position()).map(Lsn).unwrap()
    }
}

/// Record wal info in memory so that it may be quickly accessed.
struct WalIndex {
    lock: RwLock<BTreeMap<Epoch, EpochInfo>>,
    current: RwLock<(u64, EpochInfo)>,
}

impl WalIndex {
    pub fn current_epoch(&self) -> Epoch {
        NonZeroU64::new(self.current.read().0).map(Epoch).unwrap()
    }

    pub fn append(&self, lsn: Lsn, record: &Record<'_>) {
        let mut current = self.current.write();

        match record {
            Record::Commit => {
                let epoch = current.0;
                let info = mem::take(&mut current.1);
                current.0 += 1;

                let mut guard = self.lock.write();
                let epoch = NonZeroU64::new(epoch).map(Epoch).unwrap();
                guard.insert(epoch, info);
            }
            Record::Rollback => {
                current.1 = Default::default();
                current.0 += 1;
            }

            Record::PageWrite { page, data } => {
                let mut sink = Tracked::new(io::sink());

                let size = data.len();
                leb128::write::unsigned(&mut sink, size as u64)
                    .expect("no errors should be possible");

                let mut offset = lsn.0.get();
                offset += 1;
                offset += mem::size_of::<PageId>() as u64;
                offset += sink.position();
                let lsn = NonMaxU64::new(offset).map(Lsn).unwrap();

                let update = PageUpdate::Write(lsn, size);
                self.current.write().1.pages.insert(*page, update);
            }
        }
    }

    pub fn page(&self, mut epoch: Option<Epoch>, page: PageId) -> Option<PageUpdate> {
        let mut current = None;
        if epoch.is_none() {
            let current = current.insert(self.current.read());
            if let Some(update) = current.1.pages.get(&page).copied() {
                return Some(update);
            }
            epoch = Some(Epoch(NonZeroU64::new(current.0).unwrap()));
        }

        let epoch = epoch.unwrap();

        let guard = self.lock.read();
        guard
            .range(..epoch)
            .rev()
            .next()
            .and_then(|(_, info)| info.pages.get(&page))
            .copied()
    }
}
// struct StdRead<'a>(&'a sys::File);

// impl<'a> Read for StdRead<'a> {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         let buf = unsafe { &mut *(buf as *mut [_] as *mut [_]) };
//         self.0.read(buf)
//     }
// }

// struct CountRead<R>(R, u64);

// impl<R> Read for CountRead<R>
// where
//     R: Read,
// {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         let n = self.0.read(buf)?;
//         self.1 += n as u64;
//         Ok(n)
//     }
// }

// impl<R> BufRead for CountRead<R>
// where
//     R: BufRead,
// {
//     fn fill_buf(&mut self) -> io::Result<&[u8]> {
//         self.0.fill_buf()
//     }

//     fn consume(&mut self, amt: usize) {
//         self.0.consume(amt)
//     }
// }

pub enum Record<'a> {
    Commit,
    Rollback,
    PageWrite { page: PageId, data: &'a [u8] },
}

#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, IntoPrimitive, TryFromPrimitive,
)]
pub enum RecordKind {
    Commit,
    Rollback,
    PageWrite,
}

#[derive(Debug, Error)]
pub enum WalRecordError {
    #[error("incomplete wal record")]
    Incomplete,
    #[error("broken wal record")]
    Broken,
    #[error(transparent)]
    IoError(io::Error),
}

impl From<io::Error> for WalRecordError {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            WalRecordError::Incomplete
        } else {
            WalRecordError::IoError(err)
        }
    }
}

impl From<leb128::read::Error> for WalRecordError {
    fn from(err: leb128::read::Error) -> Self {
        match err {
            leb128::read::Error::IoError(err) => err.into(),
            leb128::read::Error::Overflow => WalRecordError::Broken,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordInfo {
    Commit,
    Rollback,
    PageWrite { page: PageId, data_size: usize },
}

#[derive(Default)]
struct EpochInfo {
    pages: HashMap<PageId, PageUpdate>,
}

#[derive(Debug, Clone, Copy)]
pub enum PageUpdate {
    Write(Lsn, usize),
}
