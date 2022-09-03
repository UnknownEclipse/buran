//! # The Storage Layer
//!
//! ## The Mapping Table
//! ## The B-Tree
use std::{
    num::NonZeroU64,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use moka::sync::Cache;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

use crate::Result;

use self::wal::Wal;

mod device;
mod heap;
mod io;
mod raw_wal;
mod wal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(NonZeroU64);

impl PageId {
    #[inline]
    pub fn from_u64(v: u64) -> Option<Self> {
        NonZeroU64::new(v).map(Self)
    }

    #[inline]
    pub fn from_bytes(bytes: [u8; 8]) -> Option<Self> {
        Self::from_u64(u64::from_le_bytes(bytes))
    }

    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0.get()
    }

    #[inline]
    pub fn to_bytes(self) -> [u8; 8] {
        self.as_u64().to_le_bytes()
    }
}

pub struct PageCache {
    cached: Cache<(PageId, Option<Epoch>), Arc<[u8]>>,

    readers: AtomicUsize,
    writer: Mutex<Vec<u8>>,
    checkpoint_lock: RwLock<()>,
    checkpoint_requested: AtomicBool,

    wal_size: AtomicU64,
    last_commit_lsn: AtomicU64,

    wal: Wal,
    // heap: Device,
}

pub struct CheckpointGuard<'a> {
    inner: RwLockReadGuard<'a, ()>,
}

pub struct ReadGuard<'a> {
    page_cache: &'a PageCache,
    _checkpoint_guard: CheckpointGuard<'a>,
}

pub struct WriteGuard<'a> {
    page_cache: &'a PageCache,
    wal_writer: wal::Writer<'a>,
    checkpoint_guard: CheckpointGuard<'a>,
}

impl<'a> WriteGuard<'a> {}

impl PageCache {
    pub fn reader(&self) -> ReadGuard<'_> {
        let checkpoint_guard = self.checkpoint_guard();
        let lsn = self.wal_size.load(Ordering::Relaxed);
        todo!()
    }

    pub fn writer(&self) -> WriteGuard<'_> {
        let checkpoint_guard = self.checkpoint_guard();
        let wal_writer = self.wal.writer();

        WriteGuard {
            checkpoint_guard,
            wal_writer,
            page_cache: self,
        }
    }

    fn checkpoint_guard(&self) -> CheckpointGuard<'_> {
        CheckpointGuard {
            inner: self.checkpoint_lock.read(),
        }
    }

    pub fn try_checkpoint(&self) -> Result<bool> {
        let guard = match self.checkpoint_lock.try_write() {
            Some(g) => g,
            None => return Ok(false),
        };
        todo!()
    }
}

impl<'a> WriteGuard<'a> {
    pub fn flush(&mut self) -> Result<()> {
        self.wal_writer.flush()
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        todo!()
    }
}

pub enum Record<'a> {
    Commit,
    Rollback,
    Pad(u64),
    Free(PageId),
    Alloc(PageId),
    Write { page: PageId, data: &'a [u8] },
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive)]
pub enum RecordKind {
    Commit,
    Rollback,
    Pad,
    Free,
    Alloc,
    Write,
}

impl<'a> Record<'a> {
    fn write(&self, mut write: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        match self {
            Record::Commit => write(&[RecordKind::Commit.into()]),
            Record::Rollback => write(&[RecordKind::Rollback.into()]),
            Record::Pad(n) => {
                write(&[RecordKind::Pad.into()])?;
                for _ in 0..*n {
                    write(&[0])?;
                }
                Ok(())
            }
            Record::Free(page) => {
                write(&[RecordKind::Free.into()])?;
                write(&page.to_bytes())
            }
            Record::Alloc(page) => {
                write(&[RecordKind::Alloc.into()])?;
                write(&page.to_bytes())
            }
            Record::Write { page, data } => {
                write(&[RecordKind::Write.into()])?;
                write(&page.to_bytes())?;

                let mut buf = [0; 9];
                let mut w = &mut buf[..];
                let n = leb128::write::unsigned(&mut w, data.len() as u64).unwrap();

                write(&buf[..n])?;
                write(data)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Epoch(NonZeroU64);
