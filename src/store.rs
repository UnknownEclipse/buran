use std::{
    io,
    marker::PhantomData,
    num::NonZeroU64,
    sync::atomic::{AtomicU64, AtomicUsize},
};

use moka::sync::Cache;
use tracing::error;
use triomphe::Arc;

pub use self::page_cache::PageId;
use crate::Result;

mod page_cache;
mod wal;

/// # The Storage Layer
///
/// Buran's storage layer exposes simple primitives for reading and writing database
/// pages.
///
/// ## Readers
///
/// Read transactions, or simply 'readers', allow reading from a snapshot of the
/// database at the time of creation.
///
/// ## Writes
///
/// Write transactions, or simply 'writers', allow both reading and writing from
/// the latest version of the database. Writes will not be visible to readers until the
/// write is committed.
///
/// Note that the implementation may only permit one writer at a time, so to avoid
/// deadlocks please only have one write transaction in progress from a single thread.
///
/// ## Fairness
pub struct Store {
    blocks_cache: Cache<(BlockId, Option<Epoch>), Arc<[u8]>>,
    epoch: AtomicU64,
    readers: AtomicUsize,

    mapping_table_head: Option<BlockId>,
}

impl Store {
    pub fn read(&self) -> Reader<'_> {
        todo!()
    }

    pub fn write(&self) -> Writer<'_> {
        todo!()
    }
}

pub struct Reader<'store> {
    store: &'store Store,
}

impl<'a> Reader<'a> {
    pub fn get(&self, page: PageId) -> Result<PageRef<'_>> {
        todo!()
    }
}

pub struct Writer<'store> {
    store: &'store Store,
    ended: bool,
}

impl<'a> Writer<'a> {
    pub fn get(&self, page: PageId) -> Result<PageRef<'_>> {
        todo!()
    }

    pub fn get_mut(&mut self, page: PageId) -> Result<PageMut<'_>> {
        todo!()
    }

    pub fn commit(self) -> Result<()> {
        todo!()
    }

    pub fn rollback(self) -> Result<()> {
        todo!()
    }

    fn rollback_inner(&mut self) -> Result<()> {
        todo!()
    }

    fn end_write_inner(&mut self) -> Result<()> {
        self.ended = true;
        Ok(())
    }
}

impl<'a> Drop for Writer<'a> {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        if let Err(error) = self.rollback_inner() {
            error!(?error, "rollback failed during drop call");
        }
    }
}

pub struct PageRef<'r> {
    data: Arc<[u8]>,
    _p: PhantomData<&'r Reader<'r>>,
}

impl<'r> PageRef<'r> {
    pub fn bytes(&self) -> Result<&[u8]> {
        todo!()
    }
}

pub struct PageMut<'w> {
    id: PageId,
    old: Arc<[u8]>,
    modified: Option<Box<[u8]>>,
    _p: PhantomData<&'w Writer<'w>>,
}

impl<'w> PageMut<'w> {
    pub fn bytes(&self) -> Result<&[u8]> {
        if let Some(modified) = &self.modified {
            Ok(modified)
        } else {
            Ok(&self.old)
        }
    }

    pub fn write(&mut self, data: &[u8], start: usize) -> Result<()> {
        self.bytes_mut()
            .get_mut(start..)
            .and_then(|slice| slice.get_mut(..data.len()))
            .map(|slice| slice.copy_from_slice(data))
            .ok_or(io::ErrorKind::UnexpectedEof)
            .map_err(|e| e.into())
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        self.modified
            .get_or_insert_with(|| Box::from(&self.old[..]))
    }
}

struct WriterInner {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct BlockId(NonZeroU64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Epoch(NonZeroU64);
