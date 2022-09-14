use std::{
    num::NonZeroU64,
    ops::{Deref, DerefMut},
};

use nonmax::NonMaxU64;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard};

use crate::{
    buffer_cache::{self, BufId, BufferCache},
    engine::PageAddr,
    types::{HeapAddr, Lsn},
    Result,
};

// mod inner;
// mod raw_heap;
// mod raw_wal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(NonMaxU64);

impl PageId {
    pub fn from_u64(v: u64) -> Option<Self> {
        NonMaxU64::new(v).map(Self)
    }

    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(NonZeroU64);

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
    buffer_cache: BufferCache,
    index: WalIndex,
}

impl Store {
    /// Perform an unsynchronized page read.
    fn get_page(&self, addr: PageAddr) -> Result<buffer_cache::ReadGuard<'_>> {
        self.buffer_cache.get(BufId(addr.to_u64()))
    }

    /// Get the address of the page (at the specified epoch). Note that this does not
    /// guarantee that the page exists, only where to look for it.
    fn page_addr(&self, page: PageId, epoch: Option<Epoch>) -> PageAddr {
        self.index
            .get(page, epoch)
            .map(PageAddr::Wal)
            .unwrap_or(PageAddr::Heap(HeapAddr(page.0)))
    }
}

struct WalIndex {}

impl WalIndex {
    fn get(&self, page: PageId, epoch: Option<Epoch>) -> Option<Lsn> {
        todo!()
    }
}
// impl Store {
//     pub fn read(&self) -> ReadGuard<'_> {
//         todo!()
//     }

//     pub fn write(&self) -> Writer<'_> {
//         todo!()
//     }
// }

// pub struct ReadGuard<'store> {
//     store: &'store Store,
//     checkpoint_guard: RwLockReadGuard<'store, ()>,
// }

// impl<'a> ReadGuard<'a> {
//     pub fn get(&self, page: PageId) -> Result<PageRef<'_>> {
//         todo!()
//     }
// }

// pub struct Writer<'store> {
//     store: &'store Store,
//     checkpoint_guard: RwLockUpgradableReadGuard<'store, ()>,
//     ended: bool,
// }

// impl<'a> Writer<'a> {
//     pub fn get(&self, page: PageId) -> Result<PageRef<'_>> {
//         todo!()
//     }

//     pub fn get_mut(&mut self, page: PageId) -> Result<PageMut<'_>> {
//         todo!()
//     }

//     pub fn alloc(&mut self) -> Result<(PageId, PageMut<'_>)> {
//         todo!()
//     }

//     pub fn free(&mut self, page: PageId) -> Result<()> {
//         todo!()
//     }

//     pub fn commit(self) -> Result<()> {
//         todo!()
//     }

//     pub fn rollback(self) -> Result<()> {
//         todo!()
//     }

//     fn rollback_inner(&mut self) -> Result<()> {
//         todo!()
//     }
// }

// impl<'a> Drop for Writer<'a> {
//     fn drop(&mut self) {
//         if self.ended {
//             return;
//         }
//         if let Err(error) = self.rollback_inner() {
//             error!(?error, "rollback failed during drop call");
//         }
//     }
// }

// pub struct PageRef<'r> {
//     data: buffer_cache::ReadGuard<'r>,
// }

// impl<'r> Deref for PageRef<'r> {
//     type Target = [u8];

//     fn deref(&self) -> &Self::Target {
//         &self.data
//     }
// }

// pub struct PageMut<'w> {
//     data: buffer_cache::WriteGuard<'w>,
// }

// impl<'r> Deref for PageMut<'r> {
//     type Target = [u8];

//     fn deref(&self) -> &Self::Target {
//         &self.data
//     }
// }

// impl<'w> DerefMut for PageMut<'w> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.data
//     }
// }

// struct WriterInner {}

// #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
// struct BlockId(NonZeroU64);
