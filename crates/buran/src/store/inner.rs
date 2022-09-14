use std::sync::atomic::AtomicU16;

use dashmap::DashMap;
use nonmax::NonMaxU64;

use crate::{
    buffer_cache::{BufferCache, ReadGuard},
    Result,
};

use super::{
    raw_heap::RawHeap,
    raw_wal::{Lsn, RawWal},
    Epoch, PageId,
};

pub struct StoreInner {
    buffers: BufferCache,
    epoch: AtomicU16,
    page_table: DashMap<(Epoch, PageId), NonMaxU64>,
}

impl StoreInner {
    pub fn read(&self, epoch: Option<Epoch>, page: PageId) -> Result<ReadGuard<'_>> {
        todo!()
    }
}

struct Adapter {
    wal: RawWal,
    heap: RawHeap,
}

enum PagePtr {
    Wal(Lsn),
    Heap(NonMaxU64),
}

impl PagePtr {
    pub fn from_u64(v: NonMaxU64) -> PagePtr {
        let high_bit_mask = 1 << (u64::BITS - 1);
        if v.get() & high_bit_mask != 0 {
            PagePtr::Heap(v)
        } else {
            PagePtr::Wal(Lsn(v))
        }
    }
}
