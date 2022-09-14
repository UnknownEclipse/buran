use nonmax::NonMaxU64;

use crate::types::{HeapAddr, Lsn};

mod heap;
mod wal;

pub struct Engine {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PageAddr {
    Wal(Lsn),
    Heap(HeapAddr),
}

impl PageAddr {
    pub fn from_u64(v: u64) -> Option<Self> {
        if v & ON_HEAP_BIT != 0 {
            NonMaxU64::new(v & !ON_HEAP_BIT)
                .map(HeapAddr)
                .map(PageAddr::Heap)
        } else {
            NonMaxU64::new(v).map(Lsn).map(PageAddr::Wal)
        }
    }

    pub fn to_u64(&self) -> u64 {
        match self {
            PageAddr::Wal(lsn) => lsn.0.get(),
            PageAddr::Heap(heap) => heap.0.get() | ON_HEAP_BIT,
        }
    }
}

const ON_HEAP_BIT: u64 = 1 << (u64::BITS - 1);
