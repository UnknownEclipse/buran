use nonmax::NonMaxU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub NonMaxU64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub NonMaxU64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeapAddr(pub NonMaxU64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiskAddr {
    Wal(Lsn),
    Heap(HeapAddr),
}

impl DiskAddr {
    pub fn from_u64(v: u64) -> Option<Self> {
        if v & (1 << 63) != 0 {
            NonMaxU64::new(v & !(1 << 63)).map(Lsn).map(Self::Wal)
        } else {
            NonMaxU64::new(v).map(HeapAddr).map(Self::Heap)
        }
    }

    pub fn to_u64(self) -> u64 {
        match self {
            DiskAddr::Wal(lsn) => lsn.0.get() | (1 << 63),
            DiskAddr::Heap(addr) => addr.0.get(),
        }
    }
}
