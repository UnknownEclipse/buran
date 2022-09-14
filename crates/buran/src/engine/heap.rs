use nonmax::NonMaxU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeapAddr(pub NonMaxU64);
