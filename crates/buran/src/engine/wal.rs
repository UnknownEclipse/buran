use nonmax::NonMaxU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub NonMaxU64);

pub struct Wal {}
