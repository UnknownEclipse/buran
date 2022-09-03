use parking_lot::{RwLock, RwLockReadGuard};

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

pub struct Wal {
    checkpoint_lock: RwLock<()>,
}

pub struct Reader<'a> {
    _checkpoint_guard: RwLockReadGuard<'a, ()>,
}

pub struct Writer<'a> {
    _checkpoint_guard: RwLockReadGuard<'a, ()>,
}

impl Wal {
    pub fn reader(&self) -> Reader<'_> {
        Reader {
            _checkpoint_guard: self.checkpoint_lock.read_recursive(),
        }
    }

    pub fn writer(&self) -> Writer<'_> {
        Writer {
            _checkpoint_guard: self.checkpoint_lock.read_recursive(),
        }
    }
}

impl<'a> Reader<'a> {
    pub fn read_into(&self, buf: &mut [u8], lsn: Lsn) -> Result<()> {
        todo!()
    }
}

impl<'a> Writer<'a> {
    pub fn write(&self, data: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
