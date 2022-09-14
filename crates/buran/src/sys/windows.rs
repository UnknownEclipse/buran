use std::{fs, io, mem::MaybeUninit};

use crate::Result;

pub struct File {
    file: fs::File,
}

impl File {
    pub fn from_std_file(file: fs::File) -> Result<Self> {
        Ok(Self { file })
    }

    pub fn block_size(&self) -> io::Result<u64> {
        // let path = GetFinalPathNameByHandle(self.file)?;
        // fs2::allocation_granularity(path)
    }

    pub fn allocate(&self, size: u64) -> io::Result<()> {
        fs2::FileExt::allocate(&self.file, size)
    }

    pub fn read_at(&self, buffer: &mut [MaybeUninit<u8>], offset: u64) -> io::Result<usize> {
        let buffer = unsafe { &mut *(buffer as *mut [_] as *mut [u8]) };
        self.file.seek_read(buffer, offset)
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.file.seek_write(buf, offset)
    }

    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn try_lock_shared(&self) -> io::Result<bool> {
        todo!()
    }

    pub fn try_lock_exclusive(&self) -> io::Result<bool> {
        todo!()
    }

    pub fn unlock_shared(&self) -> io::Result<()> {
        todo!()
    }

    pub fn unlock_exclusive(&self) -> io::Result<()> {
        todo!()
    }

    pub fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}
