use std::{
    mem::MaybeUninit,
    ops::Sub,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{sys, ErrorKind, Result};

use super::PageId;

pub struct RawHeap {
    file: sys::File,
    page_count: AtomicU64,
    page_size: usize,
}

impl RawHeap {
    /// Ensure disk space is allocated for at least `count` pages.
    pub fn allocate(&self, count: u64) -> Result<()> {
        let size = count
            .checked_mul(self.page_size as u64)
            .expect("page count overflow");
        self.file.allocate(size)?;
        Ok(())
    }

    pub fn read_pages(&self, start: PageId, buf: &mut [MaybeUninit<u8>]) -> Result<()> {
        let count = self.page_count.load(Ordering::Relaxed);
        let pgno = start.as_u64().get().sub(1);

        if count <= pgno {
            return Err(ErrorKind::PageNotFound.into());
        }

        let offset = pgno * self.page_size as u64;

        if buf.len() % self.page_size != 0 {
            panic!("buf size not a multiple of page size");
        }

        self.file.read_exact_at(buf, offset)?;
        Ok(())
    }

    pub fn write_pages(&self, start: PageId, buf: &[u8]) -> Result<()> {
        let pgno = start.as_u64().get().sub(1);

        let offset = pgno
            .checked_mul(self.page_size as u64)
            .ok_or(ErrorKind::PageNotFound)?;

        if buf.len() % self.page_size != 0 {
            panic!("buf size not a multiple of page size");
        }

        self.file.write_all_at(buf, offset)?;
        Ok(())
    }
}
