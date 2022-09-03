use std::cmp;

use super::RawDevice;
use crate::Result;

pub struct DebugDevice<D> {
    block_size: usize,
    inner: D,
}

impl<D> DebugDevice<D>
where
    D: RawDevice,
{
    pub fn new(inner: D) -> Result<Self> {
        let true_block_size = inner.block_size()?;

        // Fake a larger block size to make errors more likely to be caught.
        let other = true_block_size * 4096;
        let block_size = cmp::min(true_block_size, other);

        Ok(Self { block_size, inner })
    }
}

unsafe impl<D> RawDevice for DebugDevice<D>
where
    D: RawDevice,
{
    fn block_size(&self) -> Result<usize> {
        self.inner.block_size()
    }

    fn allocate(&self, size: u64) -> Result<()> {
        self.inner.allocate(size)
    }

    unsafe fn read_into(&self, buffer: &mut [u8], offset: u64) -> Result<()> {
        ensure_aligned_ptr(buffer.as_ptr(), self.block_size);
        ensure_aligned(buffer.len(), self.block_size);
        unsafe { self.inner.read_into(buffer, offset) }
    }

    unsafe fn write(&self, buf: &[u8], offset: u64) -> Result<()> {
        ensure_aligned_ptr(buf.as_ptr(), self.block_size);
        ensure_aligned(buf.len(), self.block_size);
        unsafe { self.inner.write(buf, offset) }
    }

    unsafe fn append(&self, buf: &[u8]) -> Result<()> {
        ensure_aligned_ptr(buf.as_ptr(), self.block_size);
        ensure_aligned(buf.len(), self.block_size);
        unsafe { self.inner.append(buf) }
    }

    fn set_len(&self, size: u64) -> Result<()> {
        self.inner.set_len(size)
    }

    fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    fn try_lock_shared(&self) -> Result<()> {
        self.inner.try_lock_shared()
    }

    fn try_lock_exclusive(&self) -> Result<()> {
        self.inner.try_lock_exclusive()
    }

    unsafe fn unlock_shared(&self) -> Result<()> {
        unsafe { self.inner.unlock_shared() }
    }

    unsafe fn unlock_exclusive(&self) -> Result<()> {
        unsafe { self.inner.unlock_exclusive() }
    }

    fn len(&self) -> crate::Result<u64> {
        self.inner.len()
    }
}

fn ensure_aligned_ptr(ptr: *const u8, align: usize) {
    ensure_aligned(ptr as usize, align);
}

#[track_caller]
fn ensure_aligned(size: usize, align: usize) {
    if size % align != 0 {
        panic!("invalid alignment")
    }
}
