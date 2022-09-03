use std::fmt::Display;

use tracing::trace;

use super::RawDevice;

pub struct TracingDevice<D> {
    id: String,
    inner: D,
}

impl<D> TracingDevice<D> {
    pub fn new<ID>(inner: D, id: ID) -> Self
    where
        ID: Display,
    {
        let id = id.to_string();
        Self { id, inner }
    }
}

unsafe impl<D> RawDevice for TracingDevice<D>
where
    D: RawDevice,
{
    fn block_size(&self) -> crate::Result<usize> {
        trace!("device.block_size({})", self.id);
        self.inner.block_size()
    }

    fn allocate(&self, size: u64) -> crate::Result<()> {
        trace!("device.allocate({}, {})", self.id, size);
        self.inner.allocate(size)
    }

    unsafe fn read_into(&self, buffer: &mut [u8], offset: u64) -> crate::Result<()> {
        trace!("device.read({}, {}, {})", self.id, buffer.len(), offset);
        unsafe { self.inner.read_into(buffer, offset) }
    }

    unsafe fn write(&self, buf: &[u8], offset: u64) -> crate::Result<()> {
        trace!("device.write({}, {}, {})", self.id, buf.len(), offset);
        unsafe { self.inner.write(buf, offset) }
    }

    unsafe fn append(&self, buf: &[u8]) -> crate::Result<()> {
        trace!("device.append({}, {})", self.id, buf.len());
        unsafe { self.inner.append(buf) }
    }

    fn set_len(&self, size: u64) -> crate::Result<()> {
        trace!("device.set_len({}, {})", self.id, size);
        self.inner.set_len(size)
    }

    fn sync(&self) -> crate::Result<()> {
        trace!("device.sync({})", self.id);
        self.inner.sync()
    }

    fn try_lock_shared(&self) -> crate::Result<()> {
        trace!("device.try_lock_shared({})", self.id);
        self.inner.try_lock_shared()
    }

    fn try_lock_exclusive(&self) -> crate::Result<()> {
        trace!("device.try_lock_exclusive({})", self.id);
        self.inner.try_lock_exclusive()
    }

    unsafe fn unlock_shared(&self) -> crate::Result<()> {
        trace!("device.unlock_shared({})", self.id);
        unsafe { self.inner.unlock_shared() }
    }

    unsafe fn unlock_exclusive(&self) -> crate::Result<()> {
        trace!("device.unlock_exclusive({})", self.id);
        unsafe { self.inner.unlock_exclusive() }
    }

    fn len(&self) -> crate::Result<u64> {
        trace!("device.len({})", self.id);
        self.inner.len()
    }
}
