use std::io;

use parking_lot::{lock_api, RawRwLock, RwLock};

use crate::{Error, ErrorKind, Result};

use super::RawDevice;

pub struct MemDevice {
    inner: RwLock<Vec<u8>>,
    cc: RawRwLock,
}

impl MemDevice {
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
            cc: lock_api::RawRwLock::INIT,
        }
    }
}

unsafe impl RawDevice for MemDevice {
    fn block_size(&self) -> Result<usize> {
        Ok(1)
    }

    fn allocate(&self, size: u64) -> Result<()> {
        let size = size.try_into().map_err(|_| oom())?;
        self.inner.write().try_reserve(size).map_err(|_| oom())
    }

    unsafe fn read_into(&self, buffer: &mut [u8], offset: u64) -> Result<()> {
        offset
            .try_into()
            .ok()
            .and_then(|start| {
                let end = buffer.len().checked_add(start)?;
                let guard = self.inner.read();
                let slice = guard.get(start..end)?;
                buffer.copy_from_slice(slice);
                Some(())
            })
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof).into())
    }

    unsafe fn write(&self, buf: &[u8], offset: u64) -> Result<()> {
        offset
            .try_into()
            .ok()
            .and_then(|start| {
                let end = buf.len().checked_add(start)?;
                let mut guard = self.inner.write();
                let dst = guard.get_mut(start..end)?;
                dst.copy_from_slice(buf);
                Some(())
            })
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof).into())
    }

    unsafe fn append(&self, buf: &[u8]) -> Result<()> {
        self.inner.write().extend_from_slice(buf);
        Ok(())
    }

    fn set_len(&self, size: u64) -> Result<()> {
        let size = size
            .try_into()
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        self.inner.write().resize(size, 0);
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }

    fn try_lock_shared(&self) -> Result<()> {
        if lock_api::RawRwLock::try_lock_shared(&self.cc) {
            Ok(())
        } else {
            Err(ErrorKind::LockContended.into())
        }
    }

    fn try_lock_exclusive(&self) -> Result<()> {
        if lock_api::RawRwLock::try_lock_exclusive(&self.cc) {
            Ok(())
        } else {
            Err(ErrorKind::LockContended.into())
        }
    }

    unsafe fn unlock_shared(&self) -> Result<()> {
        unsafe { lock_api::RawRwLock::unlock_shared(&self.cc) };
        Ok(())
    }

    unsafe fn unlock_exclusive(&self) -> Result<()> {
        unsafe { lock_api::RawRwLock::unlock_exclusive(&self.cc) };
        Ok(())
    }

    fn len(&self) -> crate::Result<u64> {
        Ok(self.inner.read().len() as u64)
    }
}

fn oom() -> Error {
    Error::from(io::Error::from(io::ErrorKind::OutOfMemory))
}
