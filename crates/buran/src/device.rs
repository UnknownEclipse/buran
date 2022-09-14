use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::Result;

use self::{mem::MemDevice, tracing::TracingDevice};

mod debug;
mod file;
mod mem;
mod tracing;

/// # Safety
pub unsafe trait RawDevice {
    fn block_size(&self) -> Result<usize>;
    fn allocate(&self, size: u64) -> Result<()>;
    unsafe fn read_into(&self, buffer: &mut [u8], offset: u64) -> Result<()>;
    unsafe fn write(&self, buf: &[u8], offset: u64) -> Result<()>;
    unsafe fn append(&self, buf: &[u8]) -> Result<()>;
    fn set_len(&self, size: u64) -> Result<()>;
    fn sync(&self) -> Result<()>;
    fn try_lock_shared(&self) -> Result<()>;
    fn try_lock_exclusive(&self) -> Result<()>;
    unsafe fn unlock_shared(&self) -> Result<()>;
    unsafe fn unlock_exclusive(&self) -> Result<()>;
    fn len(&self) -> Result<u64>;
}

pub type Device = Arc<dyn RawDevice + Send + Sync>;

pub fn memory() -> Device {
    static MEM_ID: AtomicUsize = AtomicUsize::new(0);
    let mem = MemDevice::new();
    let id = format!("mem.{}", MEM_ID.fetch_add(1, Ordering::Relaxed));
    let tracing = TracingDevice::new(mem, id);
    Arc::new(tracing)
}
