use std::{
    mem::MaybeUninit,
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc,
    },
};

use crate::{sys::File, Result};

use super::{
    buffer_cache::{Adapter, Buffer, BufferCache},
    buffer_slice::BufferSlice,
};

pub struct BufferedFile {
    buffer_cache: BufferCache,
    inner: Arc<Inner>,
}

impl BufferedFile {
    pub fn read(&self, size: usize) -> Result<BufferSlice> {
        todo!()
    }
}

struct Inner {
    file: File,
    count: AtomicU64,               // The number of segments
    last_segment_size: AtomicUsize, // The size of the last segment, which may not be entirely full
}

impl Adapter for Arc<Inner> {
    fn flush(&self, pg: NonZeroU64, buffer: &Buffer) -> Result<()> {
        todo!()
    }

    fn fault(&self, pg: NonZeroU64, buf: &mut [u8]) -> Result<()> {
        todo!()
    }

    unsafe fn fault_uninit(&self, pg: NonZeroU64, buf: &mut [MaybeUninit<u8>]) -> Result<()> {
        todo!()
    }
}
