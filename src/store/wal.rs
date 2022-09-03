use std::{
    mem,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::{
    util::buffer_cache::{self, BufferCache},
    Result,
};

pub struct Wal {
    inner: Arc<Inner>,
    buffer_cache: BufferCache<Adapter>,
}

pub struct Buffer {
    data: Box<[u8]>,
    inner: Arc<Inner>,
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let data = mem::take(&mut self.data);
        self.inner.unused_buffers.lock().push(data);
    }
}

struct Inner {
    unused_buffers: Mutex<Vec<Box<[u8]>>>,
}

struct Adapter {
    inner: Arc<Inner>,
}

impl buffer_cache::Adapter for Adapter {
    type Buffer = Buffer;

    fn flush(&self, pg: NonZeroU64, buffer: &Self::Buffer) -> Result<()> {
        todo!()
    }

    fn fault(&self, pg: NonZeroU64) -> Result<Arc<Self::Buffer>> {
        todo!()
    }
}
