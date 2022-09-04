// use std::{
//     mem,
//     num::NonZeroU64,
//     ops::{Deref, DerefMut},
//     sync::Arc,
// };

// use parking_lot::Mutex;

// use crate::{
//     sys,
//     util::{
//         aligned_slice::alloc_aligned_slice_zeroed,
//         buffer_cache::{self, BufferCache},
//     },
//     Result,
// };

// pub struct Wal {
//     inner: Arc<Inner>,
//     buffer_cache: BufferCache<Adapter>,
// }

// pub struct Buffer {
//     data: Box<[u8]>,
//     inner: Arc<Inner>,
// }

// impl Deref for Buffer {
//     type Target = [u8];

//     fn deref(&self) -> &Self::Target {
//         &self.data
//     }
// }

// impl DerefMut for Buffer {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.data
//     }
// }

// impl Drop for Buffer {
//     fn drop(&mut self) {
//         let data = mem::take(&mut self.data);
//         self.inner.unused_buffers.lock().push(data);
//     }
// }

// struct Inner {
//     file: sys::File,
//     segment_size: usize,
//     block_size: usize,
//     unused_buffers: Mutex<Vec<Box<[u8]>>>,
// }

// impl Inner {
//     pub fn get_buffer(&self) -> Box<[u8]> {
//         self.unused_buffers
//             .lock()
//             .pop()
//             .unwrap_or_else(|| alloc_aligned_slice_zeroed(self.segment_size, self.block_size))
//     }
// }

// struct Adapter {
//     inner: Arc<Inner>,
// }

// impl buffer_cache::Adapter for Adapter {
//     type Buffer = Buffer;

//     fn flush(&self, pg: NonZeroU64, buffer: &Self::Buffer) -> Result<()> {
//         let offset = (pg.get() - 1) * self.inner.segment_size as u64;
//         self.inner.file.write_all_at(buffer, offset)?;
//         Ok(())
//     }

//     fn fault(&self, pg: NonZeroU64) -> Result<Arc<Self::Buffer>> {
//         let offset = (pg.get() - 1) * self.inner.segment_size as u64;
//         let mut buf = self.inner.get_buffer();
//         self.inner.file.read_exact_at(&mut buf, offset)?;
//         Ok(Arc::new(Buffer {
//             data: buf,
//             inner: self.inner.clone(),
//         }))
//     }
// }
