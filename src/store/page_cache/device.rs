// use std::{fs::File, num::NonZeroU64, ops::Deref, sync::Arc};

// use rustix::fs::FileExt;

// use crate::util::{
//     aligned_slice::alloc_aligned_slice_zeroed,
//     buffer_cache::{self, BufferCache},
// };

// pub struct Device {
//     cache: BufferCache<Adapter>,
// }

// struct Adapter {
//     file: File,
//     segment_size: usize,
//     block_size: usize,
// }

// impl Adapter {
//     pub fn offset(&self, pg: NonZeroU64) -> u64 {
//         (pg.get() - 1)
//             .checked_mul(self.segment_size as u64)
//             .expect("segment number overflow")
//     }
// }

// impl buffer_cache::Adapter for Adapter {
//     type Buffer = Buffer;

//     fn flush(&self, pg: NonZeroU64, buffer: &Self::Buffer) -> crate::Result<()> {
//         // TODO: Implement more optimal write scheduling (ex. write contiguous
//         // blocks together, debouncing, etc.)
//         self.file.write_all_at(buffer, self.offset(pg))?;
//         Ok(())
//     }

//     fn fault(&self, pg: NonZeroU64) -> crate::Result<Arc<Self::Buffer>> {
//         let offset = self.offset(pg);
//         let mut buf = alloc_aligned_slice_zeroed(self.segment_size, self.block_size);
//         self.file.read_exact_at(&mut buf, offset)?;
//         Ok(Arc::new(Buffer { data: buf }))
//     }
// }

// pub struct Buffer {
//     data: Box<[u8]>,
// }

// impl Deref for Buffer {
//     type Target = [u8];

//     fn deref(&self) -> &Self::Target {
//         &self.data
//     }
// }
