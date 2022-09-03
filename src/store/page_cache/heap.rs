// use std::sync::Arc;

// use flume::Sender;
// use moka::sync::Cache;
// use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

// use crate::{device::Device, Result};

// pub struct Heap {
//     segments: Cache<u64, SegmentInner>,
//     /// Size of the heap, in segments
//     size: u64,
//     device: Device,
//     writes: Sender<SegmentInner>,
// }

// impl Heap {
//     pub fn get(&self, segment: u64) -> Result<Segment> {
//         todo!()
//     }
// }

// #[derive(Clone)]
// struct SegmentInner(Arc<RwLock<Box<[u8]>>>);

// #[derive(Clone)]
// pub struct Segment {
//     inner: SegmentInner,
//     write: Sender<SegmentInner>,
// }

// impl Segment {
//     pub fn read(&self) -> SegmentReadGuard<'_> {
//         self.0.read()
//     }

//     pub fn write(&self) -> SegmentWriteGuard<'_> {
//         self.0.write()
//     }
// }

// type SegmentReadGuard<'a> = RwLockReadGuard<'a, Box<[u8]>>;
// type SegmentWriteGuard<'a> = RwLockWriteGuard<'a, Box<[u8]>>;
