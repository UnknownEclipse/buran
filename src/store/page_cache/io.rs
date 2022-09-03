use std::{
    io, mem,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use either::Either;
use moka::sync::Cache;
use parking_lot::{Mutex, MutexGuard};

use crate::{
    device::Device,
    util::aligned_slice::{alloc_aligned_slice_zeroed, clone_slice_aligned},
    Error, Result,
};

/// The primary I/O abstraction backing Buran's storage layer.
///
/// `File`s are subdivided into segments. Each segment is a multiple of the backing
/// device's block size.
pub struct File {
    cached: Cache<u64, Arc<SegmentInner>>,
    segment_count: AtomicU64,
    segment_size: usize,
    block_size: usize,
    device: Device,
    append_lock: Mutex<()>,
    append_queue: Mutex<Vec<usize>>,

    inner: Arc<Inner>,
}

impl File {
    pub fn get(&self, segment: u64) -> Result<SegmentRef> {
        let inner = self.get_inner(segment)?;
        Ok(SegmentRef { inner })
    }

    pub fn get_mut(&self, segment: u64) -> Result<SegmentMut> {
        let inner = self.get_inner(segment)?;
        todo!()
    }

    pub fn append(&self) -> Result<SegmentMut> {
        let data = alloc_aligned_slice_zeroed(self.segment_size, self.block_size);
        let data = Either::Right(data);
        let append_guard = self.append_lock.lock();
        let index = self.segment_count.load(Ordering::Relaxed);
        Ok(SegmentMut {
            data,
            index,
            f: self,
            append_guard: Some(append_guard),
        })
    }

    pub fn allocate(&self, segment_count: u64) -> Result<()> {
        todo!()
    }

    fn get_inner(&self, segment: u64) -> Result<Arc<SegmentInner>> {
        self.cached
            .try_get_with(segment, || {
                self.read_segment(segment).map(Arc::new).map_err(|e| e.kind)
            })
            .map_err(|kind| Error {
                kind: kind.deref().clone(),
            })
    }

    fn read_segment(&self, segment: u64) -> Result<SegmentInner> {
        let mut data = alloc_aligned_slice_zeroed(self.segment_size, self.block_size);

        let offset = segment
            .checked_mul(self.segment_size as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;

        unsafe { self.device.read_into(&mut data, offset)? };

        let inner = SegmentInner {
            data,
            updated: false,
            inner: self.inner.clone(),
        };

        Ok(inner)
    }

    fn commit_write(&self, key: u64, data: Box<[u8]>) {
        let new = Arc::new(SegmentInner {
            updated: true,
            data,
            inner: self.inner.clone(),
        });
        self.cached.insert(key, new);
    }

    fn commit_append(&self, key: u64, data: Box<[u8]>) {
        self.commit_write(key, data);
        let old = self.segment_count.fetch_add(1, Ordering::Relaxed);
        assert_eq!(old, key);
    }

    fn cancel_write(&self, _: u64) {}
}

struct Inner {
    segment_size: usize,
    block_size: usize,
    unused_buffers: Mutex<Vec<Box<[u8]>>>,
}

impl Inner {
    fn get_buffer_zeroed(&self) -> Box<[u8]> {
        if let Some(mut buf) = self.unused_buffers.lock().pop() {
            buf.fill(0);
            buf
        } else {
            alloc_aligned_slice_zeroed(self.segment_size, self.block_size)
        }
    }

    fn get_buffer_from_slice(&self, slice: &[u8]) -> Box<[u8]> {
        if let Some(mut buf) = self.unused_buffers.lock().pop() {
            buf.copy_from_slice(slice);
            buf
        } else {
            assert_eq!(slice.len(), self.segment_size);
            clone_slice_aligned(slice, self.block_size)
        }
    }
}

struct SegmentInner {
    updated: bool,
    data: Box<[u8]>,
    inner: Arc<Inner>,
}

impl Drop for SegmentInner {
    fn drop(&mut self) {
        let buffer = mem::take(&mut self.data);
        self.inner.unused_buffers.lock().push(buffer);
    }
}

pub struct SegmentRef {
    inner: Arc<SegmentInner>,
}

impl Deref for SegmentRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner.data
    }
}

pub struct SegmentMut<'a> {
    f: &'a File,
    index: u64,
    data: Either<Arc<[u8]>, Box<[u8]>>,
    append_guard: Option<MutexGuard<'a, ()>>,
}

impl SegmentMut<'_> {
    pub fn commit(self) {
        if let Some(new) = self.data.right() {
            if self.append_guard.is_some() {
                self.f.commit_append(self.index, new);
            } else {
                self.f.commit_write(self.index, new);
            }
        }
    }

    pub fn rollback(self) {}
}

impl<'a> Deref for SegmentMut<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a> DerefMut for SegmentMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if self.data.is_left() {
            let data = clone_slice_aligned(&self.data, self.f.block_size);
            self.data = Either::Right(data);
        }
        self.data.as_mut().right().unwrap()
    }
}
