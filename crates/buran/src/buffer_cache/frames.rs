use std::{
    cell::UnsafeCell,
    fmt::Debug,
    mem::MaybeUninit,
    num::{NonZeroU64, NonZeroUsize},
    ops::{Deref, Index},
    slice,
    sync::{atomic::Ordering, Arc},
};

use nonmax::NonMaxU64;

use super::{BufId, Frame, FrameId, ReadGuard};

pub(super) struct FrameRef<'a> {
    frames: &'a FramesInner,
    pub id: FrameId,
}

impl<'a> FrameRef<'a> {
    pub unsafe fn from_parts(frames: &'a Frames, id: FrameId) -> Self {
        Self {
            frames: &frames.inner,
            id,
        }
    }

    /// From a frame id that is protected (as in, will not be freed before this function
    /// returns).
    pub unsafe fn from_protected(frames: &'a Frames, id: FrameId) -> Self {
        let this = unsafe { Self::from_parts(frames, id) };
        this.refcount.fetch_add(1, Ordering::Relaxed);
        this
    }

    pub fn data(&self) -> *mut [u8] {
        self.frames.data(self.id)
    }

    pub(crate) fn buffer(&self) -> BufId {
        BufId(self.buffer.load(Ordering::Relaxed))
    }
}

impl<'a> Deref for FrameRef<'a> {
    type Target = Frame;

    fn deref(&self) -> &Self::Target {
        self.frames.get(self.id)
    }
}

impl<'a> Drop for FrameRef<'a> {
    fn drop(&mut self) {
        self.frames
            .get(self.id)
            .refcount
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub(super) struct Frames {
    inner: Arc<FramesInner>,
}

impl Frames {
    pub fn get(&self, id: FrameId) -> &Frame {
        self.inner.get(id)
    }

    pub fn get_ref(&self, id: FrameId) -> FrameRef<'_> {
        unsafe { FrameRef::from_protected(self, id) }
    }

    pub fn read(&self, id: FrameId) -> ReadGuard<'_> {
        ReadGuard::from_frame(self.get_ref(id))
    }

    pub fn data(&self, frame: FrameId) -> *mut [u8] {
        self.inner.data(frame)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

unsafe impl Send for Frames {}
unsafe impl Sync for Frames {}

impl Debug for Frames {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Frames").finish_non_exhaustive()
    }
}

impl Index<FrameId> for Frames {
    type Output = Frame;

    fn index(&self, index: FrameId) -> &Self::Output {
        self.get(index)
    }
}

struct FramesInner {
    segment: Box<UnsafeCell<[MaybeUninit<u8>]>>,
    frames: Box<[Frame]>,
    /// The size of each frame. Must be a power of two.
    frame_size: NonZeroUsize,
    frame_trailing_zeros: u32,
    frame_align: NonZeroUsize,
}

impl FramesInner {
    pub fn get(&self, id: FrameId) -> &Frame {
        &self.frames[id.0.get()]
    }

    pub fn data(&self, frame: FrameId) -> *mut [u8] {
        let i = frame.0.get();

        debug_assert!(i < self.len());

        let ptr = self.segment.get() as *mut MaybeUninit<u8> as *mut u8;
        let offset = i << self.frame_trailing_zeros;

        unsafe {
            let ptr = ptr.add(offset);
            let len = self.frame_size.get();

            // TODO(correctness): This creates a temporary &mut [u8]. Is this unsound
            //     when the memory is uninit or the slice is borrowed elsewhere?
            slice::from_raw_parts_mut(ptr, len)
        }
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }
}

unsafe impl Send for FramesInner {}
unsafe impl Sync for FramesInner {}
