use std::{
    cell::UnsafeCell,
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut, Range},
    slice,
    sync::atomic::{AtomicU64, AtomicU8, Ordering},
    thread,
};

use dashmap::{
    mapref::entry::{Entry, VacantEntry},
    DashMap,
};
use flume::Sender;
use nonmax::{NonMaxU64, NonMaxUsize};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use parking_lot::{
    lock_api::{RawRwLock, RawRwLockDowngrade},
    Mutex,
};
use thread_local::ThreadLocal;

use crate::Result;

use super::wtinylfu::WTinyLfu;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufId(NonMaxU64);

pub struct BufferCache {
    buffer: Box<UnsafeCell<[MaybeUninit<u8>]>>,
    table: DashMap<BufId, FrameId>,
    buffer_size: usize,

    frames: Box<[Frame]>,
    flush_queue: Sender<FrameId>,

    // Buffer replacement algorithm stuff
    cache_state: Mutex<WTinyLfu>,
    cache_accesses: ThreadLocal<Mutex<Vec<FrameId>>>,
    cache_accesses_buffers_size: usize,
}

impl BufferCache {
    pub fn get(&self, buffer: BufId) -> Result<ReadGuard<'_>> {
        match self.table.entry(buffer) {
            Entry::Occupied(frame) => {
                let guard = unsafe {
                    // SAFETY: We hold a lock on the buffer's table entry, preventing modifications to the
                    // frame ownership
                    self.read_guard(*frame.get())
                };
                self.record_access(guard.frame);
                Ok(guard)
            }
            Entry::Vacant(slot) => {
                let guard = self.fetch(buffer, slot)?;
                Ok(guard.downgrade())
            }
        }
    }

    pub fn get_mut(&self, buffer: BufId) -> Result<WriteGuard<'_>> {
        let guard = match self.table.entry(buffer) {
            Entry::Occupied(frame) => {
                let guard = unsafe {
                    // SAFETY: We hold a lock on the buffer's table entry, preventing modifications to the
                    // frame ownership
                    self.write_guard(*frame.get())
                };
                self.record_access(guard.frame);
                guard
            }
            Entry::Vacant(slot) => self.fetch(buffer, slot)?,
        };
        self.frame(guard.frame)
            .state
            .store(FrameState::Dirty.into(), Ordering::Relaxed);
        Ok(guard)
    }

    fn fetch(&self, buf: BufId, slot: VacantEntry<BufId, FrameId>) -> Result<WriteGuard<'_>> {
        // Steps:
        // 0. We hold an exclusive lock on the buffer table slot for the given buffer,
        //    preventing other threads from attempting a fault.
        // 1. Allocate an empty frame. This frame will only be accessible from this function.
        // 2. Lock that frame exclusively
        // 3. Perform the fetch and return the same write guard.
        // 4. Insert that frame into the mapping table and drop the guard. This means
        //    that other accessors will see the correct frame, and when they attempt
        //    to take their locks they will block due to the write lock we have already
        //    placed on the frame.
        // 5. The caller may downgrade or pass through the returned guard, but we
        //    do need to return it to prevent the frame from being 'dropped' before it
        //    is used.
        let frame = self.get_empty_frame()?;

        let guard = unsafe {
            // SAFETY: We hold the lock in the mapping table entry for this buffer/frame
            self.write_guard(frame)
        };

        let index = self.frame_data_index(frame);
        let ptr = self.buffer.get() as *mut MaybeUninit<u8>;

        let dst = unsafe {
            let ptr = ptr.add(index.start);
            slice::from_raw_parts_mut(ptr, index.len())
        };

        match self.io_engine_read(buf, dst) {
            Ok(_) => {}
            Err(err) => {
                unsafe { self.frame(frame).raw_lock.unlock_exclusive() };
                return Err(err);
            }
        }

        self.frame(frame).user.store(buf.0.get(), Ordering::Relaxed);
        self.frame(frame)
            .state
            .store(FrameState::Clean.into(), Ordering::Relaxed);
        slot.insert(frame); // drop the exclusive lock on the mapping table entry for this buffer
        self.record_insert(frame);

        // The returned guard will manage the write lock we took earlie
        Ok(guard)
    }

    fn frame_data_index(&self, frame: FrameId) -> Range<usize> {
        let offset = frame.0.get() as usize * self.buffer_size;
        offset..offset + self.buffer_size
    }

    unsafe fn read_guard(&self, frame: FrameId) -> ReadGuard<'_> {
        self.frame(frame).raw_lock.lock_shared();
        ReadGuard { cache: self, frame }
    }

    unsafe fn write_guard(&self, frame: FrameId) -> WriteGuard<'_> {
        self.frame(frame).raw_lock.lock_exclusive();
        WriteGuard { cache: self, frame }
    }

    fn get_empty_frame(&self) -> Result<FrameId> {
        todo!()
    }

    fn frame(&self, frame: FrameId) -> &Frame {
        &self.frames[frame.0.get()]
    }

    fn record_access(&self, frame: FrameId) {
        let mut buf = self
            .cache_accesses
            .get_or(|| Vec::with_capacity(self.cache_accesses_buffers_size).into())
            .lock();

        if buf.len() == buf.capacity() {
            let get = |index: usize| self.frames[index].user.load(Ordering::Relaxed);

            let mut state = match self.cache_state.try_lock() {
                Some(state) => state,
                None => return,
            };
            for frame in buf.iter() {
                state.access(frame.0.get(), get);
            }
            buf.clear();
        }
        buf.push(frame);
    }

    fn evict(&self) -> Option<usize> {
        let mut state = self.cache_state.lock();
        self.drain_cache_accesses_buffer(&mut state);
        state.evict()
    }

    fn evict_many(&self, buf: &mut [usize]) -> usize {
        let mut state = self.cache_state.lock();
        self.drain_cache_accesses_buffer(&mut state);
        let mut i = 0;
        while i < buf.len() {
            if let Some(index) = state.evict() {
                buf[i] = index;
                i += 1;
            } else {
                return i + 1;
            }
        }
        buf.len()
    }

    fn drain_cache_accesses_buffer(&self, state: &mut WTinyLfu) {
        let get = |index: usize| self.frames[index].user.load(Ordering::Relaxed);

        for shard in self.cache_accesses.iter() {
            let mut guard = shard.lock();
            for frame in guard.iter() {
                state.access(frame.0.get(), get);
            }
            guard.clear();
        }
    }

    fn record_insert(&self, frame: FrameId) {
        let get = |index: usize| self.frames[index].user.load(Ordering::Relaxed);
        self.cache_state.lock().insert(frame.0.get(), get);
    }

    fn io_engine_read(&self, buf: BufId, dst: &mut [MaybeUninit<u8>]) -> Result<()> {
        todo!()
    }
}

pub struct WriteGuard<'a> {
    cache: &'a BufferCache,
    frame: FrameId,
}

impl<'a> WriteGuard<'a> {
    pub fn downgrade(self) -> ReadGuard<'a> {
        unsafe { self.frame().raw_lock.downgrade() };
        let guard = ReadGuard {
            cache: self.cache,
            frame: self.frame,
        };
        mem::forget(self);
        guard
    }

    fn frame(&self) -> &Frame {
        &self.cache.frames[self.frame.0.get()]
    }
}

impl<'a> Deref for WriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let index = self.cache.frame_data_index(self.frame);
        let ptr = self.cache.buffer.get() as *const [u8];
        unsafe { &(*ptr)[index] }
    }
}

impl<'a> DerefMut for WriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let index = self.cache.frame_data_index(self.frame);
        let ptr = self.cache.buffer.get() as *mut [u8];
        unsafe { &mut (*ptr)[index] }
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            self.frame().raw_lock.unlock_exclusive();
        }
    }
}

pub struct ReadGuard<'a> {
    cache: &'a BufferCache,
    frame: FrameId,
}

impl<'a> ReadGuard<'a> {
    fn frame(&self) -> &Frame {
        &self.cache.frames[self.frame.0.get()]
    }
}

impl<'a> Deref for ReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let index = self.cache.frame_data_index(self.frame);
        let ptr = self.cache.buffer.get() as *const [u8];
        unsafe { &(*ptr)[index] }
    }
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            self.frame().raw_lock.unlock_shared();
        }
    }
}

/// A shared handle to a fetch operation. This can be waited on in a blocking manner
/// or may be used as a future in an async context.
struct Fetch<'a> {
    cache: &'a BufferCache,
    buf: BufId,
    frame: FrameId,
}

impl<'a> Fetch<'a> {
    pub fn wait_blocking(self) -> Result<WriteGuard<'a>> {
        let thread = thread::current();

        todo!()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FrameId(NonMaxUsize);

struct Frame {
    raw_lock: parking_lot::RawRwLock,
    user: AtomicU64,
    state: AtomicU8,
}

#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, IntoPrimitive, TryFromPrimitive,
)]
enum FrameState {
    Clean,
    Dirty,
}

impl Frame {
    pub fn state(&self) -> FrameState {
        self.state.load(Ordering::Relaxed).try_into().unwrap()
    }
}
