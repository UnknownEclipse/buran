//! Work In Progress v2 buffer cache implementation
//!
//! Planned features:
//! - Async and blocking
//! - I/O Uring (and similar async io support).
//! - Asynchronous fetches and flushes. Readers can hook onto a signal, async or blocking,
//! to wait for a completed fetch.
use std::{
    cell::UnsafeCell,
    convert::TryInto,
    fmt::Debug,
    mem::MaybeUninit,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    slice,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};

use dashmap::{
    mapref::entry::{Entry, VacantEntry},
    DashMap,
};
use flume::Sender;
use nonmax::{NonMaxU64, NonMaxUsize};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{buffer_cache::wtinylfu::WTinyLfu, util::signal::Signal, Error, ErrorKind, Result};

use super::Adapter;

/// ## Implementation
///
/// [BufferCache] is implemented as a single pool of memory subdivided into `frame`s. Each `frame`
/// is referenced by its index (newtyped as `FrameId`). Each `frame` maintains its own lock state
/// and is guaranteed to only reference the section of the memory pool that it corresponds to.
///
/// The cache replacement algorithm works primarily in terms of `FrameId`s, allowing them to use
/// simpler structures like an `IndexDeque`.
///
/// ### Fault Concurrency
///
/// If several threads attempt to fault the same buffer at once, there is a chance that the
/// same buffer may be allocated several times. This is bad for many reasons, so we need to manage
/// the state of faulting pages correctly. We do this through a combination of the frame lock
/// and correct ordering of the frame mapping table.
pub struct BufferCache {
    buffer: Box<UnsafeCell<[MaybeUninit<u8>]>>,
    frame_size: NonZeroUsize,
    // frame_align: NonZeroUsize,
    frames: Box<[Frame]>,
    /// A queue of reads to be sent to the backing I/O engine. Each pair describes the destination
    /// frame as well as the buffer
    read_queue: Sender<(FrameId, BufId)>,
    cache_state: Mutex<WTinyLfu>,
    mappings: DashMap<BufId, FrameId>,
    free: Mutex<Vec<FrameId>>,
    adapter: Arc<dyn Adapter + Send + Sync>,
}

impl BufferCache {
    pub fn get(&self, buf: BufId) -> Result<ReadGuard<'_>> {
        match self.mappings.entry(buf) {
            Entry::Occupied(entry) => {
                let frame = *entry.get();
                let guard = self.frame(frame).lock.read();
                Ok(ReadGuard {
                    frame,
                    guard,
                    cache: self,
                })
            }
            Entry::Vacant(entry) => {
                self.load_into_map_slot(buf, entry)
                    .map(|(frame, guard)| ReadGuard {
                        frame,
                        guard: RwLockWriteGuard::downgrade(guard),
                        cache: self,
                    })
            }
        }
    }

    pub fn get_mut(&self, buf: BufId) -> Result<WriteGuard<'_>> {
        match self.mappings.entry(buf) {
            Entry::Occupied(entry) => {
                let frame = *entry.get();
                let guard = self.frame(frame).lock.write();
                Ok(WriteGuard {
                    frame,
                    guard,
                    cache: self,
                })
            }
            Entry::Vacant(entry) => {
                self.load_into_map_slot(buf, entry)
                    .map(|(frame, guard)| WriteGuard {
                        frame,
                        guard,
                        cache: self,
                    })
            }
        }
    }

    fn frame(&self, id: FrameId) -> &Frame {
        &self.frames[id.0.get()]
    }

    fn load_into_map_slot(
        &self,
        buf: BufId,
        slot: VacantEntry<BufId, FrameId>,
    ) -> Result<(FrameId, RwLockWriteGuard<'_, ()>)> {
        let frame = self.get_empty_frame()?;

        // Get an exclusive lock on the frame, then drop the dashmap entry. We do this
        // so that we don't block any dashmap accesses while the buffer is being loaded.
        let mut guard = self.frame(frame).lock.write();
        slot.insert(frame);
        self.frame(frame).set_state(FrameState::Clean);

        // Load the buffer into the frame
        self.load_into(buf, &mut guard)?;
        // Downgrade the lock atomically
        Ok((frame, guard))
    }

    fn frame_data(&self, id: FrameId) -> NonNull<[u8]> {
        assert!(id.0.get() < self.frames.len());

        let off = self.frame_size.get() * id.0.get();
        let len = self.frame_size.get();

        unsafe {
            let ptr = self.buffer.get() as *mut [u8] as *mut u8;
            let ptr = ptr.add(off);

            // TODO(correctness): This will create a temporary &mut [u8]. Is this sound under
            //     stacked borrows?
            let slice = slice::from_raw_parts_mut(ptr, len);
            NonNull::from(slice)
        }
    }

    /// Get an empty frame for use with a new buffer, first attempting to use an unused one,
    /// and otherwise evicting an older frame.
    ///
    /// The returned frame id is guaranteed to be unique.
    fn get_empty_frame(&self) -> Result<FrameId> {
        if let Some(id) = self.free.lock().pop() {
            return Ok(id);
        }
        let evicted = self.cache_state.lock().evict().expect("buffer cache empty");
        Ok(FrameId(NonMaxUsize::new(evicted).unwrap()))
    }

    /// Load data from a buffer into the specified frame.
    fn load_into(&self, buf: BufId, slot: &mut RwLockWriteGuard<'_, ()>) -> Result<FrameId> {
        todo!()
    }

    fn flush_frame(&self, guard: &ReadGuard<'_>) -> Result<()> {
        let frame = self.frame(guard.frame);
        let guard = frame.flush_lock.lock();
        if frame.state() != FrameState::Dirty {
            return Ok(());
        }

        // self.adapter.write(i, buf)?;
        frame.set_state(FrameState::Clean);
        Ok(())
    }
}

pub trait IoEngine {
    fn read(&self, id: BufId, buf: &mut [u8]) -> Result<()>;
    unsafe fn read_uninit(&self, id: BufId, buf: &mut [MaybeUninit<u8>]) -> Result<()>;
    fn write(&self, id: BufId, buf: &[u8]) -> Result<()>;
    fn sync(&self) -> Result<()>;
}

pub struct ReadGuard<'a> {
    cache: &'a BufferCache,
    frame: FrameId,
    guard: RwLockReadGuard<'a, ()>,
}

impl<'a> Deref for ReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.cache.frame_data(self.frame).as_ref() }
    }
}

pub struct WriteGuard<'a> {
    cache: &'a BufferCache,
    frame: FrameId,
    guard: RwLockWriteGuard<'a, ()>,
}

impl<'a> WriteGuard<'a> {
    pub fn downgrade(self) -> ReadGuard<'a> {
        let guard = RwLockWriteGuard::downgrade(self.guard);

        ReadGuard {
            frame: self.frame,
            cache: self.cache,
            guard,
        }
    }
}

impl<'a> Deref for WriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.cache.frame_data(self.frame).as_ref() }
    }
}

impl<'a> DerefMut for WriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.cache.frame_data(self.frame).as_mut() }
    }
}

/// The id of a single buffer
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufId(pub NonMaxU64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FrameId(pub NonMaxUsize);

struct Frame {
    /// Lock used to manage both data buffer concurrency and other frame operations.
    lock: RwLock<()>,
    /// The buffer that this frame is allocated for, or u64::MAX if this frame is free.
    owning_buffer: AtomicU64,
    frame_state: AtomicU8,
    /// An error that may have occurred during an asynchronous read or write
    flush_lock: Mutex<()>,
}

impl Frame {
    pub fn buffer(&self) -> Option<BufId> {
        let buf = self.owning_buffer.load(Ordering::Acquire);
        NonMaxU64::new(buf).map(BufId)
    }

    pub fn set_state(&self, state: FrameState) {
        self.frame_state.store(state.into(), Ordering::Release);
    }

    pub fn state(&self) -> FrameState {
        self.frame_state
            .load(Ordering::Acquire)
            .try_into()
            .expect("invalid frame state")
    }
}

/// A handle to a fetch operation.
struct Fetch<'a> {
    cache: &'a BufferCache,
    frame: FrameId,
}

impl<'a> Fetch<'a> {
    pub fn wait_blocking(self) -> Result<WriteGuard<'a>> {
        todo!()
    }
}

impl Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Frame").finish_non_exhaustive()
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive)]
enum FrameState {
    Free = 0,
    Dirty,
    Clean,
}
