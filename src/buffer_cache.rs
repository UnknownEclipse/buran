use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    slice,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use either::Either;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{util::aligned_slice::alloc_aligned_slice, Result};

use self::{lru2q_policy::Lru2QPolicy, lru_policy::LruPolicy, tinylfu_policy::TinyLfuPolicy};

mod lru2q_policy;
mod lru_policy;
#[cfg(test)]
mod tests;
mod tinylfu_policy;

/// A concurrent buffer cache.
///
/// ## Cache Policies
///
/// Different cache policies may be beneficial for different workloads. Currently,
/// three algorithms are supported and may be configured through the builder:
///
/// 1. [LRU](
/// https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)) - The tried and true
/// 2. [2Q](https://www.vldb.org/conf/1994/P439.PDF) - A variant of LRU developed for
/// PostgresQL that uses two primary LRU lists and a third list that remembers if a page
/// number has been recently accessed. Items re first inserted into one of the LRU lists
/// called `A1in`. If that page number
/// is already present in the third list (`A1out`), then it is instead inserted into
/// the second (main) LRU list (`Am`).
/// 3. [W-TinyLFU](https://ar5iv.labs.arxiv.org/html/1512.00727) - An algorithm derived
/// from Java's Caffeine library. This uses a [segmented
/// LRU](https://en.wikipedia.org/wiki/Cache_replacement_policies#Segmented_LRU_(SLRU))
/// as its main cache and a much smaller 'window' for new items. Admission into
/// the main cache is controlled using a
/// [Count Min Sketch](https://en.wikipedia.org/wiki/Count–min_sketch). When an element
/// is being inserted into the window, the frequency sketch is checked and if the new item
/// has a higher access frequency than the item that would be evicted from the main
/// cache, it is inserted into the main cache.
///
/// ## Buffer Allocation and Alignment
///
/// Buffers are slices of a single block of memory that is allocated upon creation
/// of the cache. It is guaranteed that no new buffers will be allocated after
/// creation (although smaller associated data structures like maps may allocate).
///
/// Many I/O subsystems also require buffers aligned to the block size (or cluster size
/// on windows). As such, alignment may be specified in the builder and it is guaranteed
/// that all buffers will have at least the requested alignment.
///
/// ## Concurrency
///
/// Buffers are accessed through guards, which function identically to an ordinary
/// `RwLock<[u8]>`. As such, it is important that a single thread does not
/// attempt to read and/or write to more than one buffer at a time, or a deadlock
/// may occur.
///
/// Holding a guard will also prevent that buffer from being reclaimed
/// by the cache policy, so they should not be held for hugely extended periods of time.
/// (In practice this should not be an issue. Only the least accessed pages will be
/// evicted, and in a reasonably large cache it will take a significant amount of
/// time before a buffer reaches eviction candidate status).
pub struct BufferCache {
    /// The memory pool used by the cache. This buffer is divided into smaller frames
    /// for each buffer.
    memory: UnsafeCell<Box<[MaybeUninit<u8>]>>,
    /// The metadata for each buffer frame.
    frames: Box<[Frame]>,
    /// Mapping from page numbers to frame indices
    frame_mappings: DashMap<NonZeroU64, usize>,
    /// The cache policy used by this cache.
    // TODO: All current policies are protected by a mutex. While parking_lot spins
    // for short critical sections, any kind of locking and contention should be
    // avoided. Caffeine uses a sharded buffer to record accesses that is lazily
    // emptied. Such a strategy could be employed here.
    cache_policy: CachePolicy,
    adapter: Arc<dyn Adapter + Send + Sync>,
    /// The size of an individual buffer in the cache. This must be a multiple of the
    /// required alignment.
    buffer_size: usize,
    /// A list of free pages. Because the cache will likely fill up quickly,
    /// an atomic flag is used to indicate if accessing the freelist should even be
    /// attempted. This turns a mandatory CAS into a simple load in the common
    /// case of a full cache.
    freelist_head: Mutex<ListHead>,
    /// Tracks whether the freelist is empty. This is used to fast path new buffer
    /// allocations (no need to lock the freelist if the freelist is known to be empty)
    freelist_is_empty: AtomicBool,
}

impl BufferCache {
    /// Read a page from the buffer cache, falling bacK to the underlying adapter
    /// if it does not exist in memory.
    pub fn get(&self, i: NonZeroU64) -> Result<ReadGuard<'_>> {
        match self.frame_mappings.get(&i) {
            Some(frame) => {
                let guard = self.frames[*frame].lock.read();
                self.cache_policy.access(&self.frames, *frame);
                Ok(ReadGuard {
                    cache: self,
                    guard,
                    frame: *frame,
                })
            }
            None => {
                let (index, guard) = self.fetch_page(i)?;
                let guard = RwLockWriteGuard::downgrade(guard);
                Ok(ReadGuard {
                    cache: self,
                    guard,
                    frame: index,
                })
            }
        }
    }

    /// Read a mutable reference to a page from the buffer cache, falling bacK to the
    /// underlying adapter if it does not exist in memory.
    ///
    /// This marks the buffer as dirty, which will be flushed to the underlying
    /// adapter upon eviction.
    pub fn get_mut(&self, i: NonZeroU64) -> Result<WriteGuard<'_>> {
        let guard = match self.frame_mappings.get(&i) {
            Some(frame) => {
                let guard = self.frames[*frame].lock.write();
                self.cache_policy.access(&self.frames, *frame);
                WriteGuard {
                    cache: self,
                    guard,
                    frame: *frame,
                }
            }
            None => {
                let (index, guard) = self.fetch_page(i)?;
                WriteGuard {
                    cache: self,
                    guard,
                    frame: index,
                }
            }
        };
        self.frames[guard.frame]
            .dirty
            .store(true, Ordering::Relaxed);
        Ok(guard)
    }

    fn fetch_page(&self, i: NonZeroU64) -> Result<(usize, RwLockWriteGuard<'_, ()>)> {
        let frameno = self.get_empty_frame()?;
        let buffer = unsafe {
            let buf = self.frame_data(frameno);
            &mut *(buf as *mut [MaybeUninit<u8>])
        };

        unsafe { self.adapter.read_uninit(i, buffer)? };
        let guard = self.frames[frameno].lock.write();
        self.cache_policy.insert(&self.frames, frameno);
        self.frame_mappings.insert(i, frameno);
        self.frames[frameno].pgno.store(i.get(), Ordering::Relaxed);
        Ok((frameno, guard))
    }

    fn get_empty_frame(&self) -> Result<usize> {
        if let Some(free) = self.pop_free() {
            return Ok(free);
        }
        let index = self
            .cache_policy
            .evict(&self.frames)
            .expect("cache should never be empty");

        let frame = &self.frames[index];

        let pgno =
            NonZeroU64::new(frame.pgno.load(Ordering::Relaxed)).expect("invalid page number");
        self.frame_mappings.remove(&pgno);

        // Ensure the frame is uniquely owned. It should not be part of any lists,
        // so the only way the index is accessible is in this function
        debug_assert!(!frame.lock.is_locked());

        if frame.dirty.load(Ordering::Relaxed) {
            self.flush_frame(pgno, index)?;
        }
        Ok(index)
    }

    /// Pop a frame from the free list, if there is one
    fn pop_free(&self) -> Option<usize> {
        if self.freelist_is_empty.load(Ordering::Acquire) {
            None
        } else {
            let frame_index = self.free_list().pop_back();
            if frame_index.is_none() {
                self.freelist_is_empty.store(true, Ordering::Release);
            }
            frame_index
        }
    }

    fn free_list(&self) -> FrameList<'_> {
        FrameList {
            frames: &self.frames,
            head: Either::Left(self.freelist_head.lock()),
        }
    }

    fn frame_data(&self, index: usize) -> *mut [u8] {
        let start = self.buffer_size * index;
        let ptr = self.memory.get();
        unsafe {
            let ptr = (*ptr).as_mut_ptr().add(start);
            let slice = slice::from_raw_parts_mut(ptr, self.buffer_size);
            slice as *mut [_] as *mut [_]
        }
    }

    fn flush_frame(&self, pg: NonZeroU64, index: usize) -> Result<()> {
        let frame = &self.frames[index];

        let buf = unsafe { &*self.frame_data(index) };
        self.adapter.write(pg, buf)?;
        frame.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }
}

pub struct BufferCacheBuilder {
    capacity: usize,
    buffer_align: usize,
    buffer_size: usize,
    cache_policy: CachePolicy,
    adapter: Arc<dyn Send + Sync + Adapter>,
}

impl BufferCacheBuilder {
    pub fn finish(self) -> BufferCache {
        assert_eq!(
            self.buffer_size % self.buffer_align,
            0,
            "buffer size must be a multiple of buffer alignment"
        );

        let buffer_size = self.buffer_size * self.capacity;
        let buffer = alloc_aligned_slice(buffer_size, self.buffer_align);

        let frames = {
            let mut v = Vec::with_capacity(self.capacity);

            for i in 0..self.capacity {
                let mut frame = Frame::default();

                // Hook into free list
                frame.prev = AtomicUsize::new(i);
                let next = if i + 1 >= self.capacity {
                    0
                } else {
                    // +1 to offset it
                    // +1 to get the next element
                    i + 2
                };
                frame.next = AtomicUsize::new(next);
                v.push(frame);
            }

            v.into_boxed_slice()
        };
        let frame_mappings = Default::default();

        let freelist = ListHead {
            head: Some(0),
            tail: Some(self.capacity - 1),
        };

        BufferCache {
            memory: UnsafeCell::new(buffer),
            frames,
            frame_mappings,
            cache_policy: self.cache_policy,
            adapter: self.adapter,
            buffer_size: self.buffer_size,
            freelist_head: Mutex::new(freelist),
            freelist_is_empty: AtomicBool::new(false),
        }
    }
}

pub struct ReadGuard<'a> {
    cache: &'a BufferCache,
    frame: usize,
    guard: RwLockReadGuard<'a, ()>,
}

impl<'a> Deref for ReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.cache.frame_data(self.frame) }
    }
}

pub struct WriteGuard<'a> {
    cache: &'a BufferCache,
    frame: usize,
    guard: RwLockWriteGuard<'a, ()>,
}

impl<'a> WriteGuard<'a> {
    pub fn downgrade(self) -> ReadGuard<'a> {
        ReadGuard {
            cache: self.cache,
            frame: self.frame,
            guard: RwLockWriteGuard::downgrade(self.guard),
        }
    }
}

impl<'a> Deref for WriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.cache.frame_data(self.frame) }
    }
}

impl<'a> DerefMut for WriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.cache.frame_data(self.frame) }
    }
}

pub trait Adapter {
    fn read(&self, i: NonZeroU64, buf: &mut [u8]) -> Result<()>;

    unsafe fn read_uninit(&self, i: NonZeroU64, buf: &mut [MaybeUninit<u8>]) -> Result<()> {
        buf.fill(MaybeUninit::new(0));
        let buf = unsafe { &mut *(buf as *mut [_] as *mut [_]) };
        self.read(i, buf)
    }

    fn write(&self, i: NonZeroU64, buf: &[u8]) -> Result<()>;

    fn sync(&self) -> Result<()>;
}

#[derive(Debug, Default)]
struct Frame {
    lock: RwLock<()>,
    dirty: AtomicBool,
    pgno: AtomicU64,
    /// These links can be part of one of several lists:
    /// 1. The free list
    /// 2. An LRU list if the frame is currently in use
    next: AtomicUsize,
    prev: AtomicUsize,

    /// The region of cache this frame is in. This corresponds to the [CacheRegion]
    /// enum and may be zero if the frame is not in the cache.
    region: AtomicU8,
}

impl Frame {
    pub fn next(&self, ordering: Ordering) -> Option<usize> {
        let val = self.next.load(ordering);
        if val == 0 {
            None
        } else {
            Some(val - 1)
        }
    }

    pub fn prev(&self, ordering: Ordering) -> Option<usize> {
        let val = self.prev.load(ordering);
        if val == 0 {
            None
        } else {
            Some(val - 1)
        }
    }

    pub fn set_next(&self, next: Option<usize>, ordering: Ordering) {
        match next {
            Some(i) => self.next.store(i + 1, ordering),
            None => self.next.store(0, ordering),
        }
    }

    pub fn set_prev(&self, prev: Option<usize>, ordering: Ordering) {
        match prev {
            Some(i) => self.prev.store(i + 1, ordering),
            None => self.prev.store(0, ordering),
        }
    }

    pub fn page_number(&self) -> NonZeroU64 {
        let pg = self.pgno.load(Ordering::Relaxed);
        NonZeroU64::new(pg).expect("invalid page number")
    }
}

struct FrameList<'a> {
    frames: &'a [Frame],
    head: Either<MutexGuard<'a, ListHead>, &'a mut ListHead>,
}

impl<'a> FrameList<'a> {
    pub fn first(&self) -> Option<usize> {
        self.head.head
    }

    pub fn last(&self) -> Option<usize> {
        self.head.tail
    }

    pub fn pop_back(&mut self) -> Option<usize> {
        let tail = self.head.tail?;

        let prev = self.frames[tail].prev(Ordering::Relaxed);
        if let Some(prev) = prev {
            self.frames[prev].set_next(None, Ordering::Relaxed);
        } else {
            self.head.head = None;
        }
        self.head.tail = prev;
        Some(tail)
    }

    pub fn pop_front(&mut self) -> Option<usize> {
        let head = self.head.head?;

        let next = self.frames[head].prev(Ordering::Relaxed);
        if let Some(next) = next {
            self.frames[next].set_prev(None, Ordering::Relaxed);
        } else {
            self.head.tail = None;
        }
        self.head.head = next;
        Some(head)
    }

    pub fn remove(&mut self, i: usize) {
        let next = self.frames[i].next(Ordering::Relaxed);
        let prev = self.frames[i].prev(Ordering::Relaxed);

        if let Some(next) = next {
            self.frames[next].set_prev(prev, Ordering::Relaxed);
        } else {
            self.head.tail = prev;
        }
        if let Some(prev) = prev {
            self.frames[prev].set_next(next, Ordering::Relaxed);
        } else {
            self.head.head = next;
        }
    }

    pub fn push_front(&mut self, i: usize) {
        let next = self.head.head;
        let frame = &self.frames[i];
        frame.set_next(next, Ordering::Relaxed);
        frame.set_prev(None, Ordering::Relaxed);
        if let Some(next) = next {
            self.frames[next].set_prev(Some(i), Ordering::Relaxed);
        } else {
            self.head.tail = Some(i);
        }
        self.head.head = Some(i);
    }

    pub fn push_back(&mut self, i: usize) {
        let prev = self.head.tail;
        let frame = &self.frames[i];
        frame.set_prev(prev, Ordering::Relaxed);
        frame.set_next(None, Ordering::Relaxed);
        if let Some(prev) = prev {
            self.frames[prev].set_next(Some(i), Ordering::Relaxed);
        } else {
            self.head.head = Some(i);
        }
        self.head.tail = Some(i);
    }
}

/// The head of a frame list
#[derive(Default)]
struct ListHead {
    head: Option<usize>,
    tail: Option<usize>,
}

enum CachePolicy {
    Lru(LruPolicy),
    Lru2Q(Mutex<Lru2QPolicy>),
    TinyLfu(Mutex<TinyLfuPolicy>),
}

impl CachePolicy {
    pub fn access(&self, frames: &[Frame], frame: usize) {
        match self {
            CachePolicy::Lru(policy) => policy.access(frames, frame),
            CachePolicy::Lru2Q(policy) => policy.lock().access(frames, frame),
            CachePolicy::TinyLfu(policy) => policy.lock().access(frames, frame),
        }
    }

    pub fn evict(&self, frames: &[Frame]) -> Option<usize> {
        match self {
            CachePolicy::Lru(policy) => policy.evict(frames),
            CachePolicy::Lru2Q(policy) => policy.lock().evict(frames),
            CachePolicy::TinyLfu(policy) => policy.lock().evict(frames),
        }
    }

    pub fn insert(&self, frames: &[Frame], frame: usize) {
        match self {
            CachePolicy::Lru(policy) => policy.insert(frames, frame),
            CachePolicy::Lru2Q(policy) => policy.lock().insert(frames, frame),
            CachePolicy::TinyLfu(policy) => policy.lock().insert(frames, frame),
        }
    }
}
