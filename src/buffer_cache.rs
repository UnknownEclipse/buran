use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    num::{NonZeroU64, NonZeroUsize},
    ops::{Deref, DerefMut},
    slice,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{util::aligned_slice::alloc_aligned_slice, Result};

use self::{
    freelist::Freelist,
    lru::Lru,
    lru2q::{Lru2Q, Lru2QBuilder},
    wtinylfu::{TinyLfuBuilder, WTinyLfu},
};

mod freelist;
mod lru;
mod lru2q;
#[cfg(test)]
mod tests;
mod wtinylfu;

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
/// `RwLock*Guard`. As such, it is important that a single thread does not
/// attempt to read and/or write to a single buffer more than once at a time, or a deadlock
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
    // NOTE: Each `Frame` has a lot of metadata. It may be a better idea to move
    // each bit of metadata into its own array. For example, the linked list used by
    // the cache policy should be stored by the cache policy itself (this also means
    // we can do away with the atomics there altogether)
    frames: Box<[Frame]>,
    /// Mapping from page numbers to frame indices
    frame_mappings: DashMap<NonZeroU64, usize>,
    /// The cache policy used by this cache.
    // TODO: All current policies are protected by a mutex. While parking_lot spins
    // for short critical sections, any kind of locking and contention should be
    // avoided. Caffeine uses a sharded buffer to record accesses that is lazily
    // emptied. Such a strategy could be employed here.
    cache_state: Mutex<CacheState>,
    adapter: Arc<dyn Adapter + Send + Sync>,
    /// The size of an individual buffer in the cache. This must be a multiple of the
    /// required alignment.
    buffer_size: usize,
    /// A list of free pages
    freelist: Freelist,
}

impl BufferCache {
    /// Read a page from the buffer cache, falling bacK to the underlying adapter
    /// if it does not exist in memory.
    pub fn get(&self, i: NonZeroU64) -> Result<ReadGuard<'_>> {
        match self.frame_mappings.get(&i) {
            Some(frame) => {
                let guard = self.frames[*frame].lock.read();
                self.cache_state.lock().access(&self.frames, *frame);
                Ok(ReadGuard {
                    cache: self,
                    _guard: guard,
                    frame: *frame,
                })
            }
            None => {
                let (index, guard) = self.fetch_page(i)?;
                let guard = RwLockWriteGuard::downgrade(guard);
                Ok(ReadGuard {
                    cache: self,
                    _guard: guard,
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
                self.cache_state.lock().access(&self.frames, *frame);
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
        self.cache_state.lock().insert(&self.frames, frameno);
        self.frame_mappings.insert(i, frameno);
        self.frames[frameno].pgno.store(i.get(), Ordering::Relaxed);
        Ok((frameno, guard))
    }

    fn get_empty_frame(&self) -> Result<usize> {
        if let Some(free) = self.pop_free() {
            println!("using free frame: {}", free);
            return Ok(free);
        }
        let index = self
            .cache_state
            .lock()
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
        self.freelist.pop()
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

pub struct CacheConfig {
    pub capacity: NonZeroUsize,
    pub buffer_align: NonZeroUsize,
    pub buffer_size: NonZeroUsize,
    pub cache_policy: CachePolicy,
    pub adapter: Arc<dyn Send + Sync + Adapter>,
}

impl CacheConfig {
    pub fn build(self) -> BufferCache {
        let bufsize = self.buffer_size.get();
        let align = self.buffer_align.get();
        let cap = self.capacity.get();

        assert_eq!(
            bufsize % align,
            0,
            "buffer size must be a multiple of buffer alignment"
        );

        let buffer_size = bufsize * cap;
        let buffer = alloc_aligned_slice(buffer_size, align);

        let frames = {
            let mut v = Vec::with_capacity(cap);
            v.resize_with(cap, Default::default);
            v.into_boxed_slice()
        };

        let frame_mappings = Default::default();

        BufferCache {
            memory: UnsafeCell::new(buffer),
            frames,
            frame_mappings,
            cache_state: Mutex::new(self.cache_policy.build(cap)),
            adapter: self.adapter,
            buffer_size: bufsize,
            freelist: Freelist::new_full(cap),
        }
    }
}

pub struct ReadGuard<'a> {
    cache: &'a BufferCache,
    frame: usize,
    _guard: RwLockReadGuard<'a, ()>,
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
            _guard: RwLockWriteGuard::downgrade(self.guard),
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

/// The adapter underlying a buffer cache.
///
/// This is responsible for only two things: reading and writing complete buffers. All
/// caching and scheduling is handled by the buffer cache.
pub trait Adapter {
    /// Read a new buffer. The provided buffer will be zeroed prior to this function's
    /// invocation. So avoid the zeroing, see [read_uninit()](Adapter::read_uninit).
    fn read(&self, i: NonZeroU64, buf: &mut [u8]) -> Result<()>;

    /// # Safety
    /// The buffer must be fully initialized.
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
    dirty: AtomicBool, // Separate dirty list?
    pgno: AtomicU64,
}

impl Frame {
    pub fn page_number(&self) -> NonZeroU64 {
        let pg = self.pgno.load(Ordering::Relaxed);
        NonZeroU64::new(pg).expect("invalid page number")
    }
}

pub enum CachePolicy {
    Lru,
    Lru2Q(Lru2QBuilder),
    TinyLfu(TinyLfuBuilder),
}

impl CachePolicy {
    fn build(self, capacity: usize) -> CacheState {
        match self {
            CachePolicy::Lru => CacheState::Lru(Lru::new(capacity)),
            CachePolicy::Lru2Q(mut builder) => {
                builder.capacity = capacity;
                CacheState::Lru2Q(builder.build())
            }
            CachePolicy::TinyLfu(mut builder) => {
                builder.capacity = capacity;
                CacheState::TinyLfu(builder.build())
            }
        }
    }
}

enum CacheState {
    Lru(Lru),
    Lru2Q(Lru2Q),
    TinyLfu(WTinyLfu),
}

impl CacheState {
    pub fn access(&mut self, frames: &[Frame], frame: usize) {
        let get = |i: usize| frames[i].page_number();

        match self {
            CacheState::Lru(policy) => policy.access(frame),
            CacheState::Lru2Q(policy) => policy.access(frame),
            CacheState::TinyLfu(policy) => policy.access(frame, get),
        }
    }

    pub fn evict(&mut self, frames: &[Frame]) -> Option<usize> {
        let get = |i: usize| frames[i].page_number();

        match self {
            CacheState::Lru(policy) => policy.evict(),
            CacheState::Lru2Q(policy) => policy.evict(get),
            CacheState::TinyLfu(policy) => policy.evict(),
        }
    }

    pub fn insert(&mut self, frames: &[Frame], frame: usize) {
        let get = |i: usize| frames[i].page_number();

        match self {
            CacheState::Lru(policy) => policy.insert(frame),
            CacheState::Lru2Q(policy) => policy.insert(frame, get),
            CacheState::TinyLfu(policy) => policy.insert(frame, get),
        }
    }
}
