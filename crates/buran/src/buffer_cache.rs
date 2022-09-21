//! Concurrent Buffer Cache
//!
//! ## Todo List
//! - Better handle 'holes' during reclamation - if the first eviction candidate
//! is in use or dirty, try the next
//! - Handle flushes

use std::{
    alloc,
    alloc::{handle_alloc_error, Layout},
    cmp,
    hash::BuildHasherDefault,
    iter,
    mem::{self, ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr::{self, NonNull},
    slice,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};

use cache_padded::CachePadded;
use flume::{unbounded, Receiver, Sender};
use nonmax::NonMaxUsize;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use parking_lot::{lock_api::RawRwLock, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHasher;
use thread_local::ThreadLocal;

use crate::util::{
    count_min::{self, Sketch},
    hash_lru::HashLru,
    index_list::{IndexList, Link},
    SyncUnsafeCell,
};

#[cfg(test)]
mod tests;

pub struct BufferCache {
    arena: Box<SyncUnsafeCell<[MaybeUninit<u8>]>>,
    count: usize,
    buffer_size: usize,
    _buffer_align: usize,
    buffer_trailing_zeros: u32,
    descriptors: Arc<[CachePadded<BufferDesc>]>,
    free: Mutex<IndexList<CachePadded<BufferDesc>>>,
    /// An LRU dirty list. The least recently written dirty buffers will be flushed first.
    dirty: Mutex<HashLru<usize, BuildHasherDefault<FxHasher>>>,
    cache_policy: Mutex<CachePolicy>,
    cache_access_buffers: ThreadLocal<Mutex<Vec<usize>>>,
    cache_access_buffer_capacity: usize,
    flushes_sender: Sender<BufferId>,
    flushes_receiver: Receiver<BufferId>,
}

impl BufferCache {
    /// Get an iterator over the addresses and indices of all buffers in this cache.
    /// This may be used for registering them for use with io_uring.
    pub fn buffer_addrs(&self) -> impl '_ + Iterator<Item = (usize, *const [u8])> {
        (0..self.count).filter_map(|i| {
            self.data_for_index(i)
                .map(|buf| (i, buf.as_ptr() as *const [_]))
        })
    }

    pub fn flushes(&self) -> Receiver<BufferId> {
        self.flushes_receiver.clone()
    }

    pub fn get_pending_flushes<'a>(&self, buf: &'a mut [MaybeUninit<BufferId>]) -> &'a [BufferId] {
        let dirty = self.dirty.lock();
        let mut iter = dirty.deque.iter().rev();

        let mut i = 0;
        while i < buf.len() {
            let index = match iter.next() {
                Some(v) => v,
                None => break,
            };
            let desc = &self.descriptors[index];
            let hdr = desc.header.read();
            if !hdr.state().is_dirty() {
                continue;
            }
            let gen = hdr.generation;
            let id = BufferId(index, gen);
            buf[i].write(id);
            i += 1;
        }

        unsafe { &*(&buf[..i] as *const [_] as *const [_]) }
    }

    fn pin(&self, id: BufferId) -> Option<Pinned<'_>> {
        // Take the guard first, to prevent it from being freed between when we
        // check the epoch and when we create a guard.

        let p = Pinned::new(self, id.index());
        if p.header().generation != id.generation() {
            None
        } else {
            Some(p)
        }
    }

    /// Attempt to get an empty buffer slot.
    ///
    /// If all buffers are in use, this will return `None`.
    pub fn slot(&self, key: u64) -> Option<UninitBuffer<'_>> {
        if let Some(index) = self.free.lock().pop_front() {
            let slot = self
                .init_slot(index, key)
                .expect("free slot, should not fail");
            self.cache_policy.lock().insert(index);
            return Some(slot);
        }

        let mut policy = self.cache_policy.lock();
        // Ensure rough cache consistency
        self.drain_cache_accesses(&mut policy);
        let index = policy.would_evict()?;

        let slot = self.init_slot(index, key)?;
        policy.evict();

        // The slot will insert the value into the cache or the freelist on drop/finish
        Some(slot)
    }

    fn init_slot(&self, index: usize, key: u64) -> Option<UninitBuffer<'_>> {
        let desc = &self.descriptors[index];
        let mut guard = desc.header.try_write()?;

        if guard.state() == State::Dirty {
            return None;
        }

        guard.generation = guard.generation.wrapping_add(1);
        guard.epoch = AtomicU64::new(0);
        guard.key = key;
        guard.state = AtomicU8::new(State::Uninit.into());

        let data_guard = unsafe { UnsafeRwLockWriteGuard::new(guard.data_latch.raw()) };
        let guard = RwLockWriteGuard::downgrade(guard);

        let pin = Pinned {
            guard,
            cache: self,
            index,
        };

        Some(UninitBuffer {
            guard: data_guard,
            pin,
        })
    }

    /// Get an initialized buffer. If the buffer exists, but is not initialized,
    /// this will return `None`. To wait for initialization to occur before returning,
    /// see [get_blocking].
    pub fn get(&self, id: BufferId) -> Option<Buffer<'_>> {
        let pin = self.pin(id)?;
        if pin.header().state().is_init() {
            self.record_access(id.index());
            Some(Buffer { pin })
        } else {
            None
        }
    }

    pub fn next_flush(&self) -> Option<Flush<'_>> {
        loop {
            let i = self.dirty.lock().would_evict()?;
            let p = Pinned::new(self, i);

            if let Some(flush) = Flush::new(p) {
                return Some(flush);
            }
        }
    }

    /// Mark a buffer as having been flushed *up to the epoch*. If the buffer has been
    /// written to since the epoch, the buffer remains dirty.
    pub fn mark_flushed(&self, id: BufferId) {
        let p = match self.pin(id) {
            Some(p) => p,
            None => return,
        };

        // Get shared latch on the buffer to prevent writers from causing races.
        let _guard = p.header().data_latch.read();

        // if epoch != p.header().epoch() {
        //     return;
        // }

        p.header().set_clean();
        self.dirty.lock().deque.remove(p.index);
    }

    fn data_for_index(&self, index: usize) -> Option<NonNull<[u8]>> {
        // https://rust-lang.github.io/unsafe-code-guidelines/layout/pointers.html
        #[repr(C)]
        struct Slice {
            ptr: *mut u8,
            len: usize,
        }

        if self.count <= index {
            return None;
        }

        let ptr = self.arena.get() as *mut MaybeUninit<u8> as *mut u8;
        let offset = index << self.buffer_trailing_zeros;
        let len = self.buffer_size;
        unsafe {
            let ptr = ptr.add(offset);
            let slice = Slice { len, ptr };
            let slice: *mut [u8] = mem::transmute(slice);
            NonNull::new(slice)
        }
    }

    fn record_access(&self, index: usize) {
        let mut queue = self
            .cache_access_buffers
            .get_or(|| Mutex::new(Vec::with_capacity(self.cache_access_buffer_capacity)))
            .lock();

        if queue.len() == queue.capacity() {
            let mut policy = self.cache_policy.lock();
            for index in queue.iter() {
                policy.access(*index);
            }
            queue.clear();
        }

        queue.push(index);
    }

    fn drain_cache_accesses(&self, policy: &mut CachePolicy) {
        for buf in self.cache_access_buffers.iter() {
            if let Some(mut guard) = buf.try_lock() {
                for &index in guard.iter() {
                    policy.access(index);
                }
                guard.clear();
            }
        }
    }
}

pub struct Flush<'a> {
    pin: Pinned<'a>,
}

impl<'a> Flush<'a> {
    fn new(p: Pinned<'a>) -> Option<Self> {
        unsafe { p.header().data_latch.raw().lock_shared() };
        if !p.header().state().is_dirty() {
            None
        } else {
            Some(Self { pin: p })
        }
    }

    pub fn data(&self) -> &[u8] {
        unsafe { self.pin.data().as_ref() }
    }

    pub fn key(&self) -> u64 {
        self.pin.header().key
    }

    pub fn buffer_id(&self) -> BufferId {
        self.pin.buffer_id()
    }

    pub fn finish(self) {
        self.pin.cache.mark_flushed(self.pin.buffer_id());
    }
}

impl<'a> Drop for Flush<'a> {
    fn drop(&mut self) {
        unsafe { self.pin.header().data_latch.raw().unlock_shared() };
    }
}

pub struct BufferCacheBuilder {
    capacity: usize,
    buffer_align: usize,
    buffer_size: usize,
}

impl BufferCacheBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            buffer_align: 1,
            buffer_size: 512,
        }
    }

    pub fn build(self) -> BufferCache {
        let dirty = Mutex::new(HashLru::with_capacity_and_hasher(
            self.capacity,
            Default::default(),
        ));

        let arena = unsafe {
            let layout = Layout::array::<u8>(self.buffer_size).unwrap();
            let layout = layout.align_to(self.buffer_align).unwrap();
            let layout = layout_repeat(layout, self.capacity).unwrap();

            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            let slice: *mut [MaybeUninit<u8>] =
                slice::from_raw_parts_mut(ptr.cast(), layout.size());
            let slice = slice as *mut SyncUnsafeCell<[MaybeUninit<u8>]>;
            Box::from_raw(slice)
        };

        let descriptors: Arc<[CachePadded<BufferDesc>]> = iter::repeat_with(Default::default)
            .take(self.capacity)
            .collect();

        let cache_policy = CachePolicy::new(descriptors.clone(), self.capacity);

        let mut free = IndexList {
            head: None,
            tail: None,
            links: descriptors.clone(),
        };
        for i in 0..self.capacity {
            free.push_back(i);
        }
        let free = Mutex::new(free);

        let (flushes_sender, flushes_receiver) = unbounded();

        BufferCache {
            arena,
            _buffer_align: self.buffer_align,
            buffer_size: self.buffer_size,
            buffer_trailing_zeros: self.buffer_size.trailing_zeros(),
            cache_access_buffers: Default::default(),
            cache_access_buffer_capacity: self.capacity / 2 + 1,
            count: self.capacity,
            cache_policy: Mutex::new(cache_policy),
            descriptors,
            dirty,
            free,
            flushes_sender,
            flushes_receiver,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufferId(usize, u64);

impl BufferId {
    pub const DANGLING: Self = Self(usize::MAX, u64::MAX);

    pub fn index(&self) -> usize {
        self.0
    }

    fn generation(&self) -> u64 {
        self.1
    }
}

struct Pinned<'a> {
    cache: &'a BufferCache,
    index: usize,
    guard: RwLockReadGuard<'a, BufferHeader>,
}

impl<'a> Pinned<'a> {
    fn new(cache: &'a BufferCache, index: usize) -> Self {
        Self {
            cache,
            index,
            guard: cache.descriptors[index].header.read(),
        }
    }

    pub fn buffer_id(&self) -> BufferId {
        BufferId(self.index, self.guard.generation)
    }

    /// Get the data slice for the buffer pinned by this object. It is important that
    /// accesses are externally synchronized, and the data is not necessarily initialized.
    pub fn data(&self) -> NonNull<[u8]> {
        self.cache
            .data_for_index(self.index)
            .expect("data must never be null")
    }

    pub fn header(&self) -> &BufferHeader {
        &self.guard
    }
}

pub struct Buffer<'a> {
    pin: Pinned<'a>,
}

impl<'a> Buffer<'a> {
    pub fn id(&self) -> BufferId {
        self.pin.buffer_id()
    }

    pub fn read(&self) -> ReadGuard<'_> {
        ReadGuard {
            guard: self.pin.header().data_latch.read(),
            pin: &self.pin,
        }
    }

    pub fn write(&self) -> WriteGuard<'_> {
        WriteGuard {
            guard: self.pin.header().data_latch.write(),
            pin: &self.pin,
        }
    }
}

struct UninitBufferListGuard<'a> {
    index: usize,
    cache: &'a BufferCache,
    enabled: bool,
}

impl<'a> Drop for UninitBufferListGuard<'a> {
    fn drop(&mut self) {
        if self.enabled {
            self.cache.free.lock().push_back(self.index);
        }
    }
}

#[must_use = "uninitialized buffers are only useful once they have been written
to and marked as initialized"]
pub struct UninitBuffer<'a> {
    pin: Pinned<'a>,
    guard: UnsafeRwLockWriteGuard,
}

impl<'a> UninitBuffer<'a> {
    fn new(pin: Pinned<'a>) -> Self {
        let guard = unsafe { UnsafeRwLockWriteGuard::new(pin.header().data_latch.raw()) };

        UninitBuffer { pin, guard }
    }

    pub fn buffer_id(&self) -> BufferId {
        self.pin.buffer_id()
    }

    /// Mark this buffer as initialized.
    ///
    /// # Safety
    ///
    /// 1. The buffer must be fully initialized.
    pub unsafe fn assume_init(self) -> Buffer<'a> {
        let mut this = ManuallyDrop::new(self);

        unsafe {
            ptr::drop_in_place(&mut this.guard);
            let pin = ptr::read(&this.pin);
            pin.cache.cache_policy.lock().insert(pin.index);
            pin.header().set_init();

            Buffer { pin }
        }
    }
}

impl<'a> Deref for UninitBuffer<'a> {
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.pin.data().as_ptr() as *mut [MaybeUninit<u8>]) }
    }
}

impl<'a> DerefMut for UninitBuffer<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.pin.data().as_ptr() as *mut [MaybeUninit<u8>]) }
    }
}

impl<'a> Drop for UninitBuffer<'a> {
    fn drop(&mut self) {
        // NOTE: This buffer will not be reachable because it is uninit, so .get()
        // will return None even with the correct id.
        self.pin.cache.free.lock().push_back(self.pin.index);
    }
}

pub struct ReadGuard<'a> {
    pin: &'a Pinned<'a>,
    guard: RwLockReadGuard<'a, ()>,
}

impl<'a> ReadGuard<'a> {
    pub fn buffer_id(&self) -> BufferId {
        self.pin.buffer_id()
    }
}

impl<'a> Deref for ReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.pin.data().as_ref() }
    }
}

pub struct WriteGuard<'a> {
    pin: &'a Pinned<'a>,
    guard: RwLockWriteGuard<'a, ()>,
}

impl<'a> WriteGuard<'a> {
    pub fn downgrade(self) -> ReadGuard<'a> {
        self.mark_dirty();
        let this = ManuallyDrop::new(self);
        unsafe {
            let pin = this.pin;
            let guard = ptr::read(&this.guard);
            let guard = RwLockWriteGuard::downgrade(guard);
            ReadGuard { pin, guard }
        }
    }

    pub fn buffer_id(&self) -> BufferId {
        self.pin.buffer_id()
    }

    fn mark_dirty(&self) {
        self.pin.header().set_dirty();
        self.pin.cache.dirty.lock().insert(self.pin.index);
    }
}

impl<'a> Deref for WriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.pin.data().as_ref() }
    }
}

impl<'a> DerefMut for WriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.pin.data().as_mut() }
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        self.mark_dirty();
    }
}

#[derive(Debug, Default)]
struct BufferHeader {
    pub generation: u64,
    pub data_latch: RwLock<()>,
    pub state: AtomicU8,
    pub epoch: AtomicU64,
    pub key: u64,
}

impl BufferHeader {
    fn set_init(&self) {
        debug_assert_eq!(self.state(), State::Uninit);
        self.set_state(State::Init);
    }

    fn set_clean(&self) {
        debug_assert!(self.state().is_init());
        self.set_state(State::Init);
    }

    fn set_dirty(&self) {
        debug_assert!(self.state().is_init());
        self.set_state(State::Dirty);
        self.bump_write_epoch();
    }

    fn state(&self) -> State {
        self.state_with(Ordering::Acquire)
    }

    fn state_with(&self, order: Ordering) -> State {
        self.state.load(order).try_into().unwrap()
    }

    fn set_state(&self, state: State) {
        self.set_state_with(state, Ordering::Release)
    }

    fn set_state_with(&self, state: State, order: Ordering) {
        self.state.store(state.into(), order)
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn bump_write_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Default)]
struct BufferDesc {
    header: RwLock<BufferHeader>,
    cache_link: SyncUnsafeCell<(
        Option<NonMaxUsize>,
        Option<NonMaxUsize>,
        Option<CacheRegion>,
    )>,
}

impl<L> Link for CachePadded<L>
where
    L: Link,
{
    fn next(&self) -> Option<NonMaxUsize> {
        self.deref().next()
    }

    fn prev(&self) -> Option<NonMaxUsize> {
        self.deref().prev()
    }

    fn set_next(&self, next: Option<NonMaxUsize>) {
        self.deref().set_next(next)
    }

    fn set_prev(&self, prev: Option<NonMaxUsize>) {
        self.deref().set_prev(prev);
    }
}

impl<L> SLruLink for CachePadded<L>
where
    L: SLruLink,
{
    fn is_protected(&self) -> bool {
        self.deref().is_protected()
    }

    fn set_protected(&self) {
        self.deref().set_protected()
    }

    fn set_probation(&self) {
        self.deref().set_probation()
    }
}

impl<L> WTinyLfuLink for CachePadded<L>
where
    L: WTinyLfuLink,
{
    fn is_main(&self) -> bool {
        self.deref().is_main()
    }

    fn set_window(&self) {
        self.deref().set_window()
    }

    fn key(&self) -> u64 {
        self.deref().key()
    }
}

impl Link for BufferDesc {
    fn next(&self) -> Option<NonMaxUsize> {
        unsafe { (*self.cache_link.get()).0 }
    }

    fn prev(&self) -> Option<NonMaxUsize> {
        unsafe { (*self.cache_link.get()).1 }
    }

    fn set_next(&self, next: Option<NonMaxUsize>) {
        unsafe {
            (*self.cache_link.get()).0 = next;
        }
    }

    fn set_prev(&self, prev: Option<NonMaxUsize>) {
        unsafe {
            (*self.cache_link.get()).1 = prev;
        }
    }
}

impl SLruLink for BufferDesc {
    fn is_protected(&self) -> bool {
        unsafe { (*self.cache_link.get()).2 == Some(CacheRegion::Protected) }
    }

    fn set_protected(&self) {
        unsafe {
            (*self.cache_link.get()).2 = Some(CacheRegion::Protected);
        }
    }

    fn set_probation(&self) {
        unsafe {
            (*self.cache_link.get()).2 = Some(CacheRegion::Probation);
        }
    }
}

impl WTinyLfuLink for BufferDesc {
    fn is_main(&self) -> bool {
        unsafe {
            matches!(
                (*self.cache_link.get()).2,
                Some(CacheRegion::Protected | CacheRegion::Probation)
            )
        }
    }

    fn set_window(&self) {
        unsafe {
            (*self.cache_link.get()).2 = Some(CacheRegion::Window);
        }
    }

    fn key(&self) -> u64 {
        unsafe { (*self.header.data_ptr()).key }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
enum State {
    Uninit = 0,
    Init,
    Dirty,
}

impl State {
    pub fn is_uninit(&self) -> bool {
        !self.is_init()
    }

    pub fn is_init(&self) -> bool {
        matches!(self, State::Init | State::Dirty)
    }

    pub fn is_dirty(&self) -> bool {
        matches!(self, State::Dirty)
    }
}

type CachePolicy = WTinyLfu<CachePadded<BufferDesc>>;

struct Lru<L> {
    list: IndexList<L>,
}

impl<L> Lru<L>
where
    L: Link,
{
    pub fn new(links: Arc<[L]>) -> Self {
        Self {
            list: IndexList {
                head: None,
                tail: None,
                links,
            },
        }
    }

    pub fn access(&mut self, index: usize) {
        self.list.remove(index);
        self.list.push_back(index);
    }

    pub fn insert(&mut self, index: usize) {
        self.list.push_back(index);
    }

    pub fn evict(&mut self) -> Option<usize> {
        self.list.pop_front()
    }

    pub fn would_evict(&self) -> Option<usize> {
        self.list.first()
    }
}

trait SLruLink: Link {
    fn is_protected(&self) -> bool;
    fn set_protected(&self);
    fn set_probation(&self);
}

struct SLru<L> {
    links: Arc<[L]>,
    probation: Lru<L>,
    protected: Lru<L>,
    protected_size: usize,
    protected_capacity: usize,
}

impl<L> SLru<L>
where
    L: SLruLink,
{
    pub fn new(protected_capacity: usize, links: Arc<[L]>) -> Self {
        let probation = Lru::new(links.clone());
        let protected = Lru::new(links.clone());

        Self {
            links,
            probation,
            protected,
            protected_size: 0,
            protected_capacity,
        }
    }

    pub fn access(&mut self, index: usize) {
        let protected = self.links[index].is_protected();

        if protected {
            self.protected.access(index);
        } else {
            self.probation.list.remove(index);
            self.protected.insert(index);
            self.links[index].set_protected();

            if self.protected_size == self.protected_capacity {
                if let Some(i) = self.protected.evict() {
                    self.probation.insert(i);
                    self.links[i].set_probation();
                }
            } else {
                self.protected_size += 1;
            }
        }
    }

    pub fn insert(&mut self, index: usize) {
        self.probation.insert(index);
        self.links[index].set_probation();
    }

    pub fn evict(&mut self) -> Option<usize> {
        if let Some(i) = self.probation.evict() {
            return Some(i);
        }
        if let Some(i) = self.protected.evict() {
            self.protected_size -= 1;
            return Some(i);
        }
        None
    }

    pub fn would_evict(&self) -> Option<usize> {
        if let Some(i) = self.probation.would_evict() {
            return Some(i);
        }
        self.protected.would_evict()
    }

    fn set_protected_capacity(&mut self, protected_capacity: usize) {
        self.protected_capacity = protected_capacity;
        self.rebalance();
    }

    fn rebalance(&mut self) {
        while self.protected_capacity < self.protected_size {
            match self.protected.evict() {
                Some(i) => self.probation.insert(i),
                None => unreachable!(),
            }
        }
        while self.protected_size < self.protected_capacity {
            match self.probation.list.pop_back() {
                Some(i) => self.protected.list.push_front(i),
                None => break,
            }
        }
    }
}

trait WTinyLfuLink: SLruLink {
    fn is_main(&self) -> bool;
    fn set_window(&self);
    fn key(&self) -> u64;
}

struct WTinyLfu<L> {
    links: Arc<[L]>,

    // Caches
    window: Lru<L>,
    slru: SLru<L>,
    capacity: usize,
    window_percentage: f32,
    main_size: usize,
    main_capacity: usize,

    // Hill Climber Optimization
    hill_climber_delta: f32,
    hit_rate: usize,
    prev_hit_rate: usize,
    hill_climber_cycle_size: usize,
    hill_climber_counter: usize,

    // Frequency analysis
    freq_sketch: count_min::Sketch<u64>,
    freq_counter: usize,
}

impl<L> WTinyLfu<L>
where
    L: WTinyLfuLink,
{
    pub fn new(links: Arc<[L]>, capacity: usize) -> Self {
        let hill_climber_cycle_size = capacity * 10;

        let window = Lru::new(links.clone());
        let freq_sketch = Sketch::new(capacity * 4);

        // Initialize this properly via the set_window_percentage() call.
        let slru = SLru::new(0, links.clone());

        let mut tinylfu = Self {
            capacity,
            hill_climber_cycle_size,
            hill_climber_delta: 0.05, // potential tuning parameter
            freq_counter: 0,
            main_size: 0,           // initialized by `set_window_percentage()`
            window_percentage: 0.1, // 10%, should be a decent initial value
            window,
            freq_sketch,
            hill_climber_counter: 0,
            hit_rate: 0,
            main_capacity: 0, // initialized by `set_window_percentage()`
            links,
            prev_hit_rate: 0,
            slru,
        };

        // Initialize things like capacities of the individual caches.
        tinylfu.set_window_percentage();

        tinylfu
    }

    pub fn access(&mut self, index: usize) {
        self.increment(index);

        if self.links[index].is_main() {
            self.slru.access(index);
            return;
        }

        self.window.list.remove(index);
        self.maybe_promote(index);
        self.adapt(true);
    }

    pub fn evict(&mut self) -> Option<usize> {
        if let Some(i) = self.window.evict() {
            return Some(i);
        }
        self.slru.evict()
    }

    pub fn would_evict(&self) -> Option<usize> {
        if let Some(i) = self.window.would_evict() {
            return Some(i);
        }
        self.slru.would_evict()
    }

    pub fn insert(&mut self, index: usize) {
        self.increment(index);
        self.maybe_promote(index);
        self.adapt(false);
    }

    fn increment(&mut self, index: usize) {
        let key = self.links[index].key();
        self.freq_sketch.increment(&key);

        self.freq_counter += 1;

        if self.freq_sketch.sample_size() <= self.freq_counter {
            self.freq_counter = 0;
            self.freq_sketch.halve();
        }
    }

    fn get_access_frequency(&mut self, index: usize) -> u32 {
        let key = self.links[index].key();
        self.freq_sketch.get(&key)
    }

    /// Promote an entry to the main cache or insert into the window if not worthy.
    fn maybe_promote(&mut self, index: usize) {
        if self.main_size < self.main_capacity {
            self.slru.insert(index);
            self.main_size += 1;
        }

        let other = self.slru.would_evict().unwrap();

        let this = self.get_access_frequency(index);
        let other_count = self.get_access_frequency(other);

        if this <= other_count {
            self.window.insert(index);
            self.links[index].set_window();
        } else {
            self.slru.evict();
            self.slru.insert(index);
            self.window.insert(other);
            self.links[index].set_probation();
            self.links[other].set_window();
        }
    }

    /// Apply a hill-climber optimization to the relative size of the window.
    fn adapt(&mut self, hit: bool) {
        self.hill_climber_counter += 1;

        if hit {
            self.hit_rate += 1;
        }

        if self.hill_climber_counter >= self.hill_climber_cycle_size {
            self.take_step();
        }
    }

    #[cold]
    fn take_step(&mut self) {
        self.hill_climber_counter = 0;

        let hit_rate = self.hit_rate;
        let prev_hit_rate = self.prev_hit_rate;

        self.hit_rate = 0;
        self.prev_hit_rate = hit_rate;

        let direction = if hit_rate < prev_hit_rate { -1.0 } else { 1.0 };
        self.hill_climber_delta *= direction;
        self.window_percentage += self.hill_climber_delta;
        self.set_window_percentage();
    }

    fn set_window_percentage(&mut self) {
        self.window_percentage = self.window_percentage.clamp(0.0, 0.8);

        let window_capacity = (self.capacity as f32 * self.window_percentage) as usize;
        self.set_main_capacity(self.capacity - window_capacity);
    }

    fn set_main_capacity(&mut self, main_capacity: usize) {
        let probation_capacity = main_capacity / 5;
        let protected_capacity = main_capacity - probation_capacity;

        assert!(1 <= probation_capacity);
        assert!(1 <= protected_capacity);

        self.slru.set_protected_capacity(protected_capacity);
        self.main_capacity = main_capacity;
        self.rebalance();
    }

    /// Rebalance the cache to ensure all caches are filled to their appropriate
    /// capacities.
    fn rebalance(&mut self) {
        // The main cache shrank
        while self.main_capacity < self.main_size {
            let i = match self.slru.evict() {
                Some(i) => i,
                None => break,
            };
            self.main_size -= 1;
            self.window.insert(i);
        }
        // This goes the other way (the main cache grew)
        while self.main_size < self.main_capacity {
            let i = match self.window.list.pop_back() {
                Some(i) => i,
                None => break,
            };
            self.slru.insert(i);
            self.main_size += 1;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheRegion {
    // WTinyLfu
    Window,
    // WTinyLfu + SLru
    Probation,
    Protected,
    // 2Q
    Am,
    A1In,
    // ARC
    L1,
    L2,
}

trait TwoQLink: Link {
    fn is_am(&self) -> bool;
    fn key(&self) -> u64;
    fn set_am(&self);
    fn set_a1in(&self);
}

struct TwoQ<L> {
    links: Arc<[L]>,
    a1in: Lru<L>,
    a1in_size: usize,
    a1in_capacity: usize,
    am: Lru<L>,
    am_size: usize,
    am_capacity: usize,
    a1out: HashLru<u64, BuildHasherDefault<FxHasher>>,
}

impl<L> TwoQ<L>
where
    L: TwoQLink,
{
    pub fn new(links: Arc<[L]>, capacity: usize) -> Self {
        assert!(4 <= capacity);

        let a1in_capacity = cmp::min(capacity / 4, 1);
        let a1out_capacity = capacity / 2;
        let am_capacity = capacity - a1in_capacity;

        let a1in = Lru::new(links.clone());
        let am = Lru::new(links.clone());
        let a1out = HashLru::with_capacity_and_hasher(a1out_capacity, Default::default());

        Self {
            links,
            a1in,
            a1in_size: 0,
            a1in_capacity,
            am,
            am_size: 0,
            am_capacity,
            a1out,
        }
    }

    pub fn insert(&mut self, index: usize) {
        let key = self.links[index].key();
        if self.a1out.deque.contains(key) {
            if self.am_size < self.am_capacity {
                self.am_size += 1;
            } else {
                let demoted = self.am.evict().unwrap();
                self.a1in.insert(demoted);
                self.links[demoted].set_a1in();
                self.a1in_size += 1;
            }
            self.am.insert(index);
            self.links[index].set_am();
        } else {
            self.a1in.insert(index);
            self.links[index].set_a1in();
            self.a1in_size += 1;
        }
    }

    pub fn access(&mut self, index: usize) {
        if self.links[index].is_am() {
            self.am.access(index);
        }
    }

    pub fn evict(&mut self) -> Option<usize> {
        if self.a1in_size > self.a1in_capacity {
            let evicted = self.a1in.evict().unwrap();
            self.a1in_size -= 1;
            self.a1out.insert(self.links[evicted].key());
            return Some(evicted);
        }
        let evicted = self.am.evict();
        if evicted.is_some() {
            self.am_size -= 1;
        }
        evicted
    }
}

// /// [https://en.wikipedia.org/wiki/Adaptive_replacement_cache]
// pub struct AdaptiveReplacementCache<L> {
//     t1: Lru<L>,
//     t1_size: usize,
//     t1_capacity: usize,
//     b1: HashLru<u64, BuildHasherDefault<FxHasher>>,

//     t2: Lru<L>,
//     t2_size: usize,
//     t2_capacity: usize,
//     b2: HashLru<u64, BuildHasherDefault<FxHasher>>,

//     center: usize,
//     last_inserted_zone: ArcZone,

//     links: Arc<[L]>,
//     capacity: usize,
//     adaptation: usize,
// }

// enum ArcZone {
//     L1,
//     L2,
// }

// impl<L> AdaptiveReplacementCache<L>
// where
//     L: Link,
// {
//     pub fn access(&mut self, index: usize) {
//         todo!()
//     }

//     pub fn evict(&mut self) -> Option<usize> {
//         let t1_avail = self.t1_capacity - self.t1_size;
//         let t2_avail = self.t2_capacity - self.t2_size;

//         let preferred = match t1_avail.cmp(&t2_avail) {
//             cmp::Ordering::Less => ArcZone::L1,
//             cmp::Ordering::Equal => {
//                 if self.t1_size < self.t2_size {
//                     ArcZone::L1
//                 } else {
//                     ArcZone::L2
//                 }
//             }
//             cmp::Ordering::Greater => ArcZone::L2,
//         };

//         match preferred {
//             ArcZone::L1 => {
//                 if let Some(i) = self.t1.evict() {
//                     self.t1_size -= 1;
//                     return Some(i);
//                 }
//                 if let Some(i) = self.t2.evict() {
//                     self.t2_size -= 1;
//                     return Some(i);
//                 }
//                 None
//             }
//             ArcZone::L2 => {
//                 if let Some(i) = self.t2.evict() {
//                     self.t2_size -= 1;
//                     return Some(i);
//                 }
//                 if let Some(i) = self.t1.evict() {
//                     self.t1_size -= 1;
//                     return Some(i);
//                 }
//                 None
//             }
//         }
//     }
// }

struct UnsafeRwLockWriteGuard {
    ptr: *const parking_lot::RawRwLock,
}

unsafe impl Send for UnsafeRwLockWriteGuard {}

impl UnsafeRwLockWriteGuard {
    pub unsafe fn new(l: &parking_lot::RawRwLock) -> Self {
        l.lock_exclusive();
        Self { ptr: l }
    }
}

impl Drop for UnsafeRwLockWriteGuard {
    fn drop(&mut self) {
        unsafe { (*self.ptr).unlock_exclusive() }
    }
}

fn layout_repeat(layout: Layout, count: usize) -> Option<Layout> {
    if layout.size() % layout.align() != 0 {
        None
    } else {
        Layout::from_size_align(layout.size().checked_mul(count)?, layout.align()).ok()
    }
}

struct BufferInner {
    data: RwLock<[u8]>,
}
