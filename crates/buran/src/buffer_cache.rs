use std::{
    cell::UnsafeCell,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
    task::Waker,
    thread::JoinHandle,
};

use dashmap::DashMap;
use flume::Sender;
use nonmax::NonMaxUsize;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::OnceCell;
use parking_lot::{lock_api::RawRwLock, Mutex};
use sharded_slab::Slab;
use thread_local::ThreadLocal;

use crate::{util::future_cell::FutureCell, Error, Result};

use self::{
    engine::EngineHandle,
    frames::{FrameRef, Frames},
};

mod buffer_table;
mod cache_accesses;
mod dirty;
mod engine;
mod frame;
mod frames;
mod freelist;
mod lru;
mod lru2q;
#[cfg(test)]
mod tests;
mod wtinylfu;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufId(pub u64);

/// A concurrent buffer cache. Buffers are uniquely identified by a 64bit integer
/// and may be safely read and written to through guards.
///
/// # Implementation
///
/// ## The I/O Engine
///
/// Because of the highly abstracted nature of requests, the replacement policy and io
/// can be separated from the rest of the cache. This further allows for the use of
/// highly efficient I/O techniques and scheduling. Foremost of these is the possibilty
/// of using `io_uring(2)` with pre-registered buffers (the frames) to reach very high
/// throughput.
///
/// The I/O engine functions almost like a server; requests are submitted by the buffer
/// cache and the engine fulfills those requests and notifies any waiters (sync or async)
/// of their completion.
///
/// The engine can also perform optimizations such as prefetching and preflushing.
pub struct BufferCache {
    frames: Frames,
    frames_slab: Slab<FetchInner>,
    table: DashMap<BufId, FrameId>,
    cache_accesses: CacheAccesses,
    engine: EngineHandle,
}

impl BufferCache {
    pub fn get(&self, buf: BufId) -> Result<ReadGuard<'_>> {
        let frame = match self.table.get(&buf) {
            Some(id) => {
                let id = *id;
                unsafe { FrameRef::from_protected(&self.frames, id) }
            }
            None => self.fetch(buf)?.wait()?,
        };

        self.cache_accesses.record(buf, frame.id);
        Ok(ReadGuard::from_frame(frame))
    }

    fn fetch(&self, buf: BufId) -> Result<Fetch<'_>> {
        use dashmap::mapref::entry::Entry::*;

        let id = self.engine.get_free()?;

        let frame = match self.table.entry(buf) {
            Occupied(entry) => {
                self.engine.reclaim(id);
                unsafe { FrameRef::from_protected(&self.frames, *entry.get()) }
            }
            Vacant(entry) => {
                let f = self.frames.get(id);
                unsafe {
                    *f.response.get() = Default::default();
                }
                let frame = unsafe { FrameRef::from_protected(&self.frames, id) };
                entry.insert(frame.id);
                frame
            }
        };
        Ok(Fetch { frame })
    }
}

impl Debug for BufferCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferCache").finish_non_exhaustive()
    }
}

pub struct ReadGuard<'a> {
    frame: FrameRef<'a>,
}

impl<'a> ReadGuard<'a> {
    /// # Safety
    /// 1. The frame must be valid for the lifetime of this guard (in other words,
    /// the reference count of the frame must include this object)
    /// 2. The frame must must be locked by a **read** lock.
    unsafe fn from_raw_frame(frame: FrameRef<'a>) -> Self {
        Self { frame }
    }

    fn from_frame(frame: FrameRef<'a>) -> Self {
        frame.lock.lock_shared();
        unsafe { Self::from_raw_frame(frame) }
    }

    fn buffer(&self) -> BufId {
        self.frame.buffer()
    }
}

impl<'a> Deref for ReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            // SAFETY: We hold a read lock on the frame
            &*self.frame.data()
        }
    }
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        unsafe { self.frame.lock.unlock_shared() };
    }
}

pub struct WriteGuard<'a> {
    frame: FrameRef<'a>,
}

impl<'a> WriteGuard<'a> {
    /// # Safety
    /// 1. The frame must be valid for the lifetime of this guard (in other words,
    /// the reference count of the frame must include this object)
    /// 2. The frame must must be locked by a **write** lock.
    unsafe fn from_raw_frame(frame: FrameRef<'a>) -> Self {
        Self { frame }
    }

    fn from_frame(frame: FrameRef<'a>) -> Self {
        frame.lock.lock_exclusive();
        Self { frame }
    }
}

impl<'a> Deref for WriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            // SAFETY: We hold a write lock on the frame
            &*self.frame.data()
        }
    }
}

impl<'a> DerefMut for WriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            // SAFETY: We hold a write lock on the frame
            &mut *self.frame.data()
        }
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        let cas = self
            .frame
            .state
            .compare_exchange(
                FrameState::Init.into(),
                FrameState::Dirty.into(),
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok();
        assert!(cas);

        unsafe {
            // SAFETY: We hold an exclusive lock
            self.frame.lock.unlock_exclusive();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FrameId(NonMaxUsize);

/// An object that will be asynchronously resolved to a frame by the engine, and
/// may be waited upon by readers and writers.
///
/// ## Implementation Notes
///
/// The frame will be initialized with a reference count of 1 that is managed by this
/// object. When all waiters finish, that 1 will be dropped. This is to prevent the
/// frame from potentially being freed between when it is allocated and when the
/// reader bumps the reference count.
#[derive(Debug, Default)]
struct MaybeFrame {
    inner: OnceCell<Result<FrameId>>,
    wakers: Mutex<Vec<Waker>>,
}

impl MaybeFrame {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn wait(&self) -> Result<FrameId> {
        self.inner
            .wait()
            .as_ref()
            .map_err(Error::clone)
            .map(|id| *id)
    }
}

struct FetchInner {
    frame: MaybeFrame,
    frames: Frames,
}

impl Drop for FetchInner {
    fn drop(&mut self) {
        if let Ok(id) = self
            .frame
            .inner
            .get()
            .expect("fetch dropped while executing")
        {
            self.frames
                .get(*id)
                .refcount
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct Frame {
    lock: parking_lot::RawRwLock,
    refcount: AtomicUsize,
    state: AtomicU8,
    buffer: AtomicU64,
    response: UnsafeCell<FutureCell<Result<()>>>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            lock: parking_lot::RawRwLock::INIT,
            refcount: Default::default(),
            state: Default::default(),
            response: Default::default(),
            buffer: Default::default(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
enum FrameState {
    Empty,
    Init,
    Fetching,
    Dirty,
}

struct Fetch<'a> {
    frame: FrameRef<'a>,
}

impl<'a> Fetch<'a> {
    fn from_frame(frame: FrameRef<'a>) -> Self {
        Self { frame }
    }
}

impl<'a> Fetch<'a> {
    /// Wait for the frame to be fetched by the backend

    pub fn wait(self) -> Result<FrameRef<'a>> {
        let result = unsafe { (*self.frame.response.get()).wait() };
        clone_result(result)?;
        Ok(self.frame)
    }

    pub async fn wait_async(self) -> Result<FrameRef<'a>> {
        let result = unsafe { (*self.frame.response.get()).wait_async().await };
        clone_result(result)?;
        Ok(self.frame)
    }
}

struct CacheAccesses {
    buffer_capacity: usize,
    buffers: ThreadLocal<Mutex<Vec<(BufId, FrameId)>>>,
    #[allow(clippy::type_complexity)]
    sink: Box<dyn Fn(&[(BufId, FrameId)])>,
}

impl CacheAccesses {
    pub fn drain_with(&self, mut sink: impl FnMut(&[(BufId, FrameId)])) {
        for buffer in self.buffers.iter() {
            let mut buffer = buffer.lock();
            sink(&buffer);
            buffer.clear();
        }
    }

    pub fn record(&self, buf: BufId, frame: FrameId) {
        let mut buffer = self
            .buffers
            .get_or(|| Mutex::new(Vec::with_capacity(self.buffer_capacity)))
            .lock();

        if buffer.len() == buffer.capacity() {
            self.sink(&buffer);
            buffer.clear();
        }

        buffer.push((buf, frame));
    }

    fn sink(&self, buf: &[(BufId, FrameId)]) {
        (self.sink)(buf);
    }
}

fn clone_result<T>(r: &Result<T>) -> Result<T>
where
    T: Clone,
{
    r.as_ref().map_err(Error::clone).map(|v| v.clone())
}

/// A handle to the backing I/O engine. This allows asynchronous interaction with the
/// engine by submitting specific requests which are then fulfilled by the engine.
struct IoEngineHandle {
    requests: Sender<Request>,
    join_handle: JoinHandle<()>,
}

impl IoEngineHandle {
    pub fn fetch(&self, buf: BufId) {
        self.send_request(Request::Fetch(buf));
    }

    pub fn dirty(&self, frame: FrameId) {
        self.send_request(Request::Dirty(frame));
    }

    pub fn flush_all(&self) {
        self.send_request(Request::FlushAll);
    }

    pub fn send_request(&self, request: Request) {
        self.requests.send(request).unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
enum Request {
    /// Fetch a buffer
    Fetch(BufId),
    /// Invalidate the buffer at the given frame index
    Invalidate(FrameId),
    /// Flush all currently dirty frames
    FlushAll,
    Dirty(FrameId),
}
