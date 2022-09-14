use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    mem::MaybeUninit,
    ops::Deref,
    pin::Pin,
    ptr::NonNull,
    slice,
    sync::{
        atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use flume::{Receiver, Sender};
use flurry::Guard;
use nonmax::NonMaxUsize;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::OnceCell;
use parking_lot::{lock_api::RawRwLock, Mutex, RwLockWriteGuard};
use thread_local::ThreadLocal;

use crate::{util::signal::Signal, Error, ErrorKind, Result};

use super::wtinylfu::WTinyLfu;

pub struct BufferCache {
    inner: Inner,
}

pub struct ReadGuard<'a> {
    frame: FrameRef<'a>,
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        unsafe { self.frame.lock.unlock_shared() };
    }
}

pub struct WriteGuard<'a> {
    cache: &'a BufferCache,
    guard: RwLockWriteGuard<'a, ()>,
}

/// # Implementation
///
/// `read()`:
/// 1. Check for presence in mapping table. If present, return the frame in the table.
/// 2.
struct Inner {
    arena: Box<UnsafeCell<[MaybeUninit<u8>]>>,

    frames: Box<[Frame]>,
    frame_size: usize,
    frame_align: usize,

    free_sender: Sender<FrameId>,

    fetch_sender: Sender<BufId>,

    cache_accesses: ThreadLocal<Mutex<VecDeque<FrameId>>>,
    cache_accesses_shard_size: usize,
    cache_state: Mutex<WTinyLfu>,

    pending_required_evictions: AtomicUsize,

    table: flurry::HashMap<u64, FrameCell>,
}

struct IoEngineState {
    /// Free requests
    freed: Receiver<FrameId>,
    /// Fetch requests
    fetches: Receiver<BufId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FrameId(NonMaxUsize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct BufId(u64);

#[derive(Debug, Default)]
struct FrameCell {
    inner: OnceCell<Result<FrameId>>,
    waiters: AtomicUsize, // The number of waiters 
    wakers: Mutex<Vec<Waker>>,
}

struct FrameRef<'a> {
    cache: &'a Inner,
    id: FrameId,
}

impl FrameCell {
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

struct Frame {
    lock: parking_lot::RawRwLock,
    refcount: AtomicUsize,
    bufid: AtomicU64,
    state: AtomicU8,
    error: Mutex<Option<Arc<ErrorKind>>>,
    signal: Signal,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            lock: parking_lot::RawRwLock::INIT,
            refcount: Default::default(),
            bufid: Default::default(),
            state: Default::default(),
            error: Default::default(),
            signal: Default::default(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
enum FrameState {
    Empty,
    Initialized,
    Fetching,
    Dirty,
}

struct Fetch<'a> {
    cache: &'a Inner,
    frame: NonNull<FrameCell>,
    guard: Guard<'a>,
}

impl Debug for BufferCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferCache").finish_non_exhaustive()
    }
}

impl Inner {
    pub fn invalidate_if(&self, f: impl Fn(u64) -> bool) -> Result<()> {
        todo!()
    }

    pub fn get(&self, buf: BufId) -> Result<ReadGuard<'_>> {
        let frame = self.fetch(buf).wait()?;
        Ok(ReadGuard { frame })
    }

    pub async fn get_async(&self, buf: BufId) -> Result<ReadGuard<'_>> {
        Ok(ReadGuard {
            frame: self.fetch(buf).await?,
        })
    }

    pub fn invalidate(&self, frame: FrameId) -> Result<()> {
        // let frame_ref = self.frame(frame);
        // let _guard = frame_ref.lock.write();
        // self.table.remove(&frame_ref.bufid.load(Ordering::Relaxed));

        // self.free_sender.send(frame).expect("send error");
        todo!()
    }

    pub fn frame_data(&self, frame: FrameId) -> *mut [u8] {
        let ptr = self.arena.get() as *mut MaybeUninit<u8> as *mut u8;

        unsafe {
            let ptr = ptr.add(frame.0.get());
            let len = self.frame_size;

            // TODO(correctness): This creates a temporary &mut [u8]. Is this unsound
            //     when the memory is uninit or the slice is borrowed elsewhere?
            slice::from_raw_parts_mut(ptr, len)
        }
    }

    pub fn record_access(&self, frame: FrameId) {
        let mut guard = self
            .cache_accesses
            .get_or(|| Mutex::new(VecDeque::with_capacity(self.cache_accesses_shard_size)))
            .lock();

        if guard.len() == guard.capacity() {
            let mut state = self.cache_state.lock();
            for entry in guard.drain(..) {
                todo!()
            }
            guard.clear();
        }

        guard.push_back(frame);
    }

    pub fn frame(&self, frame: FrameId) -> &Frame {
        &self.frames[frame.0.get()]
    }

    /// Fetch a buffer, returning a [Fetch] object.
    pub fn fetch(&self, buf: BufId) -> Fetch<'_> {
        let guard = self.table.guard();
        let frame = match self.table.try_insert(buf.0, FrameCell::new(), &guard) {
            Ok(cell) => {
                self.fetch_sender.send(buf).unwrap();
                cell
            }
            Err(error) => error.current,
        };
        let frame = NonNull::from(frame);
        Fetch {
            cache: self,
            frame,
            guard,
        }
    }
}

impl<'a> Fetch<'a> {
    /// Wait for the frame to be fetched by the backend
    pub fn wait(self) -> Result<FrameRef<'a>> {
        let id = unsafe {
            // SAFETY: The guard held by this object ensures the pointer remains valid.
            self.frame.as_ref().wait()?
        };

        self.cache
            .frame(id)
            .refcount
            .fetch_add(1, Ordering::Relaxed);
        Ok(FrameRef {
            cache: self.cache,
            id,
        })
    }
}

impl<'a> Future for Fetch<'a> {
    type Output = Result<FrameRef<'a>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let frame_cell = unsafe { self.frame.as_ref() };
        let id = if let Some(result) = frame_cell.inner.get() {
            clone_result(result)
        } else {
            let mut guard = frame_cell.wakers.lock();
            // Try again while we hold the guard in case a race occurred and the cell
            // is now initialized.
            if let Some(result) = frame_cell.inner.get() {
                clone_result(result)
            } else {
                guard.push(cx.waker().clone());
                return Poll::Pending;
            }
        };

        Poll::Ready(id.map(|id| FrameRef {
            cache: self.cache,
            id,
        }))
    }
}

impl<'a> Deref for FrameRef<'a> {
    type Target = Frame;

    fn deref(&self) -> &Self::Target {
        self.cache.frame(self.id)
    }
}

impl<'a> Drop for FrameRef<'a> {
    fn drop(&mut self) {
        self.cache
            .frame(self.id)
            .refcount
            .fetch_sub(1, Ordering::Relaxed);
    }
}

fn clone_result<T>(r: &Result<T>) -> Result<T>
where
    T: Clone,
{
    r.as_ref().map_err(Error::clone).map(|v| v.clone())
}
