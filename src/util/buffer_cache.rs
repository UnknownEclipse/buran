use std::{hash::BuildHasherDefault, num::NonZeroU64, ops::Deref, sync::Arc, thread};

use dashmap::DashMap;
use flume::{Receiver, Sender};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use rustc_hash::FxHasher;

use crate::{Error, ErrorKind, Result};

use super::{thread_id::ThreadId, tiny_lfu::TinyLfu};

pub struct BufferCache<A>
where
    A: Adapter,
{
    inner: Arc<Inner<A>>,
}

pub trait Adapter {
    /// The buffer type used by this adapter. This will generally be some variant
    /// of `Arc<Box<[u8]>>`.
    type Buffer: Clone + Deref<Target = [u8]>;

    fn flush(&self, pg: NonZeroU64, buffer: &Self::Buffer) -> Result<()>;

    fn fault(&self, pg: NonZeroU64) -> Result<Self::Buffer>;
}

impl<A> BufferCache<A>
where
    A: Adapter,
{
    pub fn get(&self, pg: NonZeroU64) -> Result<A::Buffer> {
        use dashmap::mapref::entry::Entry::*;

        self.check_background_error_state()?;

        let buffer = match self.inner.buffers.entry(pg) {
            Occupied(entry) => entry.get().clone(),
            Vacant(entry) => {
                let buffer = self.inner.adapter.fault(pg)?;
                entry.insert(buffer).clone()
            }
        };
        self.inner.record_read(pg);
        Ok(buffer)
    }

    /// Write a buffer at the given index. The write may occur asynchronously.
    pub fn insert(&self, pg: NonZeroU64, buffer: A::Buffer) -> Result<()> {
        self.check_background_error_state()?;
        self.inner.buffers.insert(pg, buffer);
        self.inner.record_read(pg);
        Ok(())
    }

    fn check_background_error_state(&self) -> Result<()> {
        if let Some(err) = self.inner.background_error.get() {
            Err(Error { kind: err.clone() })
        } else {
            Ok(())
        }
    }
}

struct Inner<A>
where
    A: Adapter,
{
    buffers: DashMap<NonZeroU64, A::Buffer, BuildHasherDefault<FxHasher>>,
    evicted: Sender<NonZeroU64>,
    /// The cache policy itself. Accesses are buffered in a series of sharded deques and
    /// the cache itself is only unlocked when those buffers are full. This helps
    /// minimize contention.
    cache_state: Mutex<TinyLfu<NonZeroU64, BuildHasherDefault<FxHasher>>>,
    /// Cache access buffer shards.
    cache_reads: Box<[Mutex<Vec<NonZeroU64>>]>,
    cache_shard_mask: u64,
    /// An error occurred in a background thread.
    // TODO: Should we allow clearing this?
    background_error: OnceCell<Arc<ErrorKind>>,

    adapter: A,
}

impl<A> Inner<A>
where
    A: Adapter,
{
    fn record_read(&self, pgno: NonZeroU64) {
        let id = ThreadId::current().as_u64().get();
        let index = (id & self.cache_shard_mask) as usize;
        let mut shard = self.cache_reads[index].lock();

        if shard.len() == shard.capacity() {
            self.drain_read_shard(&mut shard);
        }

        shard.push(pgno);
    }

    fn drain_read_shard(&self, shard: &mut Vec<NonZeroU64>) {
        let mut cache_state = self.cache_state.lock();

        for pg in shard.iter() {
            if let Some(evictee) = cache_state.insert(*pg) {
                self.evicted.send(evictee).unwrap();
            }
        }

        shard.clear();
    }
}

fn start_eviction_thread<A>(evicted: Receiver<NonZeroU64>, inner: Arc<Inner<A>>)
where
    A: 'static + Adapter + Sync + Send,
    A::Buffer: 'static + Send + Sync,
{
    thread::spawn(move || {
        for pg in evicted {
            let buffer = match inner.buffers.remove(&pg) {
                Some((_, buffer)) => buffer,
                None => continue,
            };
            match inner.adapter.flush(pg, &buffer) {
                Ok(_) => {}
                Err(error) => {
                    let error = error.kind;
                    let _ = inner.background_error.try_insert(error);
                    break;
                }
            }
        }
    });
}
