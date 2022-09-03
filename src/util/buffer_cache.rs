use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::{Deref, Not, Shl},
    sync::{Arc, Weak},
    thread,
};

use dashmap::DashMap;
use flume::{bounded, unbounded, Receiver, Sender};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{Error, ErrorKind, Result};

use super::{
    hash::TinyHashBuilder,
    thread_id::ThreadId,
    tiny_lfu::{HillClimberBuilder, HillClimberTinyLfu},
};

pub struct BufferCache<A>
where
    A: Adapter,
{
    inner: Arc<Inner<A>>,
}

pub trait Adapter {
    /// The buffer type used by this adapter. This will generally be some variant
    /// of `Arc<Box<[u8]>>`.
    type Buffer: Deref<Target = [u8]>;

    fn flush(&self, pg: NonZeroU64, buffer: &Self::Buffer) -> Result<()>;

    fn fault(&self, pg: NonZeroU64) -> Result<Arc<Self::Buffer>>;
}

impl<A> BufferCache<A>
where
    A: Adapter,
{
    pub fn get(&self, pg: NonZeroU64) -> Result<Arc<A::Buffer>> {
        use dashmap::mapref::entry::Entry::*;

        self.check_background_error_state()?;

        let (_, buffer) = match self.inner.buffers.entry(pg) {
            Occupied(entry) => entry.get().clone(),
            Vacant(entry) => {
                let buffer = self.inner.adapter.fault(pg)?;
                entry.insert((false, buffer)).clone()
            }
        };
        self.inner.record_read(pg);
        Ok(buffer)
    }

    /// Write a buffer at the given index. The write may occur asynchronously.
    pub fn write(&self, pg: NonZeroU64, buffer: Arc<A::Buffer>) -> Result<()> {
        self.check_background_error_state()?;
        self.inner.buffers.insert(pg, (true, buffer));
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

pub struct BufferCacheBuilder {
    read_buffer_shards: usize,
    read_buffer_shard_capacity: usize,
    cache_builder: HillClimberBuilder,
    evicted_chan_buffer_size: Option<NonZeroUsize>,
}

impl BufferCacheBuilder {
    pub fn build<A>(&self, adapter: A) -> BufferCache<A>
    where
        A: 'static + Adapter + Send + Sync,
        A::Buffer: 'static + Send + Sync,
    {
        let cache = self.cache_builder.build_with_hasher(Default::default());

        let (evicted_tx, evicted_rx) = self
            .evicted_chan_buffer_size
            .map(|size| bounded(size.get()))
            .unwrap_or_else(unbounded);

        let read_buffer_shards_count = self.read_buffer_shards.next_power_of_two();
        let read_buffer_shard_mask = {
            let trailing = read_buffer_shards_count.trailing_zeros();
            u64::MAX.wrapping_shr(trailing).shl(trailing).not()
        };

        let mut read_buffer_shards = Vec::new();
        read_buffer_shards.resize_with(read_buffer_shards_count, || {
            Vec::with_capacity(self.read_buffer_shard_capacity).into()
        });
        let read_buffer_shards = read_buffer_shards.into_boxed_slice();

        let inner = Inner {
            adapter,
            deferred_evictions: Default::default(),
            cache_reads: read_buffer_shards,
            cache_shard_mask: read_buffer_shard_mask,
            cache_state: Mutex::new(cache),
            evicted: evicted_tx,
            buffers: DashMap::with_hasher(Default::default()),
            background_error: OnceCell::new(),
        };

        let inner = Arc::new(inner);

        start_eviction_thread(evicted_rx, inner.clone());

        BufferCache { inner }
    }
}

struct Inner<A>
where
    A: Adapter,
{
    buffers: DashMap<NonZeroU64, (bool, Arc<A::Buffer>), TinyHashBuilder>,
    evicted: Sender<NonZeroU64>,
    deferred_evictions: DashMap<NonZeroU64, Weak<A::Buffer>>,
    /// The cache policy itself. Accesses are buffered in a series of sharded deques and
    /// the cache itself is only unlocked when those buffers are full. This helps
    /// minimize contention.
    cache_state: Mutex<HillClimberTinyLfu<NonZeroU64, TinyHashBuilder>>,
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
            for evictee in cache_state.insert(*pg) {
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
            let (modified, buffer) = match inner.buffers.remove(&pg) {
                Some((_, buffer)) => buffer,
                None => continue,
            };

            if 1 < Arc::strong_count(&buffer) {
                inner.deferred_evictions.insert(pg, Arc::downgrade(&buffer));
            }

            if !modified {
                continue;
            }

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
