use std::{
    cell::Cell,
    collections::{BTreeSet, VecDeque},
    iter::repeat_with,
    mem,
    mem::MaybeUninit,
    sync::Arc,
};

use nonmax::NonMaxUsize;

use crate::{
    buffer_cache::BufferCacheBuilder,
    util::{assert_send, assert_send_sync},
};

use super::{Buffer, BufferCache, Flush, Lru, ReadGuard, UninitBuffer, WriteGuard};

#[test]
fn send_sync() {
    assert_send_sync::<BufferCache>();

    assert_send::<Buffer<'static>>();
    assert_send::<ReadGuard<'static>>();
    assert_send::<WriteGuard<'static>>();
    assert_send::<UninitBuffer<'static>>();
    assert_send::<Flush<'static>>();
}

#[test]
fn buffer_cache() {
    let buffer_cache = BufferCacheBuilder::new(10).build();

    let mut pins = VecDeque::new();

    for i in 0..10 {
        let mut slot = buffer_cache.slot(i as u64).expect("cache full");
        slot.fill(MaybeUninit::new(i));
        let buf1 = unsafe { slot.assume_init() };
        pins.push_back(buf1);
    }

    let indices: BTreeSet<_> = pins.iter().map(|p| p.id().index()).collect();
    // no duplicates
    assert!(indices.len() == pins.len());

    // Cacpacity of 2, both buffers are currently pinned
    assert!(buffer_cache.slot(11).is_none());

    let buf1 = pins.pop_front().unwrap();
    let buf1_id = buf1.id();
    mem::drop(buf1);

    let mut slot = buffer_cache.slot(11).expect("cache full");
    slot.fill(MaybeUninit::new(11));
    let buf5 = unsafe { slot.assume_init() };
    assert_eq!(buf1_id.index(), buf5.id().index());
}

#[test]
fn lru() {
    let links: Arc<[_]> = repeat_with(ListHead::default).take(10).collect();
    let mut lru = Lru::new(links.clone());

    for i in 0..10 {
        lru.insert(i);
    }

    dbg!(links);
    for i in 0..10 {
        assert_eq!(lru.evict(), Some(i));
    }
    assert!(lru.evict().is_none());
}

#[derive(Debug, Default)]
struct ListHead {
    next: Cell<Option<NonMaxUsize>>,
    prev: Cell<Option<NonMaxUsize>>,
}

impl super::Link for ListHead {
    fn next(&self) -> Option<NonMaxUsize> {
        self.next.get()
    }

    fn prev(&self) -> Option<NonMaxUsize> {
        self.prev.get()
    }

    fn set_next(&self, next: Option<NonMaxUsize>) {
        self.next.set(next);
    }

    fn set_prev(&self, prev: Option<NonMaxUsize>) {
        self.prev.set(prev);
    }
}
