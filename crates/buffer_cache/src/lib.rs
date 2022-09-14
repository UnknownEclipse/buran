use std::{
    io,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
};

use once_cell::sync::OnceCell;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct BufferCache {
    error: OnceCell<io::Error>,
}

impl BufferCache {
    pub fn pin(&self, id: BufferId) -> Option<Pinned<'_>> {
        todo!()
    }

    pub fn slot(&self) -> UninitBuffer<'_> {
        todo!()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufferId(usize, u64);

pub struct Pinned<'a> {
    cache: &'a BufferCache,
    index: usize,
}

impl<'a> Pinned<'a> {
    pub fn read(&self) -> ReadGuard<'_> {
        todo!()
    }

    pub fn write(&self) -> WriteGuard<'_> {
        todo!()
    }
}

impl<'a> Drop for Pinned<'a> {
    fn drop(&mut self) {
        todo!()
    }
}

pub struct UninitBuffer<'a> {
    pinned: Pinned<'a>,
}

impl<'a> Deref for UninitBuffer<'a> {
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        todo!()
    }
}

impl<'a> DerefMut for UninitBuffer<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        todo!()
    }
}

pub struct ReadGuard<'a> {
    pinned: &'a Pinned<'a>,
    guard: RwLockReadGuard<'a, ()>,
}

pub struct WriteGuard<'a> {
    pinned: &'a Pinned<'a>,
    guard: RwLockWriteGuard<'a, ()>,
}

struct BufHdr {
    content_lock: RwLock<()>,
    pins: AtomicUsize,
}

impl BufHdr {
    fn pin(&self) {
        self.pins.fetch_add(1, Ordering::Relaxed);
    }

    fn unpin(&self) {
        self.pins.fetch_sub(1, Ordering::Relaxed);
    }
}
