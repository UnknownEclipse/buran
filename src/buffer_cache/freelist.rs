use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
};

use parking_lot::Mutex;

pub struct Freelist {
    inner: Mutex<Inner>,
    fused: AtomicBool,
}

impl Freelist {
    pub fn new_full(size: usize) -> Self {
        let links = (2..size + 1)
            .map(NonZeroUsize::new)
            .chain(Some(None))
            .collect();

        let inner = Inner {
            links,
            head: NonZeroUsize::new(1),
        };

        Self {
            inner: Mutex::new(inner),
            fused: AtomicBool::new(false),
        }
    }

    pub fn pop(&self) -> Option<usize> {
        if self.fused.load(Ordering::Relaxed) {
            None
        } else {
            let mut guard = self.inner.lock();
            let popped = guard.pop();
            if popped.is_none() {
                self.fused.store(true, Ordering::Relaxed);
            }
            popped
        }
    }

    pub fn push(&self, index: usize) {
        let mut guard = self.inner.lock();
        self.fused.store(true, Ordering::Relaxed);
        guard.push(index);
    }
}

struct Inner {
    links: Box<[Option<NonZeroUsize>]>,
    head: Option<NonZeroUsize>,
}

impl Inner {
    pub fn pop(&mut self) -> Option<usize> {
        let head = self.head?.get() - 1;
        self.head = self.links[head];
        Some(head)
    }

    pub fn push(&mut self, index: usize) {
        self.links[index] = self.head;
        self.head = Some(NonZeroUsize::new(index + 1).unwrap());
    }
}
