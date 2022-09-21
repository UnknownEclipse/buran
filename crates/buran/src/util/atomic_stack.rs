use std::{
    ptr::{self, NonNull},
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc,
    },
};

use seize::{Collector, Guard, Linked};

use super::SyncUnsafeCell;

pub struct RawAtomicStack<T> {
    head: AtomicPtr<T>,
}

impl<T> RawAtomicStack<T>
where
    T: Node,
{
    pub fn push(&self, ptr: NonNull<T>) {
        loop {
            let head = self.head.load(Ordering::Relaxed);

            unsafe {
                T::set_next(ptr, NonNull::new(head));
            }

            if self
                .head
                .compare_exchange_weak(head, ptr.as_ptr(), Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn pop(&self) -> Option<NonNull<T>> {
        loop {
            let head = self.head.load(Ordering::Relaxed);
            let head = NonNull::new(head)?;

            let next = unsafe { T::next(head) };
            let next = next.map(|p| p.as_ptr()).unwrap_or(ptr::null_mut());

            if self
                .head
                .compare_exchange_weak(head.as_ptr(), next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(head);
            }
        }
    }
}

pub trait Node {
    unsafe fn next(this: NonNull<Self>) -> Option<NonNull<Self>>;
    unsafe fn set_next(this: NonNull<Self>, next: Option<NonNull<Self>>);
}

pub struct GcAtomicStack<T> {
    head: AtomicPtr<Linked<SimpleNode<T>>>,
    collector: Arc<Collector>,
}

impl<T> GcAtomicStack<T> {
    pub fn push(&self, value: T) {
        let node = SimpleNode {
            next: Default::default(),
            value,
        };
        let ptr = self.collector.link_boxed(node);
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        self.push_inner(ptr);
    }

    fn push_inner(&self, ptr: NonNull<Linked<SimpleNode<T>>>) {
        loop {
            let head = self.head.load(Ordering::Relaxed);

            unsafe {
                *ptr.as_ref().next.get() = NonNull::new(head);
            }

            if self
                .head
                .compare_exchange_weak(head, ptr.as_ptr(), Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

struct SimpleNode<T> {
    next: SyncUnsafeCell<Option<NonNull<Linked<SimpleNode<T>>>>>,
    value: T,
}
