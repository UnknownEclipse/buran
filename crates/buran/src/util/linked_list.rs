use std::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    ptr::NonNull,
    sync::Arc,
};

use nonmax::NonMaxUsize;

pub type IntrusiveList<L> = LinkedList<IntrusiveAdapter<L>>;
pub type IndexList<L> = LinkedList<IndexedAdapter<L>>;

pub struct LinkedList<A>
where
    A: Adapter,
{
    head: Option<A::Ptr>,
    tail: Option<A::Ptr>,
    pub adapter: A,
}

impl<A> LinkedList<A>
where
    A: Adapter + Default,
{
    pub fn new() -> Self {
        Default::default()
    }
}

impl<A> LinkedList<A>
where
    A: Adapter,
{
    pub fn with_adapter(adapter: A) -> Self {
        LinkedList {
            head: None,
            tail: None,
            adapter,
        }
    }

    pub unsafe fn push_back(&mut self, ptr: A::Ptr) {
        unsafe { self.adapter.acquire(ptr) };
        if let Some(prev) = self.tail {
            unsafe { self.adapter.set_prev(ptr, Some(prev)) };
        } else {
            self.head = Some(ptr);
        }
        self.tail = Some(ptr);
    }

    pub unsafe fn push_front(&mut self, ptr: A::Ptr) {
        unsafe { self.adapter.acquire(ptr) };
        if let Some(next) = self.head {
            unsafe { self.adapter.set_next(ptr, Some(next)) };
        } else {
            self.tail = Some(ptr);
        }
        self.head = Some(ptr);
    }

    pub unsafe fn remove(&mut self, ptr: A::Ptr) {
        unsafe {
            let next = self.adapter.next(ptr);
            let prev = self.adapter.prev(ptr);

            if let Some(next) = next {
                self.adapter.set_prev(next, prev);
            } else {
                self.tail = prev;
            }

            if let Some(prev) = prev {
                self.adapter.set_next(prev, next);
            } else {
                self.head = next;
            }

            self.adapter.release(ptr);
        }
    }

    pub unsafe fn pop_front(&mut self) -> Option<A::Ptr> {
        unsafe {
            let front = self.head?;
            let next = self.adapter.next(front);

            if let Some(next) = next {
                self.head = Some(next);
                self.adapter.set_prev(next, None);
            } else {
                self.tail = None;
            }

            self.adapter.release(front);
            Some(front)
        }
    }

    pub unsafe fn pop_back(&mut self) -> Option<A::Ptr> {
        unsafe {
            let back = self.tail?;
            let prev = self.adapter.prev(back);

            if let Some(prev) = prev {
                self.tail = Some(prev);
                self.adapter.set_next(prev, None);
            } else {
                self.head = None;
            }

            self.adapter.release(back);
            Some(back)
        }
    }
}

impl<A> Default for LinkedList<A>
where
    A: Adapter + Default,
{
    fn default() -> Self {
        Self {
            head: Default::default(),
            tail: Default::default(),
            adapter: Default::default(),
        }
    }
}

/// # Safety
pub unsafe trait Adapter {
    type Ptr: Copy + Eq;
    type Item;

    /// # Safety
    unsafe fn next(&self, ptr: Self::Ptr) -> Option<Self::Ptr>;

    /// # Safety
    unsafe fn prev(&self, ptr: Self::Ptr) -> Option<Self::Ptr>;

    /// # Safety
    unsafe fn set_next(&mut self, ptr: Self::Ptr, next: Option<Self::Ptr>);

    /// # Safety
    unsafe fn set_prev(&mut self, ptr: Self::Ptr, prev: Option<Self::Ptr>);

    /// # Safety
    unsafe fn resolve(&self, ptr: Self::Ptr) -> &Self::Item;

    /// # Safety
    unsafe fn as_ptr(&self, item: &Self::Item) -> Self::Ptr;

    /// # Safety
    unsafe fn acquire(&mut self, ptr: Self::Ptr);

    /// # Safety
    unsafe fn release(&mut self, ptr: Self::Ptr);
}

#[derive(Debug)]
pub struct IntrusiveAdapter<L>(PhantomData<L>);

impl<L> Default for IntrusiveAdapter<L> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

#[derive(Debug)]
pub struct IndexedIntrusiveAdapter<L>(PhantomData<L>);

impl<L> Default for IndexedIntrusiveAdapter<L> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

pub trait Link {
    type Target;

    unsafe fn pointers(this: NonNull<Self::Target>) -> NonNull<Pointers<Self::Target>>;
}

#[derive(Debug, Default)]
pub struct Pointers<T> {
    inner: UnsafeCell<PointersInner<T>>,
}

unsafe impl<T: Send> Send for Pointers<T> {}
unsafe impl<T: Sync> Sync for Pointers<T> {}

unsafe impl<L> Adapter for IntrusiveAdapter<L>
where
    L: Link<Target = L>,
{
    type Ptr = NonNull<L>;

    type Item = L::Target;

    unsafe fn next(&self, ptr: Self::Ptr) -> Option<Self::Ptr> {
        unsafe { (*L::pointers(ptr).as_ref().inner.get()).next }
    }

    unsafe fn prev(&self, ptr: Self::Ptr) -> Option<Self::Ptr> {
        unsafe { (*L::pointers(ptr).as_ref().inner.get()).prev }
    }

    unsafe fn set_next(&mut self, ptr: Self::Ptr, next: Option<Self::Ptr>) {
        unsafe {
            (*L::pointers(ptr).as_ref().inner.get()).next = next;
        }
    }

    unsafe fn set_prev(&mut self, ptr: Self::Ptr, prev: Option<Self::Ptr>) {
        unsafe {
            (*L::pointers(ptr).as_ref().inner.get()).prev = prev;
        }
    }

    unsafe fn resolve(&self, ptr: Self::Ptr) -> &Self::Item {
        unsafe { ptr.as_ref() }
    }

    unsafe fn as_ptr(&self, item: &Self::Item) -> Self::Ptr {
        NonNull::from(item)
    }

    unsafe fn acquire(&mut self, _: Self::Ptr) {}

    unsafe fn release(&mut self, _: Self::Ptr) {}
}

#[derive(Debug, Default, Clone, Copy)]
struct PointersInner<T> {
    next: Option<NonNull<T>>,
    prev: Option<NonNull<T>>,
}

#[derive(Debug, Default)]
pub struct IndexListHead {
    head: Option<NonMaxUsize>,
    tail: Option<NonMaxUsize>,
}

#[derive(Debug)]
pub struct IndexedAdapter<L>(pub Arc<[L]>);

impl<L> Clone for IndexedAdapter<L> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

unsafe impl<L> Adapter for IndexedAdapter<L>
where
    L: IndexedLink,
{
    type Ptr = NonMaxUsize;
    type Item = L;

    unsafe fn next(&self, ptr: Self::Ptr) -> Option<Self::Ptr> {
        self.0[ptr.get()].indices().next.get()
    }

    unsafe fn prev(&self, ptr: Self::Ptr) -> Option<Self::Ptr> {
        self.0[ptr.get()].indices().prev.get()
    }

    unsafe fn set_next(&mut self, ptr: Self::Ptr, next: Option<Self::Ptr>) {
        self.0[ptr.get()].indices().next.set(next);
    }

    unsafe fn set_prev(&mut self, ptr: Self::Ptr, prev: Option<Self::Ptr>) {
        self.0[ptr.get()].indices().prev.set(prev);
    }

    unsafe fn resolve(&self, ptr: Self::Ptr) -> &Self::Item {
        &self.0[ptr.get()]
    }

    unsafe fn as_ptr(&self, item: &Self::Item) -> Self::Ptr {
        unsafe {
            let start = self.0.as_ptr();
            let off = (item as *const Self::Item).offset_from(start) as usize;
            NonMaxUsize::new_unchecked(off)
        }
    }

    unsafe fn acquire(&mut self, _: Self::Ptr) {}

    unsafe fn release(&mut self, _: Self::Ptr) {}
}

pub trait IndexedLink {
    fn indices(&self) -> &Indices;
}

impl IndexedLink for Indices {
    fn indices(&self) -> &Indices {
        self
    }
}

#[derive(Debug, Default)]
pub struct Indices {
    next: Cell<Option<NonMaxUsize>>,
    prev: Cell<Option<NonMaxUsize>>,
}

unsafe impl Send for Indices {}
unsafe impl Sync for Indices {}
