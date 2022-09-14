use std::{cell::UnsafeCell, marker::PhantomData, ptr::NonNull};

#[derive(Default)]
pub struct LinkedList<A>
where
    A: Adapter,
{
    head: Option<A::Ptr>,
    tail: Option<A::Ptr>,
    adapter: A,
}

impl<A> LinkedList<A>
where
    A: Adapter,
{
    /// # Safety
    pub unsafe fn push_back(&mut self, ptr: A::Ptr) {
        unsafe { self.adapter.acquire(ptr) };
        if let Some(prev) = self.tail {
            unsafe { self.adapter.set_prev(ptr, Some(prev)) };
        } else {
            self.head = Some(ptr);
        }
        self.tail = Some(ptr);
    }

    /// # Safety
    pub unsafe fn push_front(&mut self, ptr: A::Ptr) {
        unsafe { self.adapter.acquire(ptr) };
        if let Some(next) = self.head {
            unsafe { self.adapter.set_next(ptr, Some(next)) };
        } else {
            self.tail = Some(ptr);
        }
        self.head = Some(ptr);
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

pub trait Link {
    type Target;

    unsafe fn pointers(this: NonNull<Self::Target>) -> NonNull<Pointers<Self::Target>>;
}

#[derive(Debug, Default)]
pub struct Pointers<T> {
    inner: UnsafeCell<PointersInner<T>>,
}

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
