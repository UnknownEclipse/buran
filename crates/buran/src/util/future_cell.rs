pub use v2::FutureCell;

#[cfg(future_cell_v1_impl)]
mod v1 {
    // This is copied primarily from [once_cell's impl]
    // (https://github.com/matklad/once_cell/blob/master/src/imp_std.rs) but with added
    // async support and fixes for unsafe_op_in_unsafe_fn error.

    use std::{
        cell::{Cell, UnsafeCell},
        convert::Infallible,
        hint::unreachable_unchecked,
        marker::PhantomData,
        mem,
        panic::{RefUnwindSafe, UnwindSafe},
        pin::Pin,
        ptr,
        sync::atomic::{AtomicBool, AtomicPtr, Ordering},
        task::{Context, Poll, Waker},
        thread::{self, Thread},
    };

    use futures::Future;
    use sptr::Strict;

    #[derive(Debug)]
    pub struct FutureCell<T> {
        queue: AtomicPtr<Waiter>,
        _marker: PhantomData<*mut Waiter>,
        value: UnsafeCell<Option<T>>,
    }

    // Why do we need `T: Send`?
    // Thread A creates a `OnceCell` and shares it with
    // scoped thread B, which fills the cell, which is
    // then destroyed by A. That is, destructor observes
    // a sent value.
    unsafe impl<T: Sync + Send> Sync for FutureCell<T> {}
    unsafe impl<T: Send> Send for FutureCell<T> {}

    impl<T: RefUnwindSafe + UnwindSafe> RefUnwindSafe for FutureCell<T> {}
    impl<T: UnwindSafe> UnwindSafe for FutureCell<T> {}

    impl<T> FutureCell<T> {
        pub const fn new() -> FutureCell<T> {
            FutureCell {
                queue: AtomicPtr::new(INCOMPLETE_PTR),
                _marker: PhantomData,
                value: UnsafeCell::new(None),
            }
        }

        pub fn get(&self) -> Option<&T> {
            if self.is_initialized() {
                Some(unsafe { self.get_unchecked() })
            } else {
                None
            }
        }

        /// Gets the mutable reference to the underlying value.
        /// Returns `None` if the cell is empty.
        pub fn get_mut(&mut self) -> Option<&mut T> {
            // Safe b/c we have a unique access.
            unsafe { &mut *self.value.get() }.as_mut()
        }

        /// Get the reference to the underlying value, without checking if the cell
        /// is initialized.
        ///
        /// # Safety
        ///
        /// Caller must ensure that the cell is in initialized state, and that
        /// the contents are acquired by (synchronized to) this thread.
        pub unsafe fn get_unchecked(&self) -> &T {
            debug_assert!(self.is_initialized());
            let slot: &Option<T> = unsafe { &*self.value.get() };
            match slot {
                Some(value) => value,
                // This unsafe does improve performance, see `examples/bench`.
                None => {
                    debug_assert!(false);
                    unsafe { unreachable_unchecked() }
                }
            }
        }

        /// Consumes this `OnceCell`, returning the wrapped value.
        /// Returns `None` if the cell was empty.
        #[inline]
        pub fn into_inner(self) -> Option<T> {
            // Because `into_inner` takes `self` by value, the compiler statically
            // verifies that it is not currently borrowed.
            // So, it is safe to move out `Option<T>`.
            self.value.into_inner()
        }

        pub fn take(&mut self) -> Option<T> {
            mem::take(self).into_inner()
        }

        pub fn get_or_try_init<F, E>(&self, f: F) -> Result<&T, E>
        where
            F: FnOnce() -> Result<T, E>,
        {
            if let Some(value) = self.get() {
                return Ok(value);
            }
            self.initialize(f)?;

            debug_assert!(self.is_initialized());
            Ok(unsafe { self.get_unchecked() })
        }

        pub fn get_or_init<F>(&self, f: F) -> &T
        where
            F: FnOnce() -> T,
        {
            self.get_or_try_init(move || Ok::<T, Infallible>(f()))
                .unwrap()
        }

        pub fn wait(&self) -> &T {
            if let Some(value) = self.get() {
                return value;
            }
            self.wait_slow();
            debug_assert!(self.is_initialized());
            unsafe { self.get_unchecked() }
        }

        pub async fn wait_async(&self) -> &T {
            if let Some(value) = self.get() {
                return value;
            }
            self.wait_async_slow().await;
            debug_assert!(self.is_initialized());
            unsafe { self.get_unchecked() }
        }

        const fn with_value(value: T) -> FutureCell<T> {
            FutureCell {
                queue: AtomicPtr::new(COMPLETE_PTR),
                _marker: PhantomData,
                value: UnsafeCell::new(Some(value)),
            }
        }

        /// Safety: synchronizes with store to value via Release/(Acquire|SeqCst).
        #[inline]
        fn is_initialized(&self) -> bool {
            // An `Acquire` load is enough because that makes all the initialization
            // operations visible to us, and, this being a fast path, weaker
            // ordering helps with performance. This `Acquire` synchronizes with
            // `SeqCst` operations on the slow path.
            self.queue.load(Ordering::Acquire) == COMPLETE_PTR
        }

        /// Safety: synchronizes with store to value via SeqCst read from state,
        /// writes value only once because we never get to INCOMPLETE state after a
        /// successful write.
        #[cold]
        fn initialize<F, E>(&self, f: F) -> Result<(), E>
        where
            F: FnOnce() -> Result<T, E>,
        {
            let mut f = Some(f);
            let mut res: Result<(), E> = Ok(());
            let slot: *mut Option<T> = self.value.get();
            initialize_or_wait_blocking(
                &self.queue,
                Some(&mut || {
                    let f = unsafe { take_unchecked(&mut f) };
                    match f() {
                        Ok(value) => {
                            unsafe { *slot = Some(value) };
                            true
                        }
                        Err(err) => {
                            res = Err(err);
                            false
                        }
                    }
                }),
            );
            res
        }

        #[cold]
        fn wait_slow(&self) {
            initialize_or_wait_blocking(&self.queue, None);
        }

        #[cold]
        async fn wait_async_slow(&self) {
            initialize_or_wait_async(&self.queue, None).await;
        }
    }

    impl<T> Default for FutureCell<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<T> From<T> for FutureCell<T> {
        fn from(value: T) -> Self {
            Self::with_value(value)
        }
    }

    // Three states that a OnceCell can be in, encoded into the lower bits of `queue` in
    // the OnceCell structure.
    const INCOMPLETE: usize = 0x0;
    const RUNNING: usize = 0x1;
    const COMPLETE: usize = 0x2;
    const INCOMPLETE_PTR: *mut Waiter = INCOMPLETE as *mut Waiter;
    const COMPLETE_PTR: *mut Waiter = COMPLETE as *mut Waiter;

    // Mask to learn about the state. All other bits are the queue of waiters if
    // this is in the RUNNING state.
    const STATE_MASK: usize = 0x3;

    /// Representation of a node in the linked list of waiters in the RUNNING state.
    /// A waiters is stored on the stack of the waiting threads.
    #[repr(align(4))] // Ensure the two lower bits are free to use as state bits.
    struct Waiter {
        waker: Cell<Option<WakerKind>>,
        signaled: AtomicBool,
        next: *mut Waiter,
    }

    enum WakerKind {
        Blocking(Thread),
        Async(Waker),
    }

    impl WakerKind {
        fn wake(self) {
            match self {
                WakerKind::Blocking(thread) => thread.unpark(),
                WakerKind::Async(waker) => waker.wake(),
            }
        }
    }

    /// Drains and notifies the queue of waiters on drop.
    struct Guard<'a> {
        queue: &'a AtomicPtr<Waiter>,
        new_queue: *mut Waiter,
    }

    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            let queue = self.queue.swap(self.new_queue, Ordering::AcqRel);

            let state = Strict::addr(queue) & STATE_MASK;
            assert_eq!(state, RUNNING);

            unsafe {
                let mut waiter = Strict::map_addr(queue, |q| q & !STATE_MASK);
                while !waiter.is_null() {
                    let next = (*waiter).next;
                    let thread = (*waiter).waker.take().unwrap();
                    (*waiter).signaled.store(true, Ordering::Release);
                    waiter = next;
                    thread.wake();
                }
            }
        }
    }

    // Corresponds to `std::sync::Once::call_inner`.
    //
    // Originally copied from std, but since modified to remove poisoning and to
    // support wait.
    //
    // Note: this is intentionally monomorphic
    #[inline(never)]
    fn initialize_or_wait_blocking(
        queue: &AtomicPtr<Waiter>,
        mut init: Option<&mut dyn FnMut() -> bool>,
    ) {
        let mut curr_queue = queue.load(Ordering::Acquire);

        loop {
            let curr_state = Strict::addr(curr_queue) & STATE_MASK;
            match (curr_state, &mut init) {
                (COMPLETE, _) => return,
                (INCOMPLETE, Some(init)) => {
                    let exchange = queue.compare_exchange(
                        curr_queue,
                        Strict::map_addr(curr_queue, |q| (q & !STATE_MASK) | RUNNING),
                        Ordering::Acquire,
                        Ordering::Acquire,
                    );
                    if let Err(new_queue) = exchange {
                        curr_queue = new_queue;
                        continue;
                    }
                    let mut guard = Guard {
                        queue,
                        new_queue: INCOMPLETE_PTR,
                    };
                    if init() {
                        guard.new_queue = COMPLETE_PTR;
                    }
                    return;
                }
                (INCOMPLETE, None) | (RUNNING, _) => {
                    wait_blocking(queue, curr_queue);
                    curr_queue = queue.load(Ordering::Acquire);
                }
                _ => debug_assert!(false),
            }
        }
    }

    fn wait_blocking(queue: &AtomicPtr<Waiter>, mut curr_queue: *mut Waiter) {
        let curr_state = Strict::addr(curr_queue) & STATE_MASK;
        loop {
            let node = Waiter {
                waker: Cell::new(Some(WakerKind::Blocking(thread::current()))),
                signaled: AtomicBool::new(false),
                next: Strict::map_addr(curr_queue, |q| q & !STATE_MASK),
            };
            let me = &node as *const Waiter as *mut Waiter;

            let exchange = queue.compare_exchange(
                curr_queue,
                Strict::map_addr(me, |q| q | curr_state),
                Ordering::Release,
                Ordering::Relaxed,
            );
            if let Err(new_queue) = exchange {
                if Strict::addr(new_queue) & STATE_MASK != curr_state {
                    return;
                }
                curr_queue = new_queue;
                continue;
            }

            while !node.signaled.load(Ordering::Acquire) {
                thread::park();
            }
            break;
        }
    }

    #[inline(never)]
    async fn initialize_or_wait_async(
        queue: &AtomicPtr<Waiter>,
        mut init: Option<&mut dyn FnMut() -> bool>,
    ) {
        // Identical to initialize_or_wait_blocking, but replaces wait_blocking with wait_async.
        let mut curr_queue = queue.load(Ordering::Acquire);

        loop {
            let curr_state = Strict::addr(curr_queue) & STATE_MASK;
            match (curr_state, &mut init) {
                (COMPLETE, _) => return,
                (INCOMPLETE, Some(init)) => {
                    let exchange = queue.compare_exchange(
                        curr_queue,
                        Strict::map_addr(curr_queue, |q| (q & !STATE_MASK) | RUNNING),
                        Ordering::Acquire,
                        Ordering::Acquire,
                    );
                    if let Err(new_queue) = exchange {
                        curr_queue = new_queue;
                        continue;
                    }
                    let mut guard = Guard {
                        queue,
                        new_queue: INCOMPLETE_PTR,
                    };
                    if init() {
                        guard.new_queue = COMPLETE_PTR;
                    }
                    return;
                }
                (INCOMPLETE, None) | (RUNNING, _) => {
                    wait_async(queue, curr_queue).await;
                    curr_queue = queue.load(Ordering::Acquire);
                }
                _ => debug_assert!(false),
            }
        }
    }

    struct WaitAsync<'a> {
        queue: &'a AtomicPtr<Waiter>,
        waiter: UnsafeCell<Waiter>,
        polled: Cell<bool>,
        curr_queue: Cell<*mut Waiter>,
    }

    impl<'a> Future for WaitAsync<'a> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // This is adapted from the `wait_blocking` function
            let waiter = unsafe { &*self.waiter.get() };

            if self.polled.get() {
                if waiter.signaled.load(Ordering::Acquire) {
                    return Poll::Ready(());
                } else {
                    return Poll::Pending;
                }
            }

            let curr_queue = self.curr_queue.get();
            let curr_state = Strict::addr(curr_queue) & STATE_MASK;
            loop {
                unsafe {
                    (*self.waiter.get()).next = Strict::map_addr(curr_queue, |q| q & !STATE_MASK);
                    (*self.waiter.get())
                        .waker
                        .set(Some(WakerKind::Async(cx.waker().clone())));
                }

                let me = self.waiter.get() as *mut Waiter;

                let exchange = self.queue.compare_exchange(
                    curr_queue,
                    Strict::map_addr(me, |q| q | curr_state),
                    Ordering::Release,
                    Ordering::Relaxed,
                );
                if let Err(new_queue) = exchange {
                    if Strict::addr(new_queue) & STATE_MASK != curr_state {
                        return Poll::Ready(());
                    }
                    self.curr_queue.set(new_queue);
                    continue;
                }
                self.polled.set(true);
                return Poll::Pending;
            }
        }
    }

    async fn wait_async(queue: &AtomicPtr<Waiter>, curr_queue: *mut Waiter) {
        let waiter = Waiter {
            next: ptr::null_mut(),
            signaled: AtomicBool::new(false),
            waker: Cell::new(None),
        };

        WaitAsync {
            queue,
            curr_queue: Cell::new(curr_queue),
            polled: Cell::new(false),
            waiter: UnsafeCell::new(waiter),
        }
        .await
    }

    unsafe fn take_unchecked<T>(opt: &mut Option<T>) -> T {
        opt.take()
            .unwrap_or_else(|| unsafe { unreachable_unchecked() })
    }
}

mod v2 {
    use std::{
        convert::Infallible,
        fmt::{Debug, Formatter},
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::Future;
    use once_cell::sync::OnceCell;
    use pin_project_lite::pin_project;
    use tokio::sync::{futures::Notified, Notify};

    pub struct FutureCell<T> {
        cell: OnceCell<T>,
        notify: Notify,
    }

    impl<T> FutureCell<T> {
        pub fn new() -> Self {
            Self {
                cell: OnceCell::new(),
                notify: Notify::new(),
            }
        }

        pub fn get(&self) -> Option<&T> {
            self.cell.get()
        }

        pub fn get_mut(&mut self) -> Option<&mut T> {
            self.cell.get_mut()
        }

        pub fn try_insert(&self, value: T) -> Result<(), (&T, T)> {
            self.cell.try_insert(value).map(|_| {
                self.notify.notify_waiters();
            })
        }

        pub fn get_or_init<F, E>(&self, init: F) -> &T
        where
            F: FnOnce() -> T,
        {
            self.get_or_try_init(move || Ok::<T, Infallible>(init()))
                .unwrap()
        }

        pub fn get_or_try_init<F, E>(&self, init: F) -> Result<&T, E>
        where
            F: FnOnce() -> Result<T, E>,
        {
            self.cell.get_or_try_init(init).map(|value| {
                // Not necessarily the first initialization, but might notify
                // anyway as we don't have a way to do it properly.
                self.notify.notify_waiters();
                value
            })
        }

        pub fn wait(&self) -> &T {
            self.cell.wait()
        }

        pub async fn wait_async(&self) -> &T {
            if let Some(value) = self.get() {
                value
            } else {
                let notified = self.notify.notified();
                let fut = WaitAsync {
                    cell: self,
                    future: notified,
                };
                fut.await
            }
        }

        pub fn into_inner(self) -> Option<T> {
            self.cell.into_inner()
        }

        pub fn take(&mut self) -> Option<T> {
            self.cell.take()
        }
    }

    impl<T> Debug for FutureCell<T>
    where
        T: Debug,
    {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FutureCell")
                .field("value", &self.get())
                .finish()
        }
    }

    impl<T> Default for FutureCell<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    pin_project! {
        struct WaitAsync<'a, T> {
            cell: &'a FutureCell<T>,
            #[pin]
            future: Notified<'a>,
        }
    }

    impl<'a, T> Future for WaitAsync<'a, T> {
        type Output = &'a T;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if let Some(v) = self.cell.get() {
                return Poll::Ready(v);
            }
            let cell = self.cell;
            self.project().future.poll(cx).map(|_| {
                cell.get()
                    .expect("we have been notified that the cell is initialized")
            })
        }
    }
}
