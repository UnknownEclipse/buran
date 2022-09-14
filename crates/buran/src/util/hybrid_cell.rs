use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicU8, Ordering},
};

use event_listener::Event;

const STATE_EMPTY: u8 = 0;
const STATE_BUSY: u8 = 1;
const STATE_INIT: u8 = 2;

pub struct HybridCell<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
    finished_init_event: Event,
}

impl<T> HybridCell<T> {
    pub fn new() -> Self {
        HybridCell {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicU8::new(STATE_EMPTY),
            finished_init_event: Event::new(),
        }
    }

    pub fn get(&self) -> Option<&T> {
        if self.state.load(Ordering::Relaxed) == STATE_INIT {
            Some(unsafe { self.get_unchecked() })
        } else {
            None
        }
    }

    /// # Safety
    /// This `HybridCell` must be initialized.
    pub unsafe fn get_unchecked(&self) -> &T {
        unsafe { (*self.value.get()).assume_init_ref() }
    }

    pub fn wait(&self) -> &T {
        let listener = self.finished_init_event.listen();

        unsafe { self.get_unchecked() }
    }
}

impl<T> Default for HybridCell<T> {
    fn default() -> Self {
        Self::new()
    }
}
