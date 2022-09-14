use std::{
    alloc::{alloc, Layout},
    mem::MaybeUninit,
    num::NonZeroUsize,
};

use thiserror::Error;

// pub mod aligned_slice;
// pub mod alloc_aligned;
pub mod count_min;
pub mod endian;
pub mod future_cell;
// pub mod hash;
pub mod hash_deque;
pub mod hash_lru;
// pub mod hybrid_cell;
pub mod index_deque;
// pub mod linked_list;
// pub mod slice;
// pub mod thread_id;
// pub mod tiny_lfu;

/// # Safety
/// The contents of b must be fully initialized.
pub unsafe fn boxed_slice_assume_init<T>(b: Box<[MaybeUninit<T>]>) -> Box<[T]> {
    unsafe { Box::from_raw(Box::into_raw(b) as *mut [T]) }
}

pub struct BoxBuilder {
    align: Option<NonZeroUsize>,
}

#[non_exhaustive]
#[derive(Debug, Error)]
#[error("allocation failed")]
pub struct AllocError;

impl BoxBuilder {
    pub fn try_slice_uninit<T>(&self, size: usize) -> Result<Box<[MaybeUninit<T>]>, AllocError> {
        unsafe {
            let mut layout = Layout::array::<T>(size).map_err(|_| AllocError)?;
            if let Some(align) = self.align {
                layout = layout.align_to(align.get()).map_err(|_| AllocError)?;
            }
            let ptr = alloc(layout);
            if ptr.is_null() {}
        }
        todo!()
    }
}

pub fn slice_as_uninit<T>(slice: &[T]) -> &[MaybeUninit<T>] {
    unsafe { &*(slice as *const [_] as *const [_]) }
}

pub fn slice_as_uninit_mut<T>(slice: &mut [T]) -> &mut [MaybeUninit<T>] {
    unsafe { &mut *(slice as *mut [_] as *mut [_]) }
}

/// # Safety
/// Contents of slice must be initialized.
pub unsafe fn assume_init_slice<T>(slice: &[MaybeUninit<T>]) -> &[T] {
    unsafe { &*(slice as *const [_] as *const [_]) }
}

/// # Safety
/// Contents of slice must be initialized.
pub unsafe fn assume_init_slice_mut<T>(slice: &mut [MaybeUninit<T>]) -> &mut [T] {
    unsafe { &mut *(slice as *mut [_] as *mut [_]) }
}
