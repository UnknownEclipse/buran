use core::slice;
use std::{
    alloc::{alloc, alloc_zeroed, handle_alloc_error, Layout},
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
};

use bytemuck::Zeroable;

#[derive(Debug)]
pub struct AlignedSliceBuf {
    data: Box<[u8]>,
    align: usize,
}

impl Clone for AlignedSliceBuf {
    fn clone(&self) -> Self {
        let data = clone_slice_aligned(&self.data, self.align);
        Self {
            data,
            align: self.align,
        }
    }
}

impl Deref for AlignedSliceBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for AlignedSliceBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

pub fn alloc_aligned_slice_zeroed<T>(size: usize, align: usize) -> Box<[T]>
where
    T: Zeroable,
{
    unsafe {
        alloc_aligned_slice_helper(size, align, |layout| {
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            ptr.cast()
        })
    }
}

pub fn alloc_aligned_slice<T>(size: usize, align: usize) -> Box<[MaybeUninit<T>]> {
    unsafe {
        alloc_aligned_slice_helper(size, align, |layout| {
            let ptr = alloc(layout);
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            ptr.cast()
        })
    }
}

unsafe fn alloc_aligned_slice_helper<T>(
    size: usize,
    align: usize,
    alloc: impl FnOnce(Layout) -> *mut T,
) -> Box<[T]> {
    let layout = Layout::array::<T>(size)
        .and_then(|layout| layout.align_to(align))
        .unwrap();

    unsafe {
        let ptr = alloc(layout);
        let slice = slice::from_raw_parts_mut(ptr, size);
        Box::from_raw(slice)
    }
}

pub fn clone_slice_aligned<T>(slice: &[T], align: usize) -> Box<[T]>
where
    T: Clone,
{
    let mut new = alloc_aligned_slice::<T>(slice.len(), align);
    let mut items = slice.iter().cloned();
    new.fill_with(|| MaybeUninit::new(items.next().unwrap()));
    unsafe { boxed_slice_assume_init(new) }
}

unsafe fn boxed_slice_assume_init<T>(b: Box<[MaybeUninit<T>]>) -> Box<[T]> {
    let ptr = Box::into_raw(b) as *mut [T];
    unsafe { Box::from_raw(ptr) }
}
