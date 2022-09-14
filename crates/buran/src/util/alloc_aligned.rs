use std::alloc::{alloc, handle_alloc_error, Layout};

pub fn aligned_vec<T>(capacity: usize, alignment: usize) -> Vec<T> {
    let layout = Layout::array::<T>(capacity)
        .and_then(|arr| arr.align_to(alignment))
        .unwrap();
    unsafe {
        let ptr = alloc(layout);
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        let ptr = ptr.cast();
        Vec::from_raw_parts(ptr, 0, capacity)
    }
}
