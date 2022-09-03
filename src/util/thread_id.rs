use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ThreadId(NonZeroU64);

impl ThreadId {
    pub fn current() -> ThreadId {
        thread_local! {
            static OBJECT: u8 = 0;
        }
        let key = OBJECT.with(|obj| obj as *const u8 as usize);
        ThreadId(NonZeroU64::new(key as u64).unwrap())
    }

    pub fn as_u64(&self) -> NonZeroU64 {
        self.0
    }
}

// fn new_id() -> NonZeroU64 {
//     static COUNTER: AtomicU64 = AtomicU64::new(1);
//     let id = COUNTER.fetch_add(1, Ordering::Relaxed);
//     NonZeroU64::new(id).expect("thread id counter overflow")
// }
