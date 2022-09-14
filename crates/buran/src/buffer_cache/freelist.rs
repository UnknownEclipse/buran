use parking_lot::Mutex;

/// Dumb and dirty. Mutex around a stack of indices.
pub struct Freelist {
    inner: Mutex<Inner>,
}

impl Freelist {
    pub fn new_full(size: usize) -> Self {
        let inner = Inner {
            indices: (0..size).collect(),
        };

        Self {
            inner: Mutex::new(inner),
        }
    }

    pub fn pop(&self) -> Option<usize> {
        self.inner.lock().pop()
    }

    pub fn push(&self, index: usize) {
        self.inner.lock().push(index);
    }
}

struct Inner {
    /// Use a stack so we prioritize warmer (more recent) frames.
    indices: Vec<usize>,
}

impl Inner {
    pub fn pop(&mut self) -> Option<usize> {
        self.indices.pop()
    }

    pub fn push(&mut self, index: usize) {
        debug_assert_ne!(self.indices.len(), self.indices.capacity());
        self.indices.push(index);
    }
}
