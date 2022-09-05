use std::num::NonZeroUsize;

pub struct IndexDeque<'a, T> {
    pub links: &'a mut [Link<T>],
    pub parts: &'a mut Parts,
}

impl<'a, T> IndexDeque<'a, T>
where
    T: Copy,
{
    pub fn first(&self) -> Option<usize> {
        self.parts.head.map(|v| v.get() - 1)
    }

    pub fn last(&self) -> Option<usize> {
        self.parts.tail.map(|v| v.get() - 1)
    }

    pub fn remove(&mut self, index: usize) -> bool {
        let Link { next, prev, .. } = self.links[index];

        if let Some(next) = next {
            self.set_prev(next.get() - 1, prev);
        } else {
            self.parts.tail = prev;
        }
        if let Some(prev) = prev {
            self.set_next(prev.get() - 1, next);
        } else {
            self.parts.head = next;
        }
        true
    }

    pub fn push_front(&mut self, index: usize) {
        if let Some(head) = self.parts.head {
            self.set_prev(head.get() - 1, NonZeroUsize::new(index + 1));
        } else {
            self.parts.tail = NonZeroUsize::new(index + 1);
        }
        self.parts.head = NonZeroUsize::new(index + 1);
    }

    pub fn push_back(&mut self, index: usize) {
        if let Some(tail) = self.parts.tail {
            self.set_next(tail.get() - 1, NonZeroUsize::new(index + 1));
        } else {
            self.parts.head = NonZeroUsize::new(index + 1);
        }
        self.parts.tail = NonZeroUsize::new(index + 1);
    }

    pub fn pop_front(&mut self) -> Option<usize> {
        let front = self.first()?;
        self.remove(front);
        Some(front)
    }

    pub fn pop_back(&mut self) -> Option<usize> {
        let back = self.last()?;
        self.remove(back);
        Some(back)
    }

    fn set_next(&mut self, index: usize, next: Option<NonZeroUsize>) {
        self.links[index].next = next;
    }

    fn set_prev(&mut self, index: usize, prev: Option<NonZeroUsize>) {
        self.links[index].prev = prev;
    }
}

#[derive(Debug, Default)]
pub struct Parts {
    pub head: Option<NonZeroUsize>,
    pub tail: Option<NonZeroUsize>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Link<T> {
    pub data: T,
    pub next: Option<NonZeroUsize>,
    pub prev: Option<NonZeroUsize>,
}

pub struct LruParts {
    pub deque_parts: Parts,
    pub capacity: usize,
    pub len: usize,
}

pub struct LruIndexDeque<'a, T> {
    pub links: &'a mut [Link<T>],
    pub parts: &'a mut LruParts,
}

impl<'a, T> LruIndexDeque<'a, T>
where
    T: Copy,
{
    pub fn access(&mut self, index: usize) {
        let mut d = self.deque();
        d.remove(index);
        d.push_back(index);
    }

    pub fn insert(&mut self, index: usize) {
        self.deque().push_back(index);
        self.parts.len += 1;
    }

    pub fn would_evict(&mut self) -> Option<usize> {
        self.deque().first()
    }

    pub fn evict(&mut self) -> Option<usize> {
        let index = self.deque().pop_front()?;
        self.parts.len -= 1;
        Some(index)
    }

    pub fn is_full(&self) -> bool {
        self.parts.capacity <= self.parts.len
    }

    pub fn is_overflowing(&self) -> bool {
        self.parts.capacity < self.parts.len
    }

    pub fn deque(&mut self) -> IndexDeque<'_, T> {
        IndexDeque {
            links: self.links,
            parts: &mut self.parts.deque_parts,
        }
    }
}
