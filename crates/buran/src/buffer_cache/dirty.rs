use parking_lot::Mutex;

use crate::util::index_deque::{IndexDeque, Link, Parts};

pub struct Dirty {
    inner: Mutex<Inner>,
}

struct Inner {
    parts: Parts,
    links: Box<[Link<()>]>,
}

impl Inner {
    pub fn push(&mut self, index: usize) {
        let mut deque = self.deque();
        deque.remove(index);
        deque.push_back(index);
    }

    pub fn remove(&mut self, index: usize) -> bool {
        self.deque().remove(index)
    }

    pub fn pop(&mut self) -> Option<usize> {
        self.deque().pop_front()
    }

    fn deque(&mut self) -> IndexDeque<'_, ()> {
        IndexDeque {
            links: &mut self.links,
            parts: &mut self.parts,
        }
    }
}
