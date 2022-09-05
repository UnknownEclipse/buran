use crate::util::index_deque::{self, IndexDeque};

#[derive(Debug, Default)]
pub(super) struct Lru {
    links: Box<[index_deque::Link<()>]>,
    parts: index_deque::Parts,
}

impl Lru {
    pub fn access(&mut self, index: usize) {
        let mut d = self.deque();
        d.remove(index);
        d.push_back(index);
    }

    pub fn insert(&mut self, index: usize) {
        self.deque().push_back(index);
    }

    pub fn evict(&mut self) -> Option<usize> {
        self.deque().pop_front()
    }

    fn deque(&mut self) -> IndexDeque<'_, ()> {
        IndexDeque {
            links: &mut self.links,
            parts: &mut self.parts,
        }
    }
}
