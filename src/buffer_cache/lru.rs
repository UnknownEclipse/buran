use crate::util::index_deque::{self, LruIndexDeque, LruParts};

pub(super) struct Lru {
    links: Box<[index_deque::Link<()>]>,
    parts: LruParts,
}

impl Lru {
    pub fn new(capacity: usize) -> Lru {
        Lru {
            links: vec![Default::default(); capacity].into_boxed_slice(),
            parts: LruParts {
                deque_parts: Default::default(),
                capacity,
                len: 0,
            },
        }
    }

    pub fn access(&mut self, index: usize) {
        self.lru().access(index);
    }

    pub fn insert(&mut self, index: usize) {
        self.lru().insert(index);
    }

    pub fn evict(&mut self) -> Option<usize> {
        self.lru().evict()
    }

    fn lru(&mut self) -> LruIndexDeque<'_, ()> {
        LruIndexDeque {
            links: &mut self.links,
            parts: &mut self.parts,
        }
    }
}
