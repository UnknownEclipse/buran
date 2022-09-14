use std::hash::BuildHasherDefault;

use bitvec::{prelude::BitBox, vec::BitVec};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use rustc_hash::FxHasher;

use crate::util::{
    hash_lru::HashLru,
    index_deque::{self, LruIndexDeque, LruParts},
};

pub(super) struct Lru2Q {
    links: Box<[index_deque::Link<()>]>,
    is_am: BitBox, // true => Region::Am, false => Region::A1In
    am: index_deque::LruParts,
    a1in: index_deque::LruParts,
    // TODO: Use a more efficient data structure here.
    a1out: HashLru<u64, BuildHasherDefault<FxHasher>>,
}

impl Lru2Q {
    pub fn access(&mut self, index: usize) {
        let region = self.region(index);

        match region {
            Region::Am => {
                self.am().access(index);
            }
            Region::A1In => {
                self.a1in().access(index);
            }
        }
    }

    pub fn insert(&mut self, index: usize, get_pgno: impl Fn(usize) -> u64) {
        let (mut list, region) = if self.a1out.deque.contains(get_pgno(index)) {
            (self.am(), Region::Am)
        } else {
            (self.a1in(), Region::A1In)
        };

        list.insert(index);
        self.set_region(index, region);
    }

    pub fn evict(&mut self, get_pgno: impl Fn(usize) -> u64) -> Option<usize> {
        if self.a1in().is_overflowing() {
            let e = self.a1in().evict().expect("a1in is overflowing");
            self.a1out.insert(get_pgno(e));
            Some(e)
        } else {
            self.am().evict()
        }
    }

    fn am(&mut self) -> LruIndexDeque<'_, ()> {
        LruIndexDeque {
            links: &mut self.links,
            parts: &mut self.am,
        }
    }

    fn a1in(&mut self) -> LruIndexDeque<'_, ()> {
        LruIndexDeque {
            links: &mut self.links,
            parts: &mut self.a1in,
        }
    }

    fn region(&self, index: usize) -> Region {
        if self.is_am.get(index).map(|b| *b).unwrap_or(false) {
            Region::Am
        } else {
            Region::A1In
        }
    }

    fn set_region(&mut self, index: usize, region: Region) {
        let bit = region == Region::Am;
        self.is_am.set(index, bit);
    }
}

pub struct Lru2QBuilder {
    pub(super) capacity: usize,
    k_a1in: f32,
    k_a1out: f32,
}

impl Lru2QBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub(super) fn build(&self) -> Lru2Q {
        let a1in_cap = self.capacity as f32 * self.k_a1in;
        let a1in_cap = a1in_cap.floor() as usize;

        let am_cap = self.capacity - a1in_cap;

        let a1out_capacity = (self.capacity as f32 * self.k_a1out) as usize;
        let a1out = HashLru::with_capacity_and_hasher(a1out_capacity, Default::default());

        // let mut is_am = BitVec::<_,bitvec::>::from_element(false);
        let mut is_am = BitVec::new();
        is_am.resize(self.capacity, false);
        let is_am = is_am.into_boxed_bitslice();

        let links = vec![Default::default(); self.capacity].into_boxed_slice();

        Lru2Q {
            a1in: LruParts {
                capacity: a1in_cap,
                deque_parts: Default::default(),
                len: 0,
            },
            am: LruParts {
                capacity: am_cap,
                deque_parts: Default::default(),
                len: 0,
            },
            is_am,
            a1out,
            links,
        }
    }
}

impl Default for Lru2QBuilder {
    fn default() -> Self {
        Self {
            capacity: Default::default(),
            k_a1in: 0.25,
            k_a1out: 0.50,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
enum Region {
    Am,
    A1In,
}
