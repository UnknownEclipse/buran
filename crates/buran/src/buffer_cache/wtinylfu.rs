use std::{
    convert::TryFrom,
    num::NonZeroU64,
    ops::{BitAnd, BitOr},
};

use nonmax::NonMaxUsize;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::util::{
    count_min::Sketch,
    index_deque::{self, IndexDeque, LruIndexDeque, LruParts},
};

pub struct WTinyLfu {
    links: Box<[index_deque::Link<()>]>,
    regions: Box<[usize]>,

    protected: index_deque::LruParts,
    probation: index_deque::LruParts,
    window: index_deque::Parts,

    /// ### Hashing Algorithm Choice
    /// Due to the small element size, FxHash by far the fastest, however the frequency
    /// sketch is hugely dependent on hash quality, so prefer AHash for now (benchmark
    /// this!!)
    freq_sketch: Sketch<u64, ahash::RandomState>,
    freq_counter: usize,
}

impl WTinyLfu {
    pub fn insert(&mut self, index: usize, get_pgno: impl Fn(usize) -> u64) {
        self.inc_freq(get_pgno(index));

        // Try to insert into protected, if there is room
        if !self.protected().is_full() {
            self.protected().insert(index);
            self.set_region(index, Region::Protected);
            return;
        }

        // If protected is full, try to insert into probation
        if !self.probation().is_full() {
            self.probation().insert(index);
            self.set_region(index, Region::Probation);
            return;
        }

        // Otherwise, we attempt to make room in probation by comparing frequency counts
        // of the would-be evictee and the new item
        self.promote_if_worthy(index, get_pgno);
    }

    pub fn access(&mut self, index: usize, get_pgno: impl Fn(usize) -> u64) {
        self.inc_freq(get_pgno(index));

        match self.region(index) {
            Region::Probation => {
                self.probation().deque().remove(index);
                self.protected().insert(index);
                self.set_region(index, Region::Protected);
                self.balance_protected();
            }
            Region::Protected => self.protected().access(index),
            Region::Window => {
                self.window().remove(index);
                self.promote_if_worthy(index, get_pgno);
            }
        }
    }

    pub fn evict(&mut self) -> Option<usize> {
        let i = self.window().pop_front();
        if i.is_some() {
            return i;
        }
        tracing::warn!("evicting from non-window");
        let i = self.probation().evict();
        if i.is_some() {
            return i;
        }
        self.protected().evict()
    }

    fn promote_if_worthy(&mut self, index: usize, get_pgno: impl Fn(usize) -> u64) -> bool {
        debug_assert!(self.probation().is_full());

        let old_index = self
            .probation()
            .would_evict()
            .expect("probation must not be empty");

        let old = self.freq_sketch.get(&get_pgno(old_index));
        let new = self.freq_sketch.get(&get_pgno(index));

        if new < old {
            self.window().push_back(index);
            self.set_region(index, Region::Window);
            return false;
        }

        let evicted = self
            .probation()
            .evict()
            .expect("probation must not be empty");
        self.window().push_back(evicted);
        self.set_region(evicted, Region::Window);

        self.probation().insert(index);
        self.set_region(evicted, Region::Probation);

        debug_assert!(!self.probation().is_overflowing());
        true
    }

    fn balance_protected(&mut self) {
        while self.protected().is_overflowing() {
            let evictee = self
                .protected()
                .evict()
                .expect("deque must not be empty for it to be overflowing");
            self.probation().insert(evictee);
            self.set_region(evictee, Region::Probation);
        }
        self.balance_probation();
    }

    fn balance_probation(&mut self) {
        while self.probation().is_overflowing() {
            let evictee = self
                .probation()
                .evict()
                .expect("deque must not be empty for it to be overflowing");
            self.window().push_front(evictee);
            self.set_region(evictee, Region::Window);
        }
    }

    fn protected(&mut self) -> LruIndexDeque<'_, ()> {
        LruIndexDeque {
            links: &mut self.links,
            parts: &mut self.protected,
        }
    }

    fn probation(&mut self) -> LruIndexDeque<'_, ()> {
        LruIndexDeque {
            links: &mut self.links,
            parts: &mut self.probation,
        }
    }

    fn window(&mut self) -> IndexDeque<'_, ()> {
        IndexDeque {
            links: &mut self.links,
            parts: &mut self.window,
        }
    }

    fn region(&self, index: usize) -> Region {
        let width = usize::BITS as usize / 2;
        let word = index / width;
        let shift = (index - word * width) * 2;
        let mask = 0b11;
        let word = self.regions[word];
        let val = word.wrapping_shr(shift as u32).bitand(mask) as u8;
        Region::try_from(val).expect("invalid region")
    }

    fn set_region(&mut self, index: usize, region: Region) {
        let width = usize::BITS as usize / 2;
        let word = index / width;
        let shift = (index - word * width) * 2;
        let region: u8 = region.into();
        let region = region as usize;
        let mask = 0b11 << shift;

        self.regions[word] = self.regions[word].bitand(!mask).bitor(region << shift);
    }

    fn inc_freq(&mut self, pgno: u64) {
        self.freq_sketch.increment(&pgno);
        self.freq_counter += 1;

        if self.freq_sketch.sample_size() <= self.freq_counter {
            self.freq_counter = 0;
            self.freq_sketch.halve();
        }
    }

    /// Provide an iterator that provides a best estimation guess of the coldest
    /// elements in the cache. The entries are yielded in order of coldest first.
    pub fn cold(&self) -> Cold<'_> {
        Cold {
            state: self,
            current: self
                .probation
                .deque_parts
                .head
                .and_then(|i| NonMaxUsize::new(i.get() - 1)),
        }
    }
}

pub struct Cold<'a> {
    state: &'a WTinyLfu,
    current: Option<NonMaxUsize>,
}

impl<'a> Iterator for Cold<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let current = self.current?.get();
            if let Some(next) = self.state.links[current].next {
                self.current = NonMaxUsize::new(next.get() - 1);
                return Some(current);
            }

            match self.state.region(current) {
                Region::Probation => {
                    self.current = self
                        .state
                        .probation
                        .deque_parts
                        .head
                        .and_then(|head| NonMaxUsize::new(head.get() - 1));
                }
                Region::Window => return None,
                Region::Protected => {
                    self.current = self
                        .state
                        .window
                        .head
                        .and_then(|head| NonMaxUsize::new(head.get() - 1));
                }
            }
        }
    }
}

pub struct WTinyLfuBuilder {
    window_fraction: f32,
    sample_size_coefficient: usize,
}

impl WTinyLfuBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn window_fraction(&mut self, frac: f32) -> &mut Self {
        assert!((0.0..=1.0).contains(&frac));
        self.window_fraction = frac;
        self
    }

    pub(super) fn build(&self, capacity: usize) -> WTinyLfu {
        let window_size = capacity as f32 * self.window_fraction;
        let main_size = capacity - window_size as usize;

        let probation_size = main_size / 5;
        let protected_size = main_size - probation_size;

        let width = usize::BITS as usize / 2;
        let region_words = (capacity + width - 1) / width;

        let sample_size = capacity * self.sample_size_coefficient;
        let freq_sketch = Sketch::with_hasher(sample_size, Default::default());

        WTinyLfu {
            window: Default::default(),
            probation: LruParts {
                deque_parts: Default::default(),
                capacity: probation_size,
                len: 0,
            },
            protected: LruParts {
                deque_parts: Default::default(),
                capacity: protected_size,
                len: 0,
            },
            links: vec![Default::default(); capacity].into_boxed_slice(),
            regions: vec![0; region_words].into_boxed_slice(),
            freq_sketch,
            freq_counter: 0,
        }
    }
}

impl Default for WTinyLfuBuilder {
    fn default() -> Self {
        Self {
            window_fraction: 0.1,
            sample_size_coefficient: 1,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive)]
enum Region {
    Window,
    Probation,
    Protected,
}
