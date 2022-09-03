use std::{
    cmp,
    collections::{hash_map::RandomState, HashMap},
    fmt::Debug,
    hash::{BuildHasher, BuildHasherDefault, Hash},
};

use rustc_hash::FxHasher;
use smallvec::SmallVec;

use super::{count_min::Sketch, hash_deque::HashDeque};

#[cfg(test)]
mod tests;

pub struct CacheBuilder {
    pub capacity: usize,
    pub window_fraction: f32,
    pub sketch_coefficient: f32,
}

impl CacheBuilder {
    pub fn build<T: Copy>(&self) -> TinyLfu<T> {
        self.build_with_hasher(Default::default())
    }

    pub fn build_with_hasher<T, S>(&self, hasher: S) -> TinyLfu<T, S>
    where
        T: Copy,
        S: Clone,
    {
        assert!(2 <= self.capacity);
        assert!(self.window_fraction <= 1.0);
        assert!(self.sketch_coefficient.is_finite() && self.sketch_coefficient.is_sign_positive());

        let (window_size, probation_size, protected_size) =
            compute_cache_sizes(self.capacity, self.window_fraction);

        let window = Lru::with_capacity_and_hasher(window_size, hasher.clone());
        let probation = Lru::with_capacity_and_hasher(probation_size, hasher.clone());
        let protected = Lru::with_capacity_and_hasher(protected_size, hasher.clone());

        let regions = HashMap::with_capacity_and_hasher(self.capacity, hasher.clone());

        let sample_size = self.capacity as f32 * self.sketch_coefficient;
        let freq_sketch = Sketch::with_hasher(sample_size as usize, hasher);

        TinyLfu {
            window,
            probation,
            protected,
            regions,
            freq_sketch,
            counter: 0,
            capacity: self.capacity,
        }
    }
}

pub struct TinyLfu<T: Copy, S = BuildHasherDefault<FxHasher>> {
    window: Lru<T, S>,
    probation: Lru<T, S>,
    protected: Lru<T, S>,

    regions: HashMap<T, Region, S>,

    freq_sketch: Sketch<T, S>,
    counter: usize,
    capacity: usize,
}

impl<T, S> TinyLfu<T, S>
where
    T: Copy + Hash + Eq,
    S: BuildHasher,
{
    pub fn insert(&mut self, item: T) -> Option<T> {
        self.weigher_access(item);

        match self.regions.get(&item) {
            Some(Region::Protected) => {
                let evictee = self.protected.insert(item);
                debug_assert!(evictee.is_none());
                None
            }
            Some(Region::Probation) => {
                let removed = self.probation.deque.remove(item);
                debug_assert!(removed);

                let demoted = self.protected.insert(item);
                if let Some(demoted) = demoted {
                    self.regions.insert(demoted, Region::Probation);
                    let evictee = self.probation.insert(demoted);
                    debug_assert!(evictee.is_none());
                }
                self.regions.insert(item, Region::Protected);
                None
            }
            Some(Region::Window) => {
                if let Some(evictee) = self.try_admit(item) {
                    self.window.deque.remove(item);
                    self.regions.remove(&evictee);
                    Some(evictee)
                } else {
                    let evictee = self.window.insert(item);
                    debug_assert!(evictee.is_none());
                    None
                }
            }
            None => {
                if self.protected.try_insert(item) {
                    self.regions.insert(item, Region::Protected);
                    return None;
                }
                if self.probation.try_insert(item) {
                    self.regions.insert(item, Region::Probation);
                    return None;
                }
                let evictee = self.try_admit(item).or_else(|| {
                    self.regions.insert(item, Region::Window);
                    self.window.insert(item)
                });
                if let Some(e) = evictee {
                    self.regions.remove(&e);
                }
                evictee
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn set_window_fraction(&mut self, frac: f32) -> Vec<T> {
        let (window_size, probation_size, protected_size) =
            compute_cache_sizes(self.capacity, frac);

        let mut evicted = Vec::new();

        // Entries evicted due to the window shrinking are evicted normally
        evicted.extend(self.window.set_capacity(window_size));
        // Entries evicted due to the probation shrinking are evicted normally
        evicted.extend(self.probation.set_capacity(probation_size));
        // Entries evicted due to the protected shrinking are evicted to the probation
        // entries that are in turn evicted from the probation are evicted.
        evicted.extend(
            self.protected
                .set_capacity(protected_size)
                .into_iter()
                .flat_map(|e| self.probation.insert(e)),
        );

        evicted
    }

    fn weigher_access(&mut self, item: T) {
        self.freq_sketch.increment(&item);
        self.counter += 1;
        if self.freq_sketch.sample_size() <= self.counter {
            self.counter = 0;
            self.freq_sketch.halve();
        }
    }

    fn weigher_should_replace(&self, maybe_evictee: T, candidate: T) -> bool {
        let a = self.freq_sketch.get(&maybe_evictee);
        let b = self.freq_sketch.get(&candidate);
        a < b
    }

    /// Access an item not in the main cache when the main is full.
    fn try_admit(&mut self, item: T) -> Option<T> {
        let evictee = self
            .probation
            .would_evict()
            .expect("probation must not be empty");

        if self.weigher_should_replace(evictee, item) {
            let evictee = self.probation.insert(item).unwrap();
            self.regions.insert(item, Region::Probation);
            self.regions.remove(&evictee);
            Some(evictee)
        } else {
            None
        }
    }
}

impl<T, S> Debug for TinyLfu<T, S>
where
    T: Debug + Copy + Hash + Eq,
    S: Debug + BuildHasher,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TinyLfu")
            .field("window", &self.window)
            .field("probation", &self.probation)
            .field("protected", &self.protected)
            .field("regions", &self.regions)
            .field("freq_sketch", &self.freq_sketch)
            .field("counter", &self.counter)
            .finish()
    }
}

pub struct HillClimberTinyLfu<T, S = RandomState>
where
    T: Copy,
{
    inner: TinyLfu<T, S>,
    prev_hits: usize,
    hits: usize,
    frac: f32,
    delta: f32,
    cycle_size: usize,
    counter: usize,
}

impl<T, S> HillClimberTinyLfu<T, S>
where
    T: Copy + Hash + Eq,
    S: BuildHasher,
{
    pub fn insert(&mut self, item: T) -> impl Iterator<Item = T> {
        let mut evicted = SmallVec::<[T; 1]>::new();

        self.counter += 1;
        if self.cycle_size <= self.counter {
            let cmp = self.hits.cmp(&self.prev_hits);

            if cmp != cmp::Ordering::Equal {
                self.delta *= if cmp == cmp::Ordering::Less {
                    -1.0
                } else {
                    1.0
                };
                self.frac += self.delta;
                self.prev_hits = self.hits;
                self.hits = 0;
                evicted.extend(self.inner.set_window_fraction(self.frac));
            }
            self.counter = 0;
        }
        evicted.extend(self.inner.insert(item));
        evicted.into_iter()
    }
}

struct Lru<T, S> {
    deque: HashDeque<T, S>,
    capacity: usize,
}

impl<T, S> Lru<T, S>
where
    T: Copy,
{
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Lru<T, S> {
        let deque = HashDeque::with_capacity_and_hasher(capacity, hasher);
        Lru { deque, capacity }
    }
}

impl<T, S> Lru<T, S>
where
    T: Copy + Hash + Eq,
    S: BuildHasher,
{
    pub fn is_full(&self) -> bool {
        self.capacity <= self.deque.len()
    }

    pub fn insert(&mut self, item: T) -> Option<T> {
        self.deque.remove(item);
        self.deque.push_front(item);
        if self.deque.len() > self.capacity {
            self.deque.pop_back()
        } else {
            None
        }
    }

    pub fn try_insert(&mut self, item: T) -> bool {
        let full = self.is_full();
        if !full {
            self.deque.push_front(item);
        }
        !full
    }

    pub fn would_evict(&self) -> Option<T> {
        if self.deque.len() == self.capacity {
            self.deque.last()
        } else {
            None
        }
    }

    pub fn set_capacity(&mut self, capacity: usize) -> Vec<T> {
        let mut evicted = Vec::new();
        while capacity < self.deque.len() {
            evicted.extend(self.deque.pop_front());
        }
        self.capacity = capacity;
        evicted
    }
}

impl<T, S> Debug for Lru<T, S>
where
    T: Debug + Copy + Hash + Eq,
    S: BuildHasher,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lru")
            .field("capacity", &self.capacity)
            .field("deque", &self.deque)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Region {
    Probation,
    Protected,
    Window,
}

fn compute_cache_sizes(capacity: usize, window_fraction: f32) -> (usize, usize, usize) {
    let window_size = (capacity as f32 * window_fraction) as usize;
    let main_size = capacity - window_size;

    let probation_size = cmp::min(1, main_size / 5);
    let protected_size = main_size - probation_size;

    (window_size, probation_size, protected_size)
}
