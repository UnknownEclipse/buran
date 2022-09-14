// pub struct Sketch<T> {}

use std::{
    cmp,
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    marker::PhantomData,
    mem,
};

mod tests;

#[derive(Debug)]
pub struct Sketch<T: ?Sized, S = RandomState> {
    inner: Inner,
    hash_builder: S,
    _p: PhantomData<*const T>,
}

unsafe impl<T, S> Send for Sketch<T, S> where S: Send {}

unsafe impl<T, S> Sync for Sketch<T, S> where S: Send {}

impl<T> Sketch<T>
where
    T: ?Sized,
{
    pub fn new(sample_size: usize) -> Self {
        Self::with_hasher(sample_size, RandomState::new())
    }
}

impl<T, S> Sketch<T, S>
where
    T: ?Sized,
{
    pub fn with_hasher(sample_size: usize, hasher: S) -> Self {
        let inner = Inner::new(sample_size);
        Self {
            inner,
            hash_builder: hasher,
            _p: PhantomData,
        }
    }
}

impl<T, S> Sketch<T, S>
where
    T: ?Sized + Hash,
    S: BuildHasher,
{
    pub fn increment(&mut self, item: &T) {
        let hashes = self.spread(item);
        self.inner.increment(&hashes);
    }

    pub fn get(&self, item: &T) -> u32 {
        let hashes = self.spread(item);
        self.inner.get(&hashes)
    }

    pub fn sample_size(&self) -> usize {
        self.inner.sample_size()
    }

    pub fn halve(&mut self) {
        self.inner.halve();
    }

    fn spread(&self, item: &T) -> [u64; 4] {
        let mut hashes = [0; 4];

        let init = hash_one(item, &self.hash_builder);
        hashes[0] = init;

        let mut hash = init;
        hashes[1..].fill_with(|| {
            hash = hash_one(&hash, &self.hash_builder);
            hash
        });

        hashes
    }
}

fn hash_one<S, T>(item: &T, hash_builder: &S) -> u64
where
    T: ?Sized + Hash,
    S: BuildHasher,
{
    let mut state = hash_builder.build_hasher();
    item.hash(&mut state);
    state.finish()
}

#[derive(Debug)]
struct Inner {
    cells: Box<[Counters]>,
}

impl Inner {
    pub fn new(sample_size: usize) -> Inner {
        let cells = (sample_size + (Counters::WIDTH - 1)) / Counters::WIDTH;
        let cells = vec![Counters::default(); cells].into_boxed_slice();
        Inner { cells }
    }

    pub fn halve(&mut self) {
        for cell in self.cells.iter_mut() {
            cell.halve();
        }
    }

    pub fn height(&self) -> usize {
        4
    }

    pub fn width(&self) -> usize {
        self.cells.len() * Counters::WIDTH / self.height()
    }

    fn index_pair(&self, index: usize) -> (usize, usize) {
        let counters = index / Counters::WIDTH;
        let packed = index % Counters::WIDTH;
        (counters, packed)
    }

    fn to_index(&self, i: usize, j: usize) -> usize {
        j * self.width() + i
    }

    pub fn sample_size(&self) -> usize {
        self.cells.len() * Counters::WIDTH
    }

    pub fn increment(&mut self, hashes: &[u64]) {
        assert_eq!(hashes.len(), self.height());

        for (i, hash) in hashes.iter().copied().enumerate() {
            let (c, l) = self.indices(hash, i);
            self.cells[c].increment(l);
        }
    }

    pub fn get(&self, hashes: &[u64]) -> u32 {
        assert_eq!(hashes.len(), self.height());

        hashes
            .iter()
            .copied()
            .enumerate()
            .map(|(i, hash)| self.get1(hash, i))
            .min()
            .unwrap()
    }

    fn get1(&self, hash: u64, row: usize) -> u32 {
        let (c, l) = self.indices(hash, row);
        self.cells[c].get(l).unwrap()
    }

    fn indices(&self, hash: u64, row: usize) -> (usize, usize) {
        let col = (hash % self.width() as u64) as usize;
        let i = self.to_index(col, row);
        self.index_pair(i)
    }
}

const COUNTER_BITS: u32 = 4;

#[derive(Debug, Clone, Copy, Default)]
struct Counters {
    bits: usize,
}

impl Counters {
    const WIDTH: usize = mem::size_of::<usize>() * 2;

    pub fn get(&self, index: usize) -> Option<u32> {
        let shift = self.shift(index)?;
        let shifted = self.bits.wrapping_shr(shift);
        Some((shifted & 0xf) as u32)
    }

    pub fn set(&mut self, index: usize, value: u32) -> bool {
        if Self::WIDTH <= index {
            return false;
        }
        let shift = self.shift(index).unwrap();
        let mask = !(0xf << shift);
        let bits = self.bits & mask;
        let value = cmp::min(0xf, value) as usize;
        let value = value << shift;
        self.bits = value | bits;
        true
    }

    pub fn increment(&mut self, index: usize) {
        if let Some(old) = self.get(index) {
            self.set(index, old + 1);
        }
    }

    pub fn shift(&self, index: usize) -> Option<u32> {
        Self::WIDTH.gt(&index).then(|| index as u32 * COUNTER_BITS)
    }

    pub fn halve(&mut self) {
        const ONES: usize = usize::from_ne_bytes([0b10001000; mem::size_of::<usize>()]);
        let bits = self.bits.wrapping_shr(1);
        self.bits = bits & !ONES
    }
}
