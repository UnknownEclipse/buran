use std::{
    fmt::Debug,
    hash::{BuildHasher, Hash},
};

use super::hash_deque::HashDeque;

/// A simple Lru cache based on a [HashDeque].
pub struct HashLru<T, S> {
    deque: HashDeque<T, S>,
    capacity: usize,
}

impl<T, S> HashLru<T, S>
where
    T: Copy,
{
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> HashLru<T, S> {
        let deque = HashDeque::with_capacity_and_hasher(capacity, hasher);
        HashLru { deque, capacity }
    }
}

impl<T, S> HashLru<T, S>
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

impl<T, S> Debug for HashLru<T, S>
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
