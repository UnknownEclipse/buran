use std::{
    collections::{hash_map::RandomState, HashMap},
    fmt::Debug,
    hash::{BuildHasher, Hash},
};

#[cfg(test)]
mod tests;

/// A linked deque for small, cheaply hashable types that stores it's metdata in a
/// companion hash table and uses its items as references rather than pointers.
///
/// This approach is used instead of a traditional linked list-based deque for a few
/// reasons:
///
/// 1. **Safety**
/// This style of structure can be implemented in 100% safe rust.
///
/// 2. **Performance**
/// This is more of an 'it depends'. For small, cheaply hashable types like integers,
/// and using a hash function like fxhash that is meant for such small types, this
/// style is actually faster than a linked-list based alternative (the
/// hashlink crate is used in benchmarks). Of course, for any other situation
/// this will rapidly fall behind, however buran's caches are primarily id->object,
/// where an id is a small 64-bit integer. Perfect!
#[derive(Clone)]
pub struct HashDeque<T, S = RandomState> {
    head: Option<T>,
    tail: Option<T>,
    data: HashMap<T, Links<T>, S>,
}

impl<T, S> Debug for HashDeque<T, S>
where
    T: Copy + Hash + Eq + Debug,
    S: BuildHasher,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();

        let mut cur = self.head;
        while let Some(entry) = cur {
            list.entry(&entry);
            cur = self.links(entry).next;
        }
        list.finish()
    }
}

impl<T> HashDeque<T> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, Default::default())
    }
}

impl<T, S> HashDeque<T, S> {
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            head: None,
            tail: None,
            data: HashMap::with_capacity_and_hasher(capacity, hasher),
        }
    }
}

impl<T, S> HashDeque<T, S>
where
    T: Copy + Eq + Hash,
    S: BuildHasher,
{
    pub fn contains(&self, item: T) -> bool {
        self.data.contains_key(&item)
    }

    pub fn first(&self) -> Option<T> {
        self.head
    }

    pub fn last(&self) -> Option<T> {
        self.tail
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn remove(&mut self, item: T) -> bool {
        let Links { next, prev } = match self.data.remove(&item) {
            Some(links) => links,
            None => return false,
        };
        if let Some(next) = next {
            self.set_prev(next, prev);
        } else {
            self.tail = prev;
        }
        if let Some(prev) = prev {
            self.set_next(prev, next);
        } else {
            self.head = next;
        }
        true
    }

    pub fn push_front(&mut self, item: T) -> bool {
        use std::collections::hash_map::Entry::*;

        match self.data.entry(item) {
            Occupied(_) => return false,
            Vacant(e) => e.insert(Links {
                next: self.head,
                prev: None,
            }),
        };

        if let Some(head) = self.head {
            self.set_prev(head, Some(item));
        } else {
            self.tail = Some(item);
        }
        self.head = Some(item);
        true
    }

    pub fn push_back(&mut self, item: T) -> bool {
        use std::collections::hash_map::Entry::*;

        match self.data.entry(item) {
            Occupied(_) => return false,
            Vacant(e) => e.insert(Links {
                next: None,
                prev: self.tail,
            }),
        };

        if let Some(tail) = self.tail {
            self.set_next(tail, Some(item));
        } else {
            self.head = Some(item);
        }
        self.tail = Some(item);
        true
    }

    pub fn pop_front(&mut self) -> Option<T> {
        let front = self.first()?;
        self.remove(front);
        Some(front)
    }

    pub fn pop_back(&mut self) -> Option<T> {
        let back = self.last()?;
        self.remove(back);
        Some(back)
    }

    fn set_next(&mut self, item: T, next: Option<T>) {
        self.links_mut(item).next = next;
    }

    fn set_prev(&mut self, item: T, prev: Option<T>) {
        self.links_mut(item).prev = prev;
    }

    fn links(&self, item: T) -> &Links<T> {
        self.data.get(&item).expect("broken link")
    }

    fn links_mut(&mut self, item: T) -> &mut Links<T> {
        self.data.get_mut(&item).expect("broken link")
    }

    pub fn iter(&self) -> Iter<'_, T, S> {
        Iter {
            deque: self,
            front: self.head,
            back: self.tail,
        }
    }
}

impl<T, S> Default for HashDeque<T, S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            head: Default::default(),
            tail: Default::default(),
            data: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Links<T> {
    next: Option<T>,
    prev: Option<T>,
}

impl<T> Default for Links<T> {
    fn default() -> Self {
        Self {
            next: Default::default(),
            prev: Default::default(),
        }
    }
}

pub struct Iter<'a, T, S> {
    deque: &'a HashDeque<T, S>,
    front: Option<T>,
    back: Option<T>,
}

impl<'a, T, S> Iterator for Iter<'a, T, S>
where
    T: Copy + Eq + Hash,
    S: BuildHasher,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let front = self.front?;
        self.front = self.deque.links(front).next;
        Some(front)
    }
}

impl<'a, T, S> DoubleEndedIterator for Iter<'a, T, S>
where
    T: Copy + Eq + Hash,
    S: BuildHasher,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let back = self.back?;
        self.back = self.deque.links(back).prev;
        Some(back)
    }
}
