use std::{
    collections::{hash_map::RandomState, HashMap},
    fmt::Debug,
    hash::{BuildHasher, Hash},
};

use backtrace::Backtrace;
use slab::Slab;

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

#[derive(Clone)]
pub struct HashSlabDeque<T, S = RandomState> {
    head: Option<usize>,
    tail: Option<usize>,
    links: Slab<(T, Links<usize>)>,
    keys: HashMap<T, usize, S>,
}

impl<T, S> Debug for HashSlabDeque<T, S>
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

impl<T> HashSlabDeque<T> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, Default::default())
    }
}

impl<T, S> HashSlabDeque<T, S> {
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            head: None,
            tail: None,
            links: Slab::with_capacity(capacity),
            keys: HashMap::with_capacity_and_hasher(capacity, hasher),
        }
    }
}

impl<T, S> HashSlabDeque<T, S>
where
    T: Clone + Eq + Hash,
    S: BuildHasher,
{
    pub fn first(&self) -> Option<&T> {
        self.head.map(|i| &self.links[i].0)
    }

    pub fn last(&self) -> Option<&T> {
        self.tail.map(|i| &self.links[i].0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn remove(&mut self, item: T) -> bool {
        let key = match self.keys.get(&item) {
            Some(key) => *key,
            None => return false,
        };
        self.remove_key(key).is_some()
    }

    fn remove_key(&mut self, key: usize) -> Option<T> {
        if !self.links.contains(key) {
            return None;
        }

        let (item, Links { next, prev }) = self.links.remove(key);
        self.keys.remove(&item);

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
        Some(item)
    }

    pub fn push_front(&mut self, item: T) -> bool {
        use std::collections::hash_map::Entry::*;

        let key = match self.keys.entry(item.clone()) {
            Occupied(_) => return false,
            Vacant(e) => {
                let links = Links {
                    next: self.head,
                    prev: None,
                };
                let key = self.links.insert((item, links));
                e.insert(key);
                key
            }
        };

        if let Some(next) = self.head {
            self.set_prev(next, Some(key));
        } else {
            self.tail = Some(key);
        }
        self.head = Some(key);
        true
    }

    pub fn push_back(&mut self, item: T) -> bool {
        use std::collections::hash_map::Entry::*;

        let key = match self.keys.entry(item.clone()) {
            Occupied(_) => return false,
            Vacant(e) => {
                let links = Links {
                    next: None,
                    prev: self.tail,
                };
                let key = self.links.insert((item, links));
                e.insert(key);
                key
            }
        };

        if let Some(tail) = self.tail {
            self.set_next(tail, Some(key));
        } else {
            self.head = Some(key);
        }
        self.tail = Some(key);
        true
    }

    pub fn pop_front(&mut self) -> Option<T> {
        let head = self.head?;
        self.remove_key(head)
    }

    pub fn pop_back(&mut self) -> Option<T> {
        let tail = self.tail?;
        self.remove_key(tail)
    }

    fn set_next(&mut self, key: usize, next: Option<usize>) {
        self.links_mut(key).next = next;
    }

    fn set_prev(&mut self, key: usize, prev: Option<usize>) {
        self.links_mut(key).prev = prev;
    }

    fn links(&self, key: usize) -> &Links<usize> {
        &self.links[key].1
    }

    fn links_mut(&mut self, key: usize) -> &mut Links<usize> {
        &mut self.links[key].1
    }
}

impl<T, S> Default for HashSlabDeque<T, S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            head: Default::default(),
            tail: Default::default(),
            keys: Default::default(),
            links: Default::default(),
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
