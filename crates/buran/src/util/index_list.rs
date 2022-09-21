use std::sync::Arc;

use nonmax::NonMaxUsize;

#[cfg(test)]
mod tests;

pub struct IndexList<L> {
    pub head: Option<NonMaxUsize>,
    pub tail: Option<NonMaxUsize>,
    pub links: Arc<[L]>,
}

impl<L> IndexList<L>
where
    L: Link,
{
    pub fn push_back(&mut self, index: usize) {
        if let Some(prev) = self.tail {
            self.links[index].set_prev(Some(prev));
            self.links[prev.get()].set_next(NonMaxUsize::new(index));
        } else {
            self.head = NonMaxUsize::new(index);
        }
        self.tail = NonMaxUsize::new(index);
    }

    pub fn push_front(&mut self, index: usize) {
        if let Some(next) = self.head {
            self.links[index].set_next(Some(next));
            self.links[next.get()].set_prev(NonMaxUsize::new(index));
        } else {
            self.tail = NonMaxUsize::new(index);
        }
        self.head = NonMaxUsize::new(index);
    }

    pub fn remove(&mut self, index: usize) {
        let link = &self.links[index];
        let next = link.next();
        let prev = link.prev();

        if let Some(next) = next {
            self.links[next.get()].set_prev(prev);
        } else {
            self.tail = prev;
        }

        if let Some(prev) = prev {
            self.links[prev.get()].set_next(next);
        } else {
            self.head = next;
        }
    }

    pub fn pop_front(&mut self) -> Option<usize> {
        let front = self.head?;
        let next = self.links[front.get()].next();

        if let Some(next) = next {
            self.links[next.get()].set_prev(None);
        } else {
            self.tail = None;
        }
        self.head = next;

        Some(front.get())
    }

    pub fn pop_back(&mut self) -> Option<usize> {
        let back = self.tail?.get();
        let prev = self.links[back].prev();

        if let Some(prev) = prev {
            self.links[prev.get()].set_next(None);
        } else {
            self.head = None;
        }
        self.tail = prev;

        Some(back)
    }

    pub fn first(&self) -> Option<usize> {
        self.head.map(|v| v.get())
    }

    pub fn last(&self) -> Option<usize> {
        self.tail.map(|v| v.get())
    }

    pub fn iter(&self) -> Iter<'_, L> {
        Iter {
            front: self.head,
            back: self.tail,
            list: self,
        }
    }
}

pub trait Link {
    fn next(&self) -> Option<NonMaxUsize>;
    fn prev(&self) -> Option<NonMaxUsize>;
    fn set_next(&self, next: Option<NonMaxUsize>);
    fn set_prev(&self, prev: Option<NonMaxUsize>);
}

pub struct Iter<'a, L> {
    front: Option<NonMaxUsize>,
    back: Option<NonMaxUsize>,
    list: &'a IndexList<L>,
}

impl<'a, L> Iterator for Iter<'a, L>
where
    L: Link,
{
    type Item = &'a L;

    fn next(&mut self) -> Option<Self::Item> {
        let front = self.front?;
        let item = &self.list.links[front.get()];
        self.front = item.next();
        Some(item)
    }
}

impl<'a, L> DoubleEndedIterator for Iter<'a, L>
where
    L: Link,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let back = self.back?;
        let item = &self.list.links[back.get()];
        self.back = item.prev();
        Some(item)
    }
}
