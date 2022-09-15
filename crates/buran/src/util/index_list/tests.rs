use std::{cell::Cell, sync::Arc};

use nonmax::NonMaxUsize;

use super::IndexList;

#[test]
fn index_list() {
    let links: Arc<[ListHead]> = (0..100)
        .map(|i| ListHead {
            value: i,
            ..Default::default()
        })
        .collect();

    let mut list = IndexList {
        head: None,
        tail: None,
        links,
    };
    assert!(list.first().is_none());
    assert!(list.last().is_none());
    assert!(list.pop_front().is_none());
    assert!(list.pop_back().is_none());
    list.push_front(0);
    assert_eq!(list.first(), Some(0));
    assert_eq!(list.last(), Some(0));
    assert_eq!(list.pop_back(), Some(0));
    assert_eq!(list.pop_front(), None);
    list.push_front(0);
    assert_eq!(list.pop_front(), Some(0));
    assert_eq!(list.pop_back(), None);

    list.push_back(0);
    list.push_front(1);
    assert_eq!(list.first(), Some(1));
    assert_eq!(list.last(), Some(0));
}

#[derive(Debug, Default)]
struct ListHead {
    value: i32,
    next: Cell<Option<NonMaxUsize>>,
    prev: Cell<Option<NonMaxUsize>>,
}

impl super::Link for ListHead {
    fn next(&self) -> Option<NonMaxUsize> {
        self.next.get()
    }

    fn prev(&self) -> Option<NonMaxUsize> {
        self.prev.get()
    }

    fn set_next(&self, next: Option<NonMaxUsize>) {
        self.next.set(next);
    }

    fn set_prev(&self, prev: Option<NonMaxUsize>) {
        self.prev.set(prev);
    }
}
