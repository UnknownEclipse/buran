use ahash::RandomState;
use rand::{thread_rng, Rng};

use crate::util::{hash_deque::HashSlabDeque, hash_lru::HashSlabLru};

use super::HashDeque;

#[test]
fn hash_deque() {
    let mut d = HashDeque::new();

    assert_eq!(d.len(), 0);
    assert!(d.push_front(5));
    assert_eq!(d.len(), 1);
    assert_eq!(d.first(), Some(5));
}
#[test]
fn hash_slab_deque() {
    let mut d = HashSlabDeque::new();

    assert_eq!(d.len(), 0);
    assert!(d.push_front(5));
    assert_eq!(d.len(), 1);
    assert_eq!(d.first(), Some(&5));

    let mut c = HashSlabLru::with_capacity_and_hasher(
        10000,
        std::collections::hash_map::RandomState::new(),
    );

    let mut rng = thread_rng();

    for _ in 0..200000 {
        let v: u64 = rng.gen();
        c.insert(v);
    }
}
