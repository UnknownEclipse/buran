use crate::util::hash_deque::HashDeque;

#[test]
fn hash_deque() {
    let mut d = HashDeque::new();

    assert_eq!(d.len(), 0);
    assert!(d.push_front(5));
    assert_eq!(d.len(), 1);
    assert_eq!(d.first(), Some(5));
}
