use super::{Counters, Sketch};

#[test]
fn count_min() {
    let mut sketch = Sketch::new(16);

    for i in 0..15 {
        sketch.increment(&5);
        assert_eq!(sketch.get(&5), i + 1);
    }
    sketch.increment(&5);
    assert_eq!(sketch.get(&5), 15);
}

#[test]
fn counters() {
    let mut c = Counters::default();

    // All start zeroed
    for i in 0..Counters::WIDTH {
        assert_eq!(c.get(i), Some(0));
    }

    c.increment(5);
    for i in 0..Counters::WIDTH {
        let expected = if i == 5 { 1 } else { 0 };
        assert_eq!(c.get(i), Some(expected));
    }

    // Saturates to 0xf (15)
    for _ in 0..15 {
        c.increment(5);
    }
    assert_eq!(c.get(5), Some(15));

    c.halve();
    for i in 0..Counters::WIDTH {
        let expected = if i == 5 { 15 / 2 } else { 0 };
        assert_eq!(c.get(i), Some(expected));
    }
}
