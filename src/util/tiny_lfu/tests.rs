use super::CacheBuilder;

// #[test]
// fn test_tiny_lfu() {
//     let mut lfu = CacheBuilder {
//      window_fraction:
//     }
//     .build();

//     for _ in 0..2 {
//         assert!(lfu.insert(3).is_none());
//     }

//     lfu.insert(2); // Now in probation
//     lfu.insert(2); // Displaced `3` in protected.

//     dbg!(&lfu);

//     assert!(lfu.insert(0).is_none()); // In window
//     assert_eq!(lfu.insert(17), Some(0)); // Evict 0 from window, but maintain frequencies
//     assert_eq!(lfu.insert(0), Some(17)); // Moved back into the window
//     assert_eq!(lfu.insert(0), Some(3)); // Displace 3 from probation
// }
