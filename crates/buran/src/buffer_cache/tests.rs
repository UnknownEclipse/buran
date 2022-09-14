// use std::{
//     num::{NonZeroU64, NonZeroUsize},
//     sync::{
//         atomic::{AtomicUsize, Ordering},
//         Arc,
//     },
// };

// use dashmap::DashMap;

// use crate::Result;

// use super::CacheConfig;

// const INITIAL_FILL: u8 = 69;

// #[test]
// fn buffer_cache() {
//     let adapter = Arc::new(Adapter::default());

//     let bufcache = CacheConfig {
//         adapter: adapter.clone(),
//         buffer_align: NonZeroUsize::new(1).unwrap(),
//         buffer_size: NonZeroUsize::new(128).unwrap(),
//         capacity: NonZeroUsize::new(2).unwrap(),
//         cache_policy: super::CachePolicy::Lru,
//         cache_access_buffer_capacity: 64,
//     }
//     .build();

//     {
//         let guard = bufcache.get(NonZeroU64::new(5).unwrap()).unwrap();
//         assert!(guard.iter().all(|byte| *byte == INITIAL_FILL));
//     }
//     {
//         let mut guard = bufcache.get_mut(NonZeroU64::new(5).unwrap()).unwrap();
//         guard.fill(66);
//         assert_eq!(adapter.fault_count.load(Ordering::Relaxed), 1);
//     }
//     {
//         let guard = bufcache.get(NonZeroU64::new(5).unwrap()).unwrap();
//         assert!(guard.iter().all(|byte| *byte == 66));
//         assert_eq!(adapter.fault_count.load(Ordering::Relaxed), 1);
//     }
//     {
//         let mut guard = bufcache.get_mut(NonZeroU64::new(6).unwrap()).unwrap();
//         assert!(guard.iter().all(|byte| *byte == INITIAL_FILL));
//         guard.fill(66);
//         assert_eq!(adapter.fault_count.load(Ordering::Relaxed), 2);
//     }
//     {
//         let guard = bufcache.get(NonZeroU64::new(5).unwrap()).unwrap();
//         assert!(guard.iter().all(|byte| *byte == 66));
//         assert_eq!(adapter.fault_count.load(Ordering::Relaxed), 2);
//     }
//     {
//         let guard = bufcache.get(NonZeroU64::new(32).unwrap()).unwrap();
//         assert!(guard.iter().all(|byte| *byte == INITIAL_FILL));
//         assert_eq!(adapter.fault_count.load(Ordering::Relaxed), 3);
//     }
//     {
//         let guard = bufcache.get(NonZeroU64::new(6).unwrap()).unwrap();
//         assert!(guard.iter().all(|byte| *byte == 66));
//         assert_eq!(adapter.fault_count.load(Ordering::Relaxed), 4);
//     }
// }

// #[derive(Default)]
// struct Adapter {
//     blocks: DashMap<NonZeroU64, Vec<u8>>,
//     fault_count: AtomicUsize,
// }

// impl super::Adapter for Adapter {
//     fn read(&self, i: NonZeroU64, buf: &mut [u8]) -> Result<()> {
//         self.fault_count.fetch_add(1, Ordering::Relaxed);

//         if let Some(b) = self.blocks.get(&i) {
//             buf.copy_from_slice(&*b);
//         } else {
//             buf.fill(INITIAL_FILL);
//         }
//         Ok(())
//     }

//     fn write(&self, i: NonZeroU64, buf: &[u8]) -> Result<()> {
//         self.blocks.insert(i, buf.to_owned());
//         Ok(())
//     }

//     fn sync(&self) -> Result<()> {
//         Ok(())
//     }
// }