use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, BuildHasherDefault, Hash},
    iter::repeat_with,
    ops::Range,
};

use buran::util::hash_lru::{HashLru, HashSlabLru};
use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup,
    BenchmarkId, Criterion,
};
use rand::{rngs::ThreadRng, thread_rng, Rng};
use rustc_hash::FxHasher;

const SAMPLE_SIZE: usize = 16384;
const SAMPLE_RANGE: Range<u64> = 0..32_768;

// fn bench(c: &mut Criterion) {
//     let mut group = c.benchmark_group("Lru Caches");

//     let mut rng = thread_rng();

//     let elements = 8192;
//     let spec = (4096, 0..4096, 8192);

//     group.bench_with_input(
//         "HashLink std hasher from empty",
//         &spec,
//         |b, (capacity, range, count)| {
//             b.iter_batched(
//                 || random_vec(&mut rng, *count, range),
//                 |keys| {
//                     let mut lru = hashlink::LruCache::new(*capacity);
//                     for key in keys {
//                         lru.insert(black_box(key), ());
//                     }
//                 },
//                 BatchSize::SmallInput,
//             )
//         },
//     );

//     group.bench_with_input(
//         "HashLru std hasher from empty",
//         &spec,
//         |b, (capacity, range, count)| {
//             b.iter_batched(
//                 || random_vec(&mut rng, *count, range),
//                 |keys| {
//                     let mut lru = HashLru::with_capacity_and_hasher(*capacity, RandomState::new());
//                     for key in keys {
//                         lru.insert(black_box(key));
//                     }
//                 },
//                 BatchSize::SmallInput,
//             )
//         },
//     );

//     group.bench_with_input(
//         "HashLink fxhash from empty",
//         &spec,
//         |b, (capacity, range, count)| {
//             b.iter_batched(
//                 || random_vec(&mut rng, *count, range),
//                 |keys| {
//                     let mut lru =
//                         hashlink::LruCache::<_, _, BuildHasherDefault<FxHasher>>::with_hasher(
//                             *capacity,
//                             BuildHasherDefault::<FxHasher>::default(),
//                         );
//                     for key in keys {
//                         lru.insert(black_box(key), ());
//                     }
//                 },
//                 BatchSize::SmallInput,
//             )
//         },
//     );

//     group.bench_with_input(
//         "HashLru fxhash from empty",
//         &spec,
//         |b, (capacity, range, count)| {
//             b.iter_batched(
//                 || random_vec(&mut rng, *count, range),
//                 |keys| {
//                     let mut lru: HashLru<_, BuildHasherDefault<FxHasher>> =
//                         HashLru::with_capacity_and_hasher(*capacity, Default::default());
//                     for key in keys {
//                         lru.insert(black_box(key));
//                     }
//                 },
//                 BatchSize::SmallInput,
//             )
//         },
//     );

//     group.bench_with_input(
//         "HashLink ahash from empty",
//         &spec,
//         |b, (capacity, range, count)| {
//             b.iter_batched(
//                 || random_vec(&mut rng, *count, range),
//                 |keys| {
//                     let mut lru =
//                         hashlink::LruCache::with_hasher(*capacity, ahash::RandomState::default());
//                     for key in keys {
//                         lru.insert(black_box(key), ());
//                     }
//                 },
//                 BatchSize::SmallInput,
//             )
//         },
//     );

//     group.bench_with_input(
//         "HashLru ahash from empty",
//         &spec,
//         |b, (capacity, range, count)| {
//             b.iter_batched(
//                 || random_vec(&mut rng, *count, range),
//                 |keys| {
//                     let mut lru: HashLru<_, ahash::RandomState> =
//                         HashLru::with_capacity_and_hasher(*capacity, Default::default());
//                     for key in keys {
//                         lru.insert(black_box(key));
//                     }
//                 },
//                 BatchSize::SmallInput,
//             )
//         },
//     );
// }

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Lru Cache");

    type FxHashBuilder = BuildHasherDefault<FxHasher>;
    type AHashBuilder = ahash::RandomState;

    // bench_cache_impl::<hashlink::LruCache<u64, (), RandomState>>(
    //     "hashlink::LruCache<u64, RandomState>",
    //     &mut group,
    // );
    // bench_cache_impl::<hashlink::LruCache<u64, (), FxHashBuilder>>(
    //     "hashlink::LruCache<u64, FxHash>",
    //     &mut group,
    // );
    // bench_cache_impl::<hashlink::LruCache<u64, (), ahash::RandomState>>(
    //     "hashlink::LruCache<u64, AHash>",
    //     &mut group,
    // );

    // bench_cache_impl::<HashLru<u64, RandomState>>("HashLru<u64, RandomState>", &mut group);
    bench_cache_impl::<HashLru<u64, FxHashBuilder>>("HashLru<u64, FxHash>", &mut group);
    // bench_cache_impl::<HashLru<u64, ahash::RandomState>>("HashLru<u64, AHash>", &mut group);

    // bench_cache_impl::<HashSlabLru<u64, RandomState>>("HashSlabLru<u64, RandomState>", &mut group);
    bench_cache_impl::<HashSlabLru<u64, FxHashBuilder>>("HashSlabLru<u64, FxHash>", &mut group);
    bench_cache_impl::<HashSlabLru<u64, ahash::RandomState>>("HashSlabLru<u64, AHash>", &mut group);
}
trait Cache<T> {
    fn new(capacity: usize) -> Self;
    fn insert_key(&mut self, item: T);
}

fn bench_cache_impl<C>(id: &str, group: &mut BenchmarkGroup<'_, WallTime>)
where
    C: Cache<u64>,
{
    let mut rng = thread_rng();

    for capacity in [128, 512, 1024, 4096, 8192, 16384] {
        let id = BenchmarkId::new(id, capacity);
        group.bench_with_input(id, &capacity, |b, &capacity| {
            b.iter_batched(
                || random_vec(&mut rng, SAMPLE_SIZE, &SAMPLE_RANGE),
                |input| {
                    insert_all::<C>(capacity, &input);
                },
                BatchSize::SmallInput,
            )
        });
    }
}

fn insert_all<C>(capacity: usize, keys: &[u64])
where
    C: Cache<u64>,
{
    let mut cache = C::new(capacity);
    for key in keys {
        cache.insert_key(black_box(*key));
    }
}

impl<T, S> Cache<T> for hashlink::LruCache<T, (), S>
where
    T: Hash + Eq,
    S: BuildHasher + Default,
{
    fn new(capacity: usize) -> Self {
        Self::with_hasher(capacity, Default::default())
    }

    fn insert_key(&mut self, item: T) {
        self.insert(item, ());
    }
}

impl<T, S> Cache<T> for HashLru<T, S>
where
    T: Copy + Hash + Eq,
    S: BuildHasher + Default,
{
    fn new(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, Default::default())
    }

    fn insert_key(&mut self, item: T) {
        self.insert(item);
    }
}

impl<T, S> Cache<T> for HashSlabLru<T, S>
where
    T: Copy + Hash + Eq,
    S: BuildHasher + Default,
{
    fn new(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, Default::default())
    }

    fn insert_key(&mut self, item: T) {
        self.insert(item);
    }
}

fn random_vec(rng: &mut ThreadRng, count: usize, range: &Range<u64>) -> Vec<u64> {
    repeat_with(|| rng.gen_range::<u64, _>(range.clone()))
        .take(count)
        .collect()
}

criterion_group!(benches, bench);
criterion_main!(benches);
