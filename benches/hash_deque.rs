use std::{
    collections::hash_map::RandomState, hash::BuildHasherDefault, iter::repeat_with, ops::Range,
};

use buran::util::hash_lru::HashLru;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{rngs::ThreadRng, thread_rng, Rng};
use rustc_hash::FxHasher;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Lru Caches");

    let mut rng = thread_rng();

    let spec = (4096, 0..4096, 8192);

    group.bench_with_input(
        "HashLink std hasher from empty",
        &spec,
        |b, (capacity, range, count)| {
            b.iter_batched(
                || random_vec(&mut rng, *count, range),
                |keys| {
                    let mut lru = hashlink::LruCache::new(*capacity);
                    for key in keys {
                        lru.insert(black_box(key), ());
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        },
    );

    group.bench_with_input(
        "HashLru std hasher from empty",
        &spec,
        |b, (capacity, range, count)| {
            b.iter_batched(
                || random_vec(&mut rng, *count, range),
                |keys| {
                    let mut lru = HashLru::with_capacity_and_hasher(*capacity, RandomState::new());
                    for key in keys {
                        lru.insert(black_box(key));
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        },
    );

    group.bench_with_input(
        "HashLink fxhash from empty",
        &spec,
        |b, (capacity, range, count)| {
            b.iter_batched(
                || random_vec(&mut rng, *count, range),
                |keys| {
                    let mut lru =
                        hashlink::LruCache::<_, _, BuildHasherDefault<FxHasher>>::with_hasher(
                            *capacity,
                            BuildHasherDefault::<FxHasher>::default(),
                        );
                    for key in keys {
                        lru.insert(black_box(key), ());
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        },
    );

    group.bench_with_input(
        "HashLru fxhash from empty",
        &spec,
        |b, (capacity, range, count)| {
            b.iter_batched(
                || random_vec(&mut rng, *count, range),
                |keys| {
                    let mut lru: HashLru<_, BuildHasherDefault<FxHasher>> =
                        HashLru::with_capacity_and_hasher(*capacity, Default::default());
                    for key in keys {
                        lru.insert(black_box(key));
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        },
    );
}

fn random_vec(rng: &mut ThreadRng, count: usize, range: &Range<u64>) -> Vec<u64> {
    repeat_with(|| rng.gen_range::<u64, _>(range.clone()))
        .take(count)
        .collect()
}

criterion_group!(benches, bench);
criterion_main!(benches);
