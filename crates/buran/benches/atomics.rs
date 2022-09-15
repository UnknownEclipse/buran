use core::num;
use std::{
    ops::{Add, Mul},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    thread,
};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use parking_lot::Mutex;

const ITERATIONS: usize = 50_000;

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Atomic Contention");

    for i in 0..thread::available_parallelism()
        .unwrap()
        .get()
        .mul(4)
        .trailing_zeros()
        .add(1)
    {
        let num_threads = 1 << i;

        group.bench_with_input(
            BenchmarkId::new("atomic", num_threads),
            &num_threads,
            |b, num_threads| {
                b.iter(|| {
                    let atomic = AtomicUsize::new(0);

                    thread::scope(|s| {
                        for _ in 0..*num_threads {
                            s.spawn(|| {
                                for _ in 0..ITERATIONS / *num_threads {
                                    atomic.fetch_add(1, Ordering::Relaxed);
                                }
                            });
                        }
                    });
                    black_box(atomic);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mutex", num_threads),
            &num_threads,
            |b, num_threads| {
                b.iter(|| {
                    let lock = Mutex::new(0);

                    thread::scope(|s| {
                        for _ in 0..*num_threads {
                            s.spawn(|| {
                                for _ in 0..ITERATIONS / *num_threads {
                                    *lock.lock() += 1;
                                }
                            });
                        }
                    });
                    black_box(lock);
                })
            },
        );
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
