use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tempfile::TempDir;

fn write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_bench");
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || KvStore::open(TempDir::new().unwrap().path()).unwrap(),
            |mut store| {
                for i in 1..100000 {
                    store.set(format!("key{}", i), "value".to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || SledKvsEngine::open(TempDir::new().unwrap().path()).unwrap(),
            |mut store| {
                for i in 1..100000 {
                    store.set(format!("key{}", i), "value".to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn read_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_bench");
    for i in &vec![8, 12, 16, 20] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, i| {
            let mut store = KvStore::open(TempDir::new().unwrap().path()).unwrap();
            for key in 1..(1 << i) {
                store
                    .set(format!("key{}", key), "value".to_owned())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            });
        });
    }
    for i in &vec![8, 12, 16, 20] {
        group.bench_with_input(format!("sled_{}", i), i, |b, i| {
            let mut store = SledKvsEngine::open(TempDir::new().unwrap().path()).unwrap();
            for key in 1..(1 << i) {
                store
                    .set(format!("key{}", key), "value".to_owned())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
