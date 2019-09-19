#[macro_use]
extern crate criterion;

use criterion::Criterion;
use rand::Rng;
use tempfile::TempDir;

use kvs::*;

fn kvs_write_bench(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut store = KvStore::open(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    c.bench_function("kvs_write_bench", |b| {
        b.iter(|| {
            let n = rng.gen_range(1, 1000);
            store.set(format!("key{}", n), "value".to_string()).unwrap();
        })
    });
}

fn kvs_read_bench(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut store = KvStore::open(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    for i in 1..1000 {
        store
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
    }

    c.bench_function("kvs_read_bench", |b| {
        b.iter(|| {
            let n = rng.gen_range(1, 1000);
            assert_eq!(
                store.get(format!("key{}", n)).unwrap(),
                Some(format!("value{}", n))
            );
        })
    });
}

fn sled_write_bench(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut store = KvStore::open(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    c.bench_function("sled_write", move |b| {
        b.iter(|| {
            let n = rng.gen_range(1, 1000);
            store.set(format!("key{}", n), "value".to_string()).unwrap();
        })
    });
}

fn sled_read_bench(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut store = KvStore::open(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    for i in 1..1000 {
        store
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
    }

    c.bench_function("sled_read_bench", |b| {
        b.iter(|| {
            let n = rng.gen_range(1, 1000);
            assert_eq!(
                store.get(format!("key{}", n)).unwrap(),
                Some(format!("value{}", n))
            );
        })
    });
}

criterion_group!(
    benches,
    kvs_write_bench,
    kvs_read_bench,
    sled_write_bench,
    sled_read_bench
);
criterion_main!(benches);
