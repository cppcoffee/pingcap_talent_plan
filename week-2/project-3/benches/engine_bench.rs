#[macro_use]
extern crate criterion;

use criterion::{Bencher, Criterion};
use rand::Rng;
use tempfile::TempDir;

use kvs::*;

fn common_write<E: KvsEngine>(b: &mut Bencher, store: &mut E) {
    b.iter(move || {
        for i in 0..1000 {
            store.set(format!("key{}", i), "value".to_string()).unwrap();
        }
    })
}

fn write_bench(c: &mut Criterion) {
    c.bench_function("kvs_write", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        common_write(b, &mut store);
    });

    c.bench_function("sled_write", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = SledKvsEngine::open(temp_dir.path()).unwrap();
        common_write(b, &mut store);
    });
}

fn common_read<E: KvsEngine>(b: &mut Bencher, store: &mut E) {
    let mut rng = rand::thread_rng();
    for i in 1..1000 {
        store
            .set(format!("key{}", i), format!("value{}", i))
            .unwrap();
    }
    b.iter(|| {
        let n = rng.gen_range(1, 1000);
        assert_eq!(
            store.get(format!("key{}", n)).unwrap(),
            Some(format!("value{}", n))
        );
    })
}

fn read_bench(c: &mut Criterion) {
    c.bench_function("kvs_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        common_read(b, &mut store);
    });

    c.bench_function("sled_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = SledKvsEngine::open(temp_dir.path()).unwrap();
        common_read(b, &mut store);
    });
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
