use carbonmap::CarbonMap;
use criterion::{criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use parking_lot::RwLock;

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

/* ---------------- Setup Helpers ---------------- */

fn setup_carbonmap(n: usize) -> Arc<CarbonMap<u64, u64>> {
    let map = Arc::new(CarbonMap::new());

    for i in 0..n as u64 {
        map.insert(i, i);
    }

    map
}

fn setup_dashmap(n: usize) -> Arc<DashMap<u64, u64>> {
    let map = Arc::new(DashMap::new());

    for i in 0..n as u64 {
        map.insert(i, i);
    }

    map
}

fn setup_rwlock(n: usize) -> Arc<RwLock<HashMap<u64, u64>>> {
    let map = Arc::new(RwLock::new(HashMap::new()));

    {
        let mut m = map.write();
        for i in 0..n as u64 {
            m.insert(i, i);
        }
    }

    map
}

/* ---------------- Read Benchmark ---------------- */

fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("reads");

    let n = 100_000;

    let carbon = setup_carbonmap(n);
    let dash = setup_dashmap(n);
    let rw = setup_rwlock(n);

    group.bench_function("carbonmap", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let _ = carbon.get(&(i % n as u64));
            }
        });
    });

    group.bench_function("dashmap", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let _ = dash.get(&(i % n as u64));
            }
        });
    });

    group.bench_function("rwlock", |b| {
        b.iter(|| {
            let m = rw.read();
            for i in 0..1000 {
                let _ = m.get(&(i % n as u64));
            }
        });
    });

    group.finish();
}

/* ---------------- Write Benchmark ---------------- */

fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes");

    group.bench_function("carbonmap", |b| {
        b.iter(|| {
            let map = CarbonMap::new();

            for i in 0..10_000 {
                map.insert(i, i);
            }
        });
    });

    group.bench_function("dashmap", |b| {
        b.iter(|| {
            let map = DashMap::new();

            for i in 0..10_000 {
                map.insert(i, i);
            }
        });
    });

    group.bench_function("rwlock", |b| {
        b.iter(|| {
            let map = RwLock::new(HashMap::new());

            {
                let mut m = map.write();
                for i in 0..10_000 {
                    m.insert(i, i);
                }
            }
        });
    });

    group.finish();
}

/* ---------------- Concurrent Benchmark ---------------- */

fn bench_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");

    let threads = 8;
    let ops = 50_000;

    group.bench_function("carbonmap", |b| {
        b.iter(|| {
            let map = Arc::new(CarbonMap::new());

            let mut handles = vec![];

            for t in 0..threads {
                let m = map.clone();

                handles.push(thread::spawn(move || {
                    for i in 0..ops {
                        m.insert((t * ops + i) as u64, i as u64);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.bench_function("dashmap", |b| {
        b.iter(|| {
            let map = Arc::new(DashMap::new());

            let mut handles = vec![];

            for t in 0..threads {
                let m = map.clone();

                handles.push(thread::spawn(move || {
                    for i in 0..ops {
                        m.insert((t * ops + i) as u64, i as u64);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.bench_function("rwlock", |b| {
        b.iter(|| {
            let map = Arc::new(RwLock::new(HashMap::new()));

            let mut handles = vec![];

            for t in 0..threads {
                let m = map.clone();

                handles.push(thread::spawn(move || {
                    for i in 0..ops {
                        let mut g = m.write();
                        g.insert((t * ops + i) as u64, i as u64);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.finish();
}

/* ---------------- Register ---------------- */

criterion_group!(
    benches,
    bench_reads,
    bench_writes,
    bench_concurrent
);

criterion_main!(benches);
