use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use indexmap::IndexMap;
use kaspa_database::prelude::{Cache, CachePolicy};
use parking_lot::RwLock;
use rand::Rng;
use std::{hint::black_box, sync::Arc, time::Duration};

struct UpstreamCountCache {
    map: Arc<RwLock<IndexMap<u64, u64>>>,
    capacity: usize,
}

impl UpstreamCountCache {
    fn full(capacity: usize) -> Self {
        Self { map: Arc::new(RwLock::new((0..capacity as u64).map(|key| (key, key)).collect())), capacity }
    }

    fn insert(&self, key: u64, value: u64) {
        let mut map = self.map.write();
        if map.len() == self.capacity {
            let index = rand::thread_rng().gen_range(0..self.capacity);
            map.swap_remove_index(index);
        }
        map.insert(key, value);
    }

    fn get(&self, key: &u64) -> Option<u64> {
        self.map.read().get(key).copied()
    }
}

fn full_cache_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("count_cache_existing_update");
    for capacity in [64, 4096, 65536] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("upstream", capacity), &capacity, |b, &capacity| {
            let cache = UpstreamCountCache::full(capacity);
            b.iter(|| cache.insert(black_box(0), black_box(1)));
        });
        group.bench_with_input(BenchmarkId::new("active", capacity), &capacity, |b, &capacity| {
            let cache = Cache::<u64, u64>::new(CachePolicy::Count(capacity));
            cache.insert_many(&mut (0..capacity as u64).map(|key| (key, key)));
            b.iter(|| cache.insert(black_box(0), black_box(1)));
        });
    }
    group.finish();
}

fn disabled_cache_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("disabled_cache_read");
    let upstream = UpstreamCountCache::full(0);
    group.bench_function("upstream", |b| {
        b.iter(|| black_box(upstream.get(black_box(&0))));
    });
    let active = Cache::<u64, u64>::new(CachePolicy::Empty);
    group.bench_function("active", |b| {
        b.iter(|| black_box(active.get(black_box(&0))));
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(30).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(2));
    targets = full_cache_updates, disabled_cache_reads
}
criterion_main!(benches);
