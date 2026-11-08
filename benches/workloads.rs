use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lsmkv::benchmark::{step, Dataset, Workload, DEFAULT_KEYS};

fn workloads(criterion: &mut Criterion) {
    let dataset = Dataset::new(DEFAULT_KEYS).expect("prepare one-million-key benchmark dataset");
    let mut group = criterion.benchmark_group("lsmkv-one-million-keys");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
    for workload in Workload::ALL {
        group.throughput(Throughput::Elements(if workload == Workload::Scan100 {
            100
        } else {
            1
        }));
        let mut sequence = 0u64;
        group.bench_function(workload.name(), |bencher| {
            bencher.iter(|| {
                step(dataset.db(), dataset.keys(), workload, black_box(sequence))
                    .expect("benchmark operation failed");
                sequence = sequence.wrapping_add(1);
            });
        });
        dataset.db().sync().expect("sync benchmark workload");
    }
    group.finish();
}

criterion_group!(benches, workloads);
criterion_main!(benches);
