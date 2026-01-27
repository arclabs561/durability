//! Benchmarks for `durability::checkpoint`.
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use durability::checkpoint::CheckpointFile;
use durability::storage::{Directory, MemoryDirectory};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Snap {
    n: u64,
    // keep unicode in the payload
    city: String,
    // some bulk data
    data: Vec<u32>,
}

fn bench_checkpoint_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint");

    group.bench_function("write_read_small_memory", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let ckpt = CheckpointFile::new(dir.clone());
                let snap = Snap {
                    n: 7,
                    city: "東京".into(),
                    data: (0..256).collect(),
                };
                (dir, ckpt, snap)
            },
            |(dir, ckpt, snap)| {
                ckpt.write_postcard("c.bin", 42, &snap).unwrap();
                let (_last, out): (u64, Snap) = ckpt.read_postcard("c.bin").unwrap();
                std::hint::black_box(out);
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("write_read_1mbish_memory", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let ckpt = CheckpointFile::new(dir.clone());
                let snap = Snap {
                    n: 7,
                    city: "Москва".into(),
                    data: (0..(256 * 1024)).collect(), // ~1 MiB
                };
                (dir, ckpt, snap)
            },
            |(dir, ckpt, snap)| {
                ckpt.write_postcard("c.bin", 42, &snap).unwrap();
                let (_last, out): (u64, Snap) = ckpt.read_postcard("c.bin").unwrap();
                std::hint::black_box(out);
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_checkpoint_roundtrip);
criterion_main!(benches);
