//! Benchmarks for end-to-end recovery (checkpoint + WAL).
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use durability::checkpointing::{CheckpointManager, CheckpointSegment, CheckpointState};
use durability::recover::RecoveryManager;
use durability::storage::{Directory, MemoryDirectory};
use durability::walog::{WalEntry, WalWriter};
use std::sync::Arc;

fn bench_recover_checkpoint_plus_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("recover");

    group.bench_function("recover_1k_segments_10k_deletes_memory", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

                // Build a checkpoint with 1k segments.
                let ckpt_mgr = CheckpointManager::new(dir.clone());
                let mut segs = Vec::with_capacity(1000);
                for sid in 1u64..=1000u64 {
                    segs.push(CheckpointSegment {
                        segment_id: sid,
                        doc_count: 100,
                        deleted_docs: vec![],
                    });
                }
                let state = CheckpointState { segments: segs };
                ckpt_mgr
                    .write_checkpoint(&state, 0, "checkpoints/c1.chk")
                    .unwrap();

                // WAL: 10k deletes spread across segments.
                let mut wal = WalWriter::new(dir.clone());
                for i in 0..10_000u64 {
                    let seg = (i % 1000) + 1;
                    let doc = (i % 100) as u32;
                    wal.append(WalEntry::DeleteDocuments {
                        entry_id: 0,
                        deletes: vec![(seg, doc)],
                    })
                    .unwrap();
                }

                (dir,)
            },
            |(dir,)| {
                let rec = RecoveryManager::new(dir)
                    .recover(Some("checkpoints/c1.chk"))
                    .unwrap();
                std::hint::black_box(rec);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_recover_checkpoint_plus_wal);
criterion_main!(benches);
