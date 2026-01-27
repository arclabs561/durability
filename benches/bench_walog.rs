//! Benchmarks for `durability::walog` (write + replay).
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use durability::storage::{Directory, FlushPolicy, FsDirectory, MemoryDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;

fn bench_wal_write_and_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("walog");

    group.bench_function("append_10k_memory", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let writer = WalWriter::new(dir.clone());
                (dir, writer)
            },
            |(dir, mut writer)| {
                for i in 0..10_000u64 {
                    let _ = writer
                        .append(WalEntry::AddSegment {
                            entry_id: 0,
                            segment_id: i + 1,
                            doc_count: (i as u32) % 1000,
                        })
                        .unwrap();
                }
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("replay_10k_memory", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let mut writer = WalWriter::new(dir.clone());
                for i in 0..10_000u64 {
                    let _ = writer
                        .append(WalEntry::AddSegment {
                            entry_id: 0,
                            segment_id: i + 1,
                            doc_count: (i as u32) % 1000,
                        })
                        .unwrap();
                }
                (dir,)
            },
            |(dir,)| {
                let r = WalReader::new(dir);
                let entries = r.replay().unwrap();
                std::hint::black_box(entries);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_10k_memory_buffer_64k_flush_every_64", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let writer =
                    WalWriter::with_options(dir.clone(), FlushPolicy::EveryN(64), 64 * 1024);
                (dir, writer)
            },
            |(dir, mut writer)| {
                for i in 0..10_000u64 {
                    let _ = writer
                        .append(WalEntry::AddSegment {
                            entry_id: 0,
                            segment_id: i + 1,
                            doc_count: (i as u32) % 1000,
                        })
                        .unwrap();
                }
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_10k_fs", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer = WalWriter::new(dir.clone());
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..10_000u64 {
                    let _ = writer
                        .append(WalEntry::AddSegment {
                            entry_id: 0,
                            segment_id: i + 1,
                            doc_count: (i as u32) % 1000,
                        })
                        .unwrap();
                }
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_10k_fs_no_flush", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer = WalWriter::with_flush_policy(dir.clone(), FlushPolicy::Manual);
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..10_000u64 {
                    let _ = writer
                        .append(WalEntry::AddSegment {
                            entry_id: 0,
                            segment_id: i + 1,
                            doc_count: (i as u32) % 1000,
                        })
                        .unwrap();
                }
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_10k_fs_flush_every_64", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer = WalWriter::with_flush_policy(dir.clone(), FlushPolicy::EveryN(64));
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..10_000u64 {
                    let _ = writer
                        .append(WalEntry::AddSegment {
                            entry_id: 0,
                            segment_id: i + 1,
                            doc_count: (i as u32) % 1000,
                        })
                        .unwrap();
                }
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_10k_fs_buffer_64k_flush_every_64", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer =
                    WalWriter::with_options(dir.clone(), FlushPolicy::EveryN(64), 64 * 1024);
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..10_000u64 {
                    let _ = writer
                        .append(WalEntry::AddSegment {
                            entry_id: 0,
                            segment_id: i + 1,
                            doc_count: (i as u32) % 1000,
                        })
                        .unwrap();
                }
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_wal_write_and_replay);
criterion_main!(benches);
