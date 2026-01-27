//! Benchmarks for `durability::recordlog`.
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use durability::recordlog::{RecordLogReadMode, RecordLogReader, RecordLogWriter};
use durability::storage::{Directory, FlushPolicy, FsDirectory, MemoryDirectory};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Msg {
    text: String,
}

fn diverse_texts() -> Vec<String> {
    vec![
        "Marie Curie discovered radium in Paris.".into(),
        "習近平在北京會見了普京。".into(),
        "التقى محمد بن سلمان بالرئيس في الرياض".into(),
        "Путин встретился с Си Цзиньпином в Москве.".into(),
        "Dr. 田中 presented her research at MIT's AI conference.".into(),
        "Москва (Moskva/Moscow) hosted the 東京 (Tokyo) delegation.".into(),
        "François Müller and José García met in São Paulo.".into(),
        "Pelé, Madonna, and Cher performed at the concert.".into(),
        "Her Majesty Queen Elizabeth II met Prime Minister शर्मा.".into(),
    ]
}

fn bench_recordlog_append_and_read(c: &mut Criterion) {
    let texts = diverse_texts();

    let mut group = c.benchmark_group("recordlog");

    group.bench_function("append_postcard_1k_memory", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let writer = RecordLogWriter::new(dir.clone(), "log.bin");
                (dir, writer)
            },
            |(dir, mut writer)| {
                for i in 0..1000 {
                    let t = texts[i % texts.len()].clone();
                    writer.append_postcard(&Msg { text: t }).unwrap();
                }
                // keep dir alive
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("read_all_postcard_1k_memory", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let mut writer = RecordLogWriter::new(dir.clone(), "log.bin");
                for i in 0..1000 {
                    let t = texts[i % texts.len()].clone();
                    writer.append_postcard(&Msg { text: t }).unwrap();
                }
                (dir,)
            },
            |(dir,)| {
                let reader = RecordLogReader::new(dir.clone(), "log.bin");
                let xs: Vec<Msg> = reader.read_all_postcard(RecordLogReadMode::Strict).unwrap();
                std::hint::black_box(xs);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_postcard_1k_memory_buffer_64k_flush_every_64", |b| {
        b.iter_batched(
            || {
                let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
                let writer = RecordLogWriter::with_options(
                    dir.clone(),
                    "log.bin",
                    FlushPolicy::EveryN(64),
                    64 * 1024,
                );
                (dir, writer)
            },
            |(dir, mut writer)| {
                for i in 0..1000 {
                    let t = texts[i % texts.len()].clone();
                    writer.append_postcard(&Msg { text: t }).unwrap();
                }
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_postcard_1k_fs", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer = RecordLogWriter::new(dir.clone(), "log.bin");
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..1000 {
                    let t = texts[i % texts.len()].clone();
                    writer.append_postcard(&Msg { text: t }).unwrap();
                }
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_postcard_1k_fs_no_flush", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer =
                    RecordLogWriter::with_flush_policy(dir.clone(), "log.bin", FlushPolicy::Manual);
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..1000 {
                    let t = texts[i % texts.len()].clone();
                    writer.append_postcard(&Msg { text: t }).unwrap();
                }
                // One explicit flush at end (still cheaper than per-append).
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_postcard_1k_fs_flush_every_64", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer = RecordLogWriter::with_flush_policy(
                    dir.clone(),
                    "log.bin",
                    FlushPolicy::EveryN(64),
                );
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..1000 {
                    let t = texts[i % texts.len()].clone();
                    writer.append_postcard(&Msg { text: t }).unwrap();
                }
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("append_postcard_1k_fs_buffer_64k_flush_every_64", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let dir = FsDirectory::new(tmp.path()).unwrap();
                let dir: Arc<dyn Directory> = Arc::new(dir);
                let writer = RecordLogWriter::with_options(
                    dir.clone(),
                    "log.bin",
                    FlushPolicy::EveryN(64),
                    64 * 1024,
                );
                (tmp, dir, writer)
            },
            |(_tmp, dir, mut writer)| {
                for i in 0..1000 {
                    let t = texts[i % texts.len()].clone();
                    writer.append_postcard(&Msg { text: t }).unwrap();
                }
                writer.flush().unwrap();
                drop(dir);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_recordlog_append_and_read);
criterion_main!(benches);
