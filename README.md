# durability

[![crates.io](https://img.shields.io/crates/v/durability.svg)](https://crates.io/crates/durability)
[![Documentation](https://docs.rs/durability/badge.svg)](https://docs.rs/durability)

Durability primitives for local persistence.

`durability` provides filesystem and in-memory directories, atomic writes, WALs,
record logs, checkpoint files, CRC validation, and sync helpers.

It does not own indexing, segment selection, compaction, or reader visibility.

## Quick start

```toml
[dependencies]
durability = "0.7"
```

```rust
use durability::storage::MemoryDirectory;
use durability::walog::{WalWriter, WalReader, WalEntry};

let dir = MemoryDirectory::arc();

// open() creates a fresh WAL or resumes an existing one.
let mut w = WalWriter::<WalEntry>::open(dir.clone()).unwrap();
w.append(&WalEntry::AddSegment { segment_id: 1, doc_count: 100 }).unwrap();
w.append(&WalEntry::DeleteDocuments { deletes: vec![(1, 42)] }).unwrap();
w.flush().unwrap();

assert_eq!(w.last_entry_id(), Some(2));
drop(w);

// Recover
let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
assert_eq!(records.len(), 2);
assert_eq!(records[0].entry_id, 1);
```

`WalWriter<E>` and `WalReader<E>` are generic: define your own entry type with
`#[derive(Serialize, Deserialize)]` and use `WalWriter::<YourType>::open(dir)`.

## Feature flags

`postcard` is enabled by default. It provides typed serde/postcard helpers:
`WalWriter::append`, `WalReader::replay`, `CheckpointFile::write_postcard`,
`recover`, and `publish`.

`serde` is enabled by `postcard`. In no-default builds, enable `serde` directly
if you only need `WalEntry` serialization derives without postcard helpers.

With `default-features = false`, `durability` still provides `Directory`,
`RecordLogWriter::append_bytes`, `RecordLogReader::read_all`,
`CheckpointFile::{write_bytes, read_bytes}`, raw WAL append/replay, WAL
maintenance, and sync helpers.

## Backends

`FsDirectory` is the stable-storage backend. It exposes filesystem paths for
fsync, parent-directory sync, and mmap helpers. `MemoryDirectory` is useful for
tests and process-local stores; it does not provide power-loss durability and
cannot support mmap.

The optional `mmap` feature provides `MappedFile` for read-only memory maps with
advisory access hints. Higher-level crates decide which files are safe and useful
to map.

## Thread-safe writer

`SyncWalWriter` wraps a `WalWriter` in a `Mutex` for concurrent access.
`append_durable` appends one entry and syncs it to stable storage while holding
that mutex:

```rust
use durability::storage::FsDirectory;
use durability::walog::SyncWalWriter;
use std::sync::Arc;

# #[derive(serde::Serialize, serde::Deserialize)] enum Op { Set(String) }
let dir = FsDirectory::arc("/tmp/wal-demo").unwrap();
let sw = Arc::new(SyncWalWriter::<Op>::open(dir).unwrap());

let sw2 = sw.clone();
std::thread::spawn(move || {
    sw2.append_durable(&Op::Set("hello".into())).unwrap();
});
sw.append_durable(&Op::Set("world".into())).unwrap();
```

## Generic recovery

`recover_with_wal` coordinates checkpoint loading and WAL replay for any
entry type and checkpoint schema:

```rust
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::walog::WalWriter;
use durability::storage::MemoryDirectory;

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Snap { counter: u64 }

#[derive(serde::Serialize, serde::Deserialize)]
enum Op { Inc, Dec }

let dir = MemoryDirectory::arc();
let mut w = WalWriter::<Op>::new(dir.clone());
w.append(&Op::Inc).unwrap();
w.append(&Op::Inc).unwrap();
w.append(&Op::Dec).unwrap();
w.flush().unwrap();
drop(w);

let result = recover_with_wal::<Snap, Op, _>(
    &dir, None, RecoveryOptions::strict(),
    |ckpt| ckpt.unwrap_or_default().counter,
    |counter, _id, entry| match entry {
        Op::Inc => *counter += 1,
        Op::Dec => *counter = counter.saturating_sub(1),
    },
).unwrap();
assert_eq!(result.state, 1); // 0 + 1 + 1 - 1
```

## Tuning

```rust
use durability::storage::{FsDirectory, FlushPolicy};
use durability::walog::WalWriter;
# #[derive(serde::Serialize, serde::Deserialize)] enum Op { X }

let dir = FsDirectory::arc("/tmp/wal-tuning").unwrap();
let mut w = WalWriter::<Op>::with_options(
    dir, FlushPolicy::Interval(std::time::Duration::from_millis(10)), 64 * 1024,
);
w.set_segment_size_limit_bytes(64 * 1024 * 1024); // 64 MiB segments
w.set_segment_max_age(std::time::Duration::from_secs(300)); // rotate after 5 min
w.set_preallocate_bytes(4 * 1024 * 1024); // 4 MiB preallocation
w.set_recycle_capacity(4); // reuse up to 4 truncated segments
```

## Async support

The `async` feature provides `AsyncDirectory` and `BlockingBridge` for tokio:

```toml
[dependencies]
durability = { version = "0.7", features = ["async"] }
```

```rust,no_run
use durability::async_dir::{AsyncDirectory, BlockingBridge};
use durability::storage::FsDirectory;

# async fn example() {
let bridge = BlockingBridge::new(FsDirectory::new("/tmp/async-demo").unwrap());
bridge.atomic_write("data.bin", b"hello".to_vec()).await.unwrap();
let data = bridge.read_file("data.bin").await.unwrap();
# }
```

## Modules

| Module | Purpose |
|--------|---------|
| `storage` | `Directory` trait, `FsDirectory`, `MemoryDirectory`, sync helpers |
| `walog` | Generic WAL: raw bytes always, typed serde/postcard helpers with feature `postcard` |
| `recordlog` | Append-only single-file log with CRC framing |
| `checkpoint` | CRC-validated snapshot files |
| `recover` | Generic `recover_with_wal()` + segment-specific `RecoveryManager` (feature `postcard`) |
| `publish` | Crash-safe checkpoint publish + WAL truncation (feature `postcard`) |
| `async_dir` | `AsyncDirectory` trait + `BlockingBridge` (feature `async`) |

## Not provided

- **Multi-process locking**: single-writer-per-directory assumed. Advisory lockfile catches in-process double-instantiation only.
- **Per-write fsync by default**: writes are buffered. Use `flush_and_sync()` for a durability barrier.
- **fsync failure recovery**: a failed fsync poisons the writer. Callers should treat this as unrecoverable and restart from WAL.

## Contract

- **Prefix property**: best-effort replay returns a prefix of the valid stream.
- **Narrow best-effort**: tolerance applies only to the final segment's torn tail. Corruption in non-final segments is an error.
- **Checksum coverage**: current checkpoint and WAL writes CRC-check the durable metadata they trust plus the payload. Legacy checkpoint v1 and WAL v2 files with payload-only CRCs remain readable.
- **Deterministic checkpoints**: payloads are written with stable ordering.

## Running

- Tests: `cargo test`
- Property tests: `PROPTEST_CASES=512 cargo test --test prop_wal_resume`
- Benches: `cargo bench`
- Fuzzing: `cargo fuzz run fuzz_wal_entry_decode` (see `fuzz/`)
