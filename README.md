# durability

[![crates.io](https://img.shields.io/crates/v/durability.svg)](https://crates.io/crates/durability)
[![Documentation](https://docs.rs/durability/badge.svg)](https://docs.rs/durability)
[![CI](https://github.com/arclabs561/durability/actions/workflows/ci.yml/badge.svg)](https://github.com/arclabs561/durability/actions/workflows/ci.yml)

Durability primitives for local persistence.

## Quick start

```rust
use durability::storage::MemoryDirectory;
use durability::walog::{WalWriter, WalReader, WalEntry};

// MemoryDirectory::arc() returns Arc<dyn Directory> directly
let dir = MemoryDirectory::arc();

// Write
let mut w = WalWriter::<WalEntry>::new(dir.clone());
w.append(&WalEntry::AddSegment { segment_id: 1, doc_count: 100 }).unwrap();
w.append(&WalEntry::DeleteDocuments { deletes: vec![(1, 42)] }).unwrap();
w.flush().unwrap();

assert_eq!(w.last_entry_id(), Some(2));

// Recover
let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
assert_eq!(records.len(), 2);
assert_eq!(records[0].entry_id, 1); // entry_id assigned by writer
```

`WalWriter<E>` and `WalReader<E>` are generic -- define your own entry type with
`#[derive(Serialize, Deserialize)]` and use `WalWriter::<YourType>::new(dir)`.
Entry IDs are assigned by the writer and stored in the frame header, not in your payload.

**Important**: `WalWriter::new()` creates a fresh WAL and errors if segments already
exist. Use `WalWriter::resume()` to continue an existing WAL (it handles both empty
and non-empty directories).

For batch writes (amortize flush cost across multiple entries):

```rust
let ids = w.append_batch(&[
    WalEntry::AddSegment { segment_id: 3, doc_count: 50 },
    WalEntry::DeleteDocuments { deletes: vec![(1, 10)] },
]).unwrap();
// Single flush for both entries
```

For large WALs, use streaming replay to avoid collecting into a `Vec`:

```rust
reader.replay_each(|record| {
    println!("entry {}: {:?}", record.entry_id, record.payload);
    Ok(())
})?;
```

## Not provided (and why)

- **Multi-process locking**: This crate does not manage `flock` or IPC locks.
  Single-writer-per-directory is assumed. Multiple writers silently corrupt data.
  `WalWriter` creates an advisory lockfile (`wal/.lock`) to catch accidental
  double-instantiation within a process, but this does not guarantee cross-process
  exclusion.
- **Strong consistency by default**: `write` calls are buffered.
  Use `flush_and_sync()` when you need a durability barrier.
- **fsync failure recovery**: A failed `fsync` on Linux clears dirty pages; retrying
  reports false success. This crate propagates IO errors as fatal. Callers should
  treat fsync failure as unrecoverable and restart from WAL.

## What really matters (failure model)

- **Crash / torn writes**: partial writes at the tail (e.g. process crash mid-record).
- **Corruption detection**: CRC/magic/version/type mismatches are treated as errors (even in "best-effort" modes).
- **Stable storage vs "reported success"**: unless you add explicit barriers, a successful write may still be only in OS caches.

## Contract surface (what you get)

- **Prefix property**:
  Best-effort replay returns a prefix of the valid operation stream. No garbage, no reordering.
- **Narrow best-effort scope**:
  Tolerance applies only to the **final** WAL segment's **torn tail records**, and also tolerates a **torn header** in the final segment (crash during segment creation).
  Corruption in non-final segments is an error.
- **Deterministic checkpoints**:
  Checkpoint payloads are written deterministically (stable ordering).

## Stable-storage durability (opt-in)

If you need "survives power loss after success", add explicit barriers:

- `WalWriter::flush_and_sync()` / `RecordLogWriter::flush_and_sync()`
- `durability::storage::sync_file(dir, path)` -- fsync the file
- `durability::storage::sync_parent_dir(dir, path)` -- sync the parent directory (needed for durable create/rename)
- `Directory` trait provides `atomic_write_durable` / `atomic_rename_durable`

If the backend cannot map paths to the OS filesystem (`Directory::file_path()` returns `None`), these operations return `NotSupported`.

## Checkpoint publishing + WAL truncation

To truncate old WAL segments safely:

1. Write a durable checkpoint
2. Append `WalEntry::Checkpoint` to the WAL and make it durable
3. Only then delete WAL segments covered by the checkpoint

Use `CheckpointPublisher` for this pattern. After truncation, recovery should
start from the latest checkpoint marker (see `RecoveryManager::recover_latest`).

## Modules at a glance

- `storage`: `Directory` abstraction + `FsDirectory`/`MemoryDirectory` + sync helpers.
- `recordlog`: append-only log with CRC framing + strict/best-effort replay.
- `walog`: generic multi-segment WAL (`WalWriter<E>` / `WalReader<E>`) with strict/best-effort replay and `resume` repair. Includes `WalEntry` for segment-index use cases.
- `checkpoint`: CRC-validated snapshot files (postcard payloads).
- `recover`: generic `recover_with_wal()` for any checkpoint + WAL entry types, plus segment-specific `RecoveryManager`.
- `publish`: crash-safe checkpoint publish + WAL truncation.

## Running

- Tests: `cargo test`
- Heavier property runs: `PROPTEST_CASES=512 cargo test --test prop_wal_resume`
- Benches: `cargo bench`

## Fuzzing (opt-in)

Property tests cover semantic invariants; fuzzing covers "never panic on weird bytes".

- Install: `cargo install cargo-fuzz`
- Run (see `fuzz/`):
  - `cargo fuzz run fuzz_wal_entry_decode`
  - `cargo fuzz run fuzz_wal_dir_replay`
  - `cargo fuzz run fuzz_checkpoint_read`
  - `cargo fuzz run fuzz_recordlog_read`
