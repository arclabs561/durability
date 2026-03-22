# durability

[![crates.io](https://img.shields.io/crates/v/durability.svg)](https://crates.io/crates/durability)
[![Documentation](https://docs.rs/durability/badge.svg)](https://docs.rs/durability)
[![CI](https://github.com/arclabs561/durability/actions/workflows/ci.yml/badge.svg)](https://github.com/arclabs561/durability/actions/workflows/ci.yml)

Crash-consistent persistence primitives: directory abstraction, record logs, generic WAL, checkpoints, and recovery.

## Quick start

```rust
use durability::storage::MemoryDirectory;
use durability::walog::{WalWriter, WalReader, WalEntry};
use std::sync::Arc;

let dir: Arc<dyn durability::Directory> = Arc::new(MemoryDirectory::new());

// Write
let mut w = WalWriter::<WalEntry>::new(dir.clone());
w.append(&WalEntry::AddSegment { segment_id: 1, doc_count: 100 }).unwrap();
w.append(&WalEntry::DeleteDocuments { deletes: vec![(1, 42)] }).unwrap();
w.flush().unwrap();

// Recover
let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
assert_eq!(records.len(), 2);
assert_eq!(records[0].entry_id, 1); // entry_id assigned by writer
```

`WalWriter<E>` and `WalReader<E>` are generic -- define your own entry type with
`#[derive(Serialize, Deserialize)]` and use `WalWriter::<YourType>::new(dir)`.
Entry IDs are assigned by the writer and stored in the frame header, not in your payload.

## Not Provided (and why)

- **Multi-process locking**: This crate does not manage `flock` or IPC locks.
  - *Recommendation*: Use an external lock manager or a single-writer process architecture.
- **Strong consistency by default**: `write` calls are buffered.
  - *Recommendation*: Use `sync_all` explicitly when you need a durability barrier.

## What really matters (failure model)

Most bugs in persistence layers come from being vague about failures. This crate is explicit about the failures it targets:

- **Crash / torn writes**: partial writes at the tail (e.g. process crash mid-record).
- **Corruption detection**: CRC/magic/version/type mismatches are treated as errors (even in “best-effort” modes).
- **Stable storage vs “reported success”**: unless you add explicit barriers, a successful write may still be only in OS caches.

## Contract surface (what you get)

- **Prefix property**:
  - Best-effort replay returns a prefix of the valid operation stream. It never returns “garbage” operations.
- **Narrow best-effort scope**:
  - Best-effort tolerance applies only to the **final** WAL segment’s **torn tail records**, and also tolerates a **torn header** in the final segment (crash during segment creation).
  - Corruption in non-final segments is an error.
- **Deterministic checkpoints**:
  - Checkpoint payloads are written deterministically (stable ordering), to avoid nondeterministic churn.

## Stable-storage durability (opt-in)

If you need “survives power loss after success”, you must add explicit barriers:

- Use `durability::storage::sync_file(dir, path)` to `sync_all` the file.
- Use `durability::storage::sync_parent_dir(dir, path)` to sync the parent directory when you rely on durable create/rename.
- Prefer the first-class trait `durability::DurableDirectory` when you want code to *declare* that it
  needs stable-storage durability operations (it provides `atomic_write_durable` / `atomic_rename_durable`).
- For convenience:
  - `WalWriter::flush_and_sync()`
  - `RecordLogWriter::flush_and_sync()`

If the backend cannot map paths to the OS filesystem (`Directory::file_path()` returns `None`), these operations return `NotSupported`.

## Checkpoint publishing + WAL truncation (the “real lifecycle”)

If you want to truncate old WAL segments, do it in a crash-safe order:

1) write a *durable* checkpoint,
2) append `WalEntry::Checkpoint` to the WAL and make *that* durable,
3) only then delete WAL segments that are fully covered by the checkpoint.

Use `CheckpointPublisher` for this pattern.

After truncation, recovery should start from the latest checkpoint marker (see `RecoveryManager::recover_latest`).

## Modules at a glance

- `storage`: `Directory` abstraction + `FsDirectory`/`MemoryDirectory` + sync helpers.
- `recordlog`: generic append-only log with CRC framing + strict/best-effort replay.
- `walog`: generic multi-segment WAL (`WalWriter<E>` / `WalReader<E>`) under `wal/` with strict/best-effort replay and `WalWriter::resume` repair. Includes `WalEntry` for segment-index use cases.
- `checkpoint` / `checkpointing`: checksumed snapshot files (postcard payloads).
- `replay`: generic record-log replay helpers (checkpoint + suffix pattern).
- `recover`: checkpoint + WAL recovery for a “segments + deletes” view.
- `publish`: crash-safe checkpoint publish + WAL truncation.

## Running

- Tests: `cargo test`
- Heavier property runs: `PROPTEST_CASES=512 cargo test --test prop_wal_resume`
- Benches: `cargo bench`

## Fuzzing (opt-in)

Property tests are great for semantic invariants; fuzzing is great for “never panic on weird bytes”.

Recommended (if you have LLVM tooling available):

- Install: `cargo install cargo-fuzz`
- Run (examples; see `fuzz/`):
  - `cargo fuzz run fuzz_wal_entry_decode`
  - `cargo fuzz run fuzz_wal_dir_replay`
  - `cargo fuzz run fuzz_checkpoint_read`
  - `cargo fuzz run fuzz_recordlog_read`
