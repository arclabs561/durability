# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] - 2026-10-09

Includes every fix in 0.7.3.

### Changed

- `mmap::MappedFile::open` is now an `unsafe fn` (breaking). The mapping hands
  out `&[u8]` views of the file, so truncating the file while it is mapped
  raised `SIGBUS` in safe code, and rewriting it changed bytes behind a shared
  reference. Callers now state that nothing truncates or modifies the file
  while the `MappedFile` is alive; writers should write a new file and rename
  it into place.

## [0.7.3] - 2026-10-09

### Fixed

- `WalWriter::resume` no longer truncates the last segment at a frame whose
  length runs past EOF when a valid frame follows it. A flipped bit in a
  length field previously looked like a torn tail, so resume deleted every
  later synced record and reused their entry IDs; it now returns an error.
- `RecordLogWriter` cuts a torn tail before appending to an existing log.
  Records appended after a partial record (from a crash or a full disk) were
  unreadable because every read stopped at the torn bytes with
  `CrcMismatch`.
- `wal/.lock` is now held with an OS file lock (`fs4`) instead of a file
  created with `O_EXCL` and removed on drop. A crash no longer leaves a stale
  lock that blocks `resume`, and `resume_after_crash` no longer deletes the
  lock of a live writer.
- Strictly decoded (non-final) WAL segments treat zero padding through EOF as
  the end of the segment. A crash during rotation with preallocation could
  leave an older segment zero-padded, which made the whole WAL unreadable.
  The rotation shrink is now checked and synced.
- Resuming after a torn final segment continues entry IDs from the previous
  segment instead of restarting at 1.
- Appends that would overflow the `u64` entry ID space return an error
  instead of wrapping or saturating.

### Changed

- Narrowed the documented persistence guarantees: the crate provides
  integrity-checked recovery and explicit sync steps, not a universal
  power-loss guarantee.

### Upgrade note

- A 0.7.2 process and a 0.7.3 process do not exclude each other on the same
  WAL directory, because they use different lock mechanisms. Stop all writers
  before upgrading.

## [0.7.2] - 2026-07-09

### Changed

- WAL and checkpoint metadata are now covered by the CRCs.

## [0.7.1] - 2026-07-04

### Changed

- Made `serde` optional. Default builds are unchanged because `postcard` enables
  `serde`; `default-features = false` raw builds no longer compile
  `serde`/`serde_derive` unless callers opt into `features = ["serde"]`.

## [0.7.0] - 2026-07-03

### Changed

- Length-prefixed payload reads (checkpoint, record log, WAL) no longer
  allocate the claimed length up front. A corrupt length prefix between the
  true remaining bytes and the format cap previously forced a zeroed
  allocation up to the cap (256 MiB for checkpoints) before the read failed;
  the buffer now grows with the bytes actually present, and a short read
  still surfaces as the same truncation error class.
- Recovery no longer falls back to an older checkpoint when a newer checkpoint's
  file is missing and the WAL does not provably start at entry 1. Prefix
  truncation could have dropped entries between the two, so replaying forward
  from the older checkpoint risked silently skipping them; `recover_latest`
  (and the best-effort variant) now return `InvalidState` instead. Recovery is
  unaffected when the newest checkpoint file is present or when the WAL retains
  the full entry-1 prefix.

## [0.6.12] - 2026-07-02

### Added

- Added raw checkpoint APIs: `CheckpointFile::write_bytes`,
  `write_bytes_durable`, and `read_bytes`.
- Added raw WAL APIs: `WalWriter::append_bytes`, `WalReader::replay_bytes`,
  raw streaming replay helpers, and `SyncWalWriter` byte append variants.
- Added a default-enabled `postcard` feature for typed serde/postcard WAL,
  checkpoint, recovery, and publish helpers.
- Added `Directory::delete_durable` for delete operations that need the same
  parent-directory sync as durable renames and writes.

### Changed

- `postcard` is now optional. Default builds keep the existing typed APIs;
  `default-features = false` builds expose the raw persistence primitives
  without compiling `postcard`.
- `RecordLogWriter` now poisons itself after any write, flush, or sync error:
  every later `append_bytes`/`flush`/`flush_and_sync` returns
  `PersistenceError::InvalidState` instead of writing past a failure. Callers
  that retried through a failed writer must reopen the log instead.

## [0.6.11] - 2026-06-28

### Changed

- `RecordLogWriter::flush_and_sync` now fsyncs the file's parent directory only
  once (after the file is created), not on every record. The parent-dir fsync
  makes the file name durable, and appends never change the directory entry, so
  the per-record parent fsync added no durability and roughly doubled the per-op
  sync cost under a per-record sync policy. Recovery semantics are unchanged.

## [0.6.10] - 2026-06-26

### Changed
- Expanded WAL recycle/resume and merge-delete recovery regression coverage.

## [0.6.9] - 2026-06-26

### Fixed
- Made latest-checkpoint recovery error instead of silently replaying an
  incomplete WAL suffix when checkpoint metadata is missing after WAL prefix
  truncation.

## [0.6.8] - 2026-06-26

### Fixed
- Limited docs.rs to the default documentation target so the hosted build stays
  within its memory budget.

## [0.6.7] - 2026-06-26

### Fixed
- Stopped point-in-time generic recovery before the requested entry ceiling, so
  corrupt or oversized WAL payloads after the cutoff are ignored.

### Changed
- Hardened WAL recovery tests for checkpoint replay boundaries, payload-size
  limits, non-WAL directory entries, and bounded recovery fuzzing.
- Added a tracked WAL/recovery hardening roadmap for the remaining release gates.

## [0.6.6] - 2026-06-26

### Added
- `WalWriter::resume_after_crash` and `SyncWalWriter::resume_after_crash` for explicit stale-lock recovery.

### Changed
- `WalWriter::resume` now respects an existing `wal/.lock` instead of silently removing it.

### Fixed
- Reject WAL segment holes, segment header mismatches, and entry ID gaps during replay, resume, and maintenance.
- Preserve a contiguous WAL suffix when a truncation observer vetoes deletion.
- Report missing recycled segment files instead of silently allocating a fresh segment.
- Corrected `SyncWalWriter` documentation that described group-commit behavior it did not provide.

## [0.6.5] - 2026-06-12

### Added
- `storage::sync_parent_of_path` raw-path helper for syncing a file's parent directory.

### Fixed
- Page-align the `WillNeed` madvise start address for Linux in the mmap module.
- Read `errno` via std instead of `libc::__error` in the mmap module.

## [0.6.4] - 2026-04-27

### Added
- `storage::sync_parent_of_path` raw-path helper.
- Expanded CONTRIBUTING.md (setup, style, testing, PR expectations).

## [0.6.3] - 2026-04-20

### Fixed
- Clippy `explicit_counter_loop` warning in the walog module.
- Stale version reference in the README.

## [0.6.2] - 2026-04-14

### Added
- `mmap` module with `madvise` advisory hints.

## [0.6.1] - 2026-04-10

### Added
- 14 tests covering audit gaps.

## [0.6.0] - 2026-04-10

### Added
- `SyncWalWriter`, segment recycling, and `truncate_to_recycle`.
- `WalWriter::open`, generic `CheckpointPublisher`, `WalObserver`, and async `Directory`.
- `[workspace]` table for standalone builds.

### Changed
- Research-driven improvements from a WAL landscape review.
- Expanded walog module documentation for the new features.

## [0.5.1] - 2026-04-05

### Added
- Debug-mode warning when a buffer is dropped unflushed.

### Changed
- `CheckpointHeader` fields are now private; added `missing_docs` enforcement and doc-tests.

### Fixed
- Frame decoder desync, rotation poisoning, and sentinel early-stop.

## [0.5.0] - 2026-03-30

### Added
- `kv_store` example for the generic recovery API.
- Exhaustive failure test plus research-derived edge-case tests.
- Crash-during-resume test.

### Changed
- `WalWriter` is now `Send`; recovery can early-stop.
- Replaced the `RecoveryMode` enum with `RecoveryOptions` used directly.
- Dropped the `byteorder` dependency in favor of std `le_bytes`.
- Internal codec types are now doc-hidden.

### Removed
- `DurableDirectory` trait; its methods were folded into `Directory`.

### Fixed
- Preallocation recovery, writer poisoning, and temp-file cleanup.

## [0.4.0] - 2026-03-26

### Added
- Point-in-time recovery and file preallocation.
- Generic recovery.

### Fixed
- Path-traversal vulnerability.

## [0.3.0] - 2026-03-26

### Added
- Streaming WAL replay and countdown fault injection.
- `append_batch`, `entry_count`, and metadata accessors.
- `fdatasync`, advisory lockfile, and model-based property tests.

### Changed
- Folded `checkpointing.rs` into `recover.rs` and narrowed the API.
- Upgraded `thiserror` to v2.
- Switched the publish workflow to OIDC trusted publishing.

### Removed
- `RecordLogWriter::new_conservative` (zero callers).

### Fixed
- Lockfile interference with fault-injection tests.
- `MemoryDirectory` semantics; hardened `FsDirectory`.

## [0.2.0] - 2026-03-22

### Added
- `entry_id` in the WAL frame.
- Documented WAL frame layout.

### Changed
- Generalized the WAL implementation.

## [0.1.1] - 2026-03-08

### Added
- Initial release: write-ahead log with crash recovery.
