# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
