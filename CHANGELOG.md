# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
