//! `WalWriter::open_with_options` and `resume_with_options` keep the caller's
//! flush policy and buffer size on an existing WAL.
//!
//! `open` and `resume` use the default buffered policy, so a WAL written with
//! `FlushPolicy::PerAppend` silently became buffered after a restart.

use durability::storage::{Directory, FlushPolicy, MemoryDirectory};
use durability::walog::{WalReader, WalWriter};
use std::sync::Arc;

fn replayed_count(dir: &Arc<dyn Directory>) -> usize {
    WalReader::<()>::new(dir.clone())
        .replay_bytes()
        .unwrap()
        .len()
}

fn existing_wal() -> Arc<dyn Directory> {
    let dir = MemoryDirectory::arc();
    let mut w = WalWriter::<()>::with_flush_policy(dir.clone(), FlushPolicy::PerAppend);
    w.append_bytes(b"first-run").unwrap();
    drop(w);
    dir
}

#[test]
fn open_with_options_keeps_flush_policy_on_reopen() {
    let dir = existing_wal();
    let mut w = WalWriter::<()>::open_with_options(dir.clone(), FlushPolicy::PerAppend, 0).unwrap();
    assert_eq!(w.append_bytes(b"after-restart").unwrap(), 2);
    // PerAppend: visible without an explicit flush and before drop.
    assert_eq!(replayed_count(&dir), 2);
    drop(w);
}

#[test]
fn resume_with_options_keeps_flush_policy() {
    let dir = existing_wal();
    let mut w =
        WalWriter::<()>::resume_with_options(dir.clone(), FlushPolicy::PerAppend, 0).unwrap();
    assert_eq!(w.append_bytes(b"after-restart").unwrap(), 2);
    assert_eq!(replayed_count(&dir), 2);
    drop(w);
}

#[test]
fn open_with_options_on_empty_directory_uses_options() {
    let dir = MemoryDirectory::arc();
    let mut w = WalWriter::<()>::open_with_options(dir.clone(), FlushPolicy::PerAppend, 0).unwrap();
    w.append_bytes(b"x").unwrap();
    assert_eq!(replayed_count(&dir), 1);
    drop(w);
}

#[test]
fn open_with_options_buffers_when_asked() {
    let dir = existing_wal();
    let mut w =
        WalWriter::<()>::open_with_options(dir.clone(), FlushPolicy::Manual, 64 * 1024).unwrap();
    w.append_bytes(b"buffered").unwrap();
    assert_eq!(replayed_count(&dir), 1, "Manual policy buffers the append");
    w.flush().unwrap();
    assert_eq!(replayed_count(&dir), 2);
}

/// The README's tuning snippet runs again on the same directory, as it would
/// after a process restart.
#[test]
fn readme_tuning_snippet_runs_twice_on_one_directory() {
    let tmp = tempfile::tempdir().unwrap();
    for run in 1..=2u64 {
        let dir = durability::storage::FsDirectory::arc(tmp.path()).unwrap();
        let mut w = WalWriter::<()>::open_with_options(
            dir,
            FlushPolicy::Interval(std::time::Duration::from_millis(10)),
            64 * 1024,
        )
        .unwrap();
        w.set_segment_size_limit_bytes(64 * 1024 * 1024);
        w.set_segment_max_age(std::time::Duration::from_secs(300));
        w.set_preallocate_bytes(4 * 1024 * 1024);
        w.set_recycle_capacity(4);
        assert_eq!(w.append_bytes(b"entry").unwrap(), run);
    }
    let dir = durability::storage::FsDirectory::arc(tmp.path()).unwrap();
    assert_eq!(replayed_count(&dir), 2, "both runs' appends survive drop");
}
