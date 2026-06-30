//! E2E test: full lifecycle using generic recovery + WAL truncation.
//!
//! Exercises: multi-segment WAL, checkpoint, truncation, recovery from
//! checkpoint, all using custom types (not WalEntry).
#![cfg(feature = "postcard")]

use durability::checkpoint::CheckpointFile;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalMaintenance, WalWriter};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum Op {
    Set(String, i64),
    Inc(String, i64),
    Del(String),
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Snapshot {
    data: Vec<(String, i64)>,
}

fn init(ckpt: Option<Snapshot>) -> BTreeMap<String, i64> {
    match ckpt {
        Some(s) => s.data.into_iter().collect(),
        None => BTreeMap::new(),
    }
}

fn apply(state: &mut BTreeMap<String, i64>, _id: u64, op: Op) {
    match op {
        Op::Set(k, v) => {
            state.insert(k, v);
        }
        Op::Inc(k, delta) => {
            *state.entry(k).or_insert(0) += delta;
        }
        Op::Del(k) => {
            state.remove(&k);
        }
    }
}

fn to_snapshot(state: &BTreeMap<String, i64>) -> Snapshot {
    Snapshot {
        data: state.iter().map(|(k, v)| (k.clone(), *v)).collect(),
    }
}

/// Full lifecycle: write across segments, checkpoint mid-stream, truncate
/// old segments, write more, recover from checkpoint.
#[test]
fn full_lifecycle_checkpoint_truncate_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    // Use PerAppend + unbuffered + tiny segments to force rotation.
    let mut w =
        WalWriter::<Op>::with_options(dir.clone(), durability::storage::FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(128);

    // Phase 1: write 20 ops (forces multiple segments with tiny limit).
    for i in 0..20u64 {
        w.append(&Op::Set(format!("k{i}"), i as i64)).unwrap();
    }

    // Verify multiple segments.
    let segs = dir.list_dir("wal").unwrap();
    let seg_count = segs.iter().filter(|s| s.ends_with(".log")).count();
    assert!(seg_count > 1, "expected multiple segments, got {seg_count}");

    // Phase 2: checkpoint at entry 10 (mid-stream).
    let mid =
        recover_with_wal::<Snapshot, Op, _>(&dir, None, RecoveryOptions::up_to(10), init, apply)
            .unwrap();
    assert_eq!(mid.last_entry_id, 10);

    CheckpointFile::new(dir.clone())
        .write_postcard("ckpt.bin", 10, &to_snapshot(&mid.state))
        .unwrap();

    // Phase 3: truncate only segments fully covered by checkpoint (entry <= 10).
    // Segments containing entries 11-20 survive.
    let maint = WalMaintenance::new(dir.clone());
    let deleted = maint.truncate_prefix(10).unwrap();
    assert!(deleted > 0, "should have truncated at least one segment");

    // Phase 4: write more entries (21-30) using the same writer.
    for i in 20..30u64 {
        w.append(&Op::Set(format!("k{i}"), i as i64)).unwrap();
    }
    w.flush().unwrap();
    drop(w);

    // Phase 5: recover from checkpoint + surviving WAL.
    let final_state = recover_with_wal::<Snapshot, Op, _>(
        &dir,
        Some("ckpt.bin"),
        RecoveryOptions::strict(),
        init,
        apply,
    )
    .unwrap();

    assert_eq!(final_state.last_entry_id, 30);
    // All 30 keys should be present (k0-k9 from checkpoint, k10-k29 from WAL).
    for i in 0..30u64 {
        assert_eq!(
            final_state.state.get(&format!("k{i}")),
            Some(&(i as i64)),
            "missing key k{i}"
        );
    }
}

/// Full recovery from scratch matches recovery from checkpoint.
#[test]
fn checkpoint_recovery_matches_full_replay() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut w = WalWriter::<Op>::new(dir.clone());
    for i in 0..15u64 {
        w.append(&Op::Set(format!("k{i}"), i as i64)).unwrap();
    }
    w.flush().unwrap();

    // Checkpoint at entry 8.
    let mid =
        recover_with_wal::<Snapshot, Op, _>(&dir, None, RecoveryOptions::up_to(8), init, apply)
            .unwrap();
    CheckpointFile::new(dir.clone())
        .write_postcard("ckpt.bin", 8, &to_snapshot(&mid.state))
        .unwrap();

    drop(w);

    // Recover both ways.
    let from_ckpt = recover_with_wal::<Snapshot, Op, _>(
        &dir,
        Some("ckpt.bin"),
        RecoveryOptions::strict(),
        init,
        apply,
    )
    .unwrap();

    let from_scratch =
        recover_with_wal::<Snapshot, Op, _>(&dir, None, RecoveryOptions::strict(), init, apply)
            .unwrap();

    assert_eq!(from_ckpt.state, from_scratch.state);
    assert_eq!(from_ckpt.last_entry_id, from_scratch.last_entry_id);
}

/// Point-in-time recovery after truncation.
#[test]
fn point_in_time_recovery_after_truncation() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut w =
        WalWriter::<Op>::with_options(dir.clone(), durability::storage::FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(128);

    for i in 1..=15u64 {
        w.append(&Op::Set(format!("k{i}"), i as i64)).unwrap();
    }

    // Checkpoint at entry 5.
    CheckpointFile::new(dir.clone())
        .write_postcard(
            "ckpt.bin",
            5,
            &to_snapshot(
                &recover_with_wal::<Snapshot, Op, _>(
                    &dir,
                    None,
                    RecoveryOptions::up_to(5),
                    init,
                    apply,
                )
                .unwrap()
                .state,
            ),
        )
        .unwrap();

    // Truncate segments covered by entry 5.
    WalMaintenance::new(dir.clone()).truncate_prefix(5).unwrap();

    // Write entries 16-20.
    for i in 16..=20u64 {
        w.append(&Op::Set(format!("k{i}"), i as i64)).unwrap();
    }
    w.flush().unwrap();
    drop(w);

    // Point-in-time at entry 12.
    let pit = recover_with_wal::<Snapshot, Op, _>(
        &dir,
        Some("ckpt.bin"),
        RecoveryOptions::up_to(12),
        init,
        apply,
    )
    .unwrap();

    assert_eq!(pit.last_entry_id, 12);
    assert!(pit.state.contains_key("k12"));
    assert!(!pit.state.contains_key("k13"));
    // Keys 1-5 from checkpoint, 6-12 from WAL.
    assert_eq!(pit.state.len(), 12);
}
