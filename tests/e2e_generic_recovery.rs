//! Integration tests for the generic `recover_with_wal` function.
//!
//! These tests prove the generic recovery works with fully custom types
//! (not WalEntry), validating the F3 generalization.

use durability::checkpoint::CheckpointFile;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::{Directory, FsDirectory, MemoryDirectory};
use durability::walog::WalWriter;
use std::sync::Arc;

// -- Custom domain types (completely independent of WalEntry) ----------------

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum KvOp {
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct KvCheckpoint {
    entries: Vec<(String, String)>,
}

type KvState = std::collections::HashMap<String, String>;

fn kv_init(ckpt: Option<KvCheckpoint>) -> KvState {
    match ckpt {
        Some(c) => c.entries.into_iter().collect(),
        None => KvState::new(),
    }
}

fn kv_apply(state: &mut KvState, _entry_id: u64, op: KvOp) {
    match op {
        KvOp::Put { key, value } => {
            state.insert(key, value);
        }
        KvOp::Delete { key } => {
            state.remove(&key);
        }
    }
}

fn kv_to_checkpoint(state: &KvState) -> KvCheckpoint {
    let mut entries: Vec<(String, String)> =
        state.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    entries.sort();
    KvCheckpoint { entries }
}

// -- Tests -------------------------------------------------------------------

#[test]
fn generic_recovery_with_no_checkpoint_and_no_wal() {
    let dir = MemoryDirectory::arc();
    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::strict(),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert!(result.state.is_empty());
    assert_eq!(result.last_entry_id, 0);
}

#[test]
fn generic_recovery_with_wal_only() {
    let dir = MemoryDirectory::arc();

    // Write some KV ops to WAL.
    let mut w = WalWriter::<KvOp>::new(dir.clone());
    w.append(&KvOp::Put {
        key: "a".into(),
        value: "1".into(),
    })
    .unwrap();
    w.append(&KvOp::Put {
        key: "b".into(),
        value: "2".into(),
    })
    .unwrap();
    w.append(&KvOp::Delete { key: "a".into() }).unwrap();
    w.flush().unwrap();
    drop(w);

    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::strict(),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert_eq!(result.state.len(), 1);
    assert_eq!(result.state.get("b"), Some(&"2".to_string()));
    assert!(!result.state.contains_key("a"));
    assert_eq!(result.last_entry_id, 3);
}

#[test]
fn generic_recovery_with_checkpoint_then_wal() {
    let dir = MemoryDirectory::arc();

    // Write initial ops.
    let mut w = WalWriter::<KvOp>::new(dir.clone());
    w.append(&KvOp::Put {
        key: "x".into(),
        value: "10".into(),
    })
    .unwrap();
    w.append(&KvOp::Put {
        key: "y".into(),
        value: "20".into(),
    })
    .unwrap();
    w.flush().unwrap();

    // Recover to build state, then checkpoint at entry 2.
    let mid = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::strict(),
        kv_init,
        kv_apply,
    )
    .unwrap();
    assert_eq!(mid.last_entry_id, 2);

    let ckpt_state = kv_to_checkpoint(&mid.state);
    let ckpt = CheckpointFile::new(dir.clone());
    ckpt.write_postcard("checkpoints/kv.chk", mid.last_entry_id, &ckpt_state)
        .unwrap();

    // Write more ops after checkpoint.
    w.append(&KvOp::Put {
        key: "z".into(),
        value: "30".into(),
    })
    .unwrap();
    w.append(&KvOp::Delete { key: "x".into() }).unwrap();
    w.flush().unwrap();
    drop(w);

    // Recover with checkpoint -- should have x deleted, y=20, z=30.
    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        Some("checkpoints/kv.chk"),
        RecoveryOptions::strict(),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert_eq!(result.last_entry_id, 4);
    assert!(!result.state.contains_key("x"));
    assert_eq!(result.state.get("y"), Some(&"20".to_string()));
    assert_eq!(result.state.get("z"), Some(&"30".to_string()));
}

#[test]
fn generic_recovery_best_effort_ignores_corrupt_checkpoint() {
    let dir = MemoryDirectory::arc();

    // Write a corrupt checkpoint.
    dir.atomic_write("checkpoints/bad.chk", b"CORRUPT_DATA")
        .unwrap();

    // Write some WAL entries.
    let mut w = WalWriter::<KvOp>::new(dir.clone());
    w.append(&KvOp::Put {
        key: "k".into(),
        value: "v".into(),
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    // Strict mode should error.
    let err = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        Some("checkpoints/bad.chk"),
        RecoveryOptions::strict(),
        kv_init,
        kv_apply,
    );
    assert!(err.is_err());

    // Best-effort should skip checkpoint and recover from WAL.
    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        Some("checkpoints/bad.chk"),
        RecoveryOptions::best_effort(),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert_eq!(result.state.get("k"), Some(&"v".to_string()));
    assert_eq!(result.last_entry_id, 1);
}

#[test]
fn generic_recovery_missing_checkpoint_path_is_clean_start() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<KvOp>::new(dir.clone());
    w.append(&KvOp::Put {
        key: "a".into(),
        value: "1".into(),
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    // Checkpoint path doesn't exist -- should recover from WAL alone.
    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        Some("checkpoints/nonexistent.chk"),
        RecoveryOptions::strict(),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert_eq!(result.state.get("a"), Some(&"1".to_string()));
    assert_eq!(result.last_entry_id, 1);
}

#[test]
fn point_in_time_recovery_stops_at_entry() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<KvOp>::new(dir.clone());
    for i in 1..=10u64 {
        w.append(&KvOp::Put {
            key: format!("k{i}"),
            value: format!("v{i}"),
        })
        .unwrap();
    }
    w.flush().unwrap();
    drop(w);

    // Recover up to entry 5.
    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::up_to(5),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert_eq!(result.last_entry_id, 5);
    assert_eq!(result.state.len(), 5);
    assert!(result.state.contains_key("k5"));
    assert!(!result.state.contains_key("k6"));
}

#[test]
fn point_in_time_recovery_with_checkpoint_before_cutoff() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<KvOp>::new(dir.clone());
    for i in 1..=5u64 {
        w.append(&KvOp::Put {
            key: format!("k{i}"),
            value: format!("v{i}"),
        })
        .unwrap();
    }
    w.flush().unwrap();

    // Checkpoint at entry 3.
    let mid = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::up_to(3),
        kv_init,
        kv_apply,
    )
    .unwrap();
    let ckpt_state = kv_to_checkpoint(&mid.state);
    let ckpt = CheckpointFile::new(dir.clone());
    ckpt.write_postcard("checkpoints/kv3.chk", 3, &ckpt_state)
        .unwrap();

    // Write entries 6-10.
    for i in 6..=10u64 {
        w.append(&KvOp::Put {
            key: format!("k{i}"),
            value: format!("v{i}"),
        })
        .unwrap();
    }
    w.flush().unwrap();
    drop(w);

    // Recover from checkpoint (entry 3) up to entry 7.
    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        Some("checkpoints/kv3.chk"),
        RecoveryOptions::up_to(7),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert_eq!(result.last_entry_id, 7);
    // Should have k1-k7 (k1-k3 from checkpoint, k4-k7 from WAL replay).
    assert_eq!(result.state.len(), 7);
    assert!(result.state.contains_key("k7"));
    assert!(!result.state.contains_key("k8"));
}

#[test]
fn point_in_time_with_cutoff_zero_applies_nothing() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<KvOp>::new(dir.clone());
    w.append(&KvOp::Put {
        key: "a".into(),
        value: "1".into(),
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::up_to(0),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert!(result.state.is_empty());
    assert_eq!(result.last_entry_id, 0);
}

#[test]
fn generic_recovery_on_fs_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut w = WalWriter::<KvOp>::new(dir.clone());
    w.append(&KvOp::Put {
        key: "fs_key".into(),
        value: "fs_val".into(),
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    let result = recover_with_wal::<KvCheckpoint, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::strict(),
        kv_init,
        kv_apply,
    )
    .unwrap();

    assert_eq!(result.state.get("fs_key"), Some(&"fs_val".to_string()));
}
