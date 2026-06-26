//! Fault-injection matrix for `CheckpointPublisher`.
//!
//! We vary the failure point and assert the safety invariants:
//! - no truncation unless the WAL checkpoint marker is durably recorded
//! - failures never reduce recoverability (worst case: extra WAL remains)

mod support;

use durability::checkpoint::CheckpointFile;
use durability::publish::CheckpointPublisher;
use durability::recover::{CheckpointSegment, CheckpointState, RecoveryManager};
use durability::storage::{Directory, FlushPolicy, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;

use support::FaultyDirectory;

fn checkpoint_state(segment_id: u64, doc_count: u32, deleted_docs: Vec<u32>) -> CheckpointState {
    CheckpointState {
        segments: vec![CheckpointSegment {
            segment_id,
            doc_count,
            deleted_docs,
        }],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailPoint {
    None,
    // Fail when WAL marker append would need to open append handle.
    WalAppendFile,
    // Fail when WAL marker attempts stable-storage proof (file_path missing).
    WalFilePath,
    // Fail delete during truncation.
    WalDelete,
}

fn run_case(fp: FailPoint) {
    let tmp = tempfile::tempdir().unwrap();
    let faulty = FaultyDirectory::new(FsDirectory::new(tmp.path()).unwrap());
    let cfg = faulty.cfg();
    let dir: Arc<dyn Directory> = Arc::new(faulty);

    // Create some WAL state.
    let mut wal = WalWriter::<WalEntry>::new(dir.clone());
    let id1 = wal
        .append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 5,
        })
        .unwrap();
    let id2 = wal
        .append(&WalEntry::DeleteDocuments {
            deletes: vec![(1, 4)],
        })
        .unwrap();
    wal.flush_and_sync().unwrap();

    // Restart so marker append uses append_file path.
    drop(wal);
    let mut wal = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
    // Force the checkpoint marker into a *new* segment so truncation has a prefix
    // segment it can delete (segment 1 end_entry_id == checkpoint_last_entry_id).
    wal.set_segment_size_limit_bytes(1);

    // Checkpoint state from recovery.
    let mgr = RecoveryManager::new(dir.clone());
    let before = mgr.recover(None).unwrap();
    let ckpt_state = RecoveryManager::to_checkpoint_state(&before);
    let last = before.last_entry_id;
    assert_eq!(last, id2);
    assert_eq!(id1, 1);

    // Configure fault.
    {
        let mut c = cfg.lock().unwrap();
        match fp {
            FailPoint::None => {}
            FailPoint::WalAppendFile => c.fail_wal_append_file = true,
            FailPoint::WalFilePath => c.fail_wal_file_path = true,
            FailPoint::WalDelete => c.fail_wal_delete = true,
        }
    }

    let res = CheckpointPublisher::new(dir.clone()).publish_checkpoint(
        &mut wal,
        &ckpt_state,
        last,
        "checkpoints/m.chk",
    );

    match fp {
        FailPoint::None => {
            let pubr = res.unwrap();
            assert_eq!(pubr.checkpoint_path, "checkpoints/m.chk");
            assert_eq!(pubr.checkpoint_last_entry_id, last);
            assert!(pubr.wal_checkpoint_entry_id > last);
            assert!(pubr.deleted_wal_segments >= 1);
        }
        _ => assert!(res.is_err()),
    }

    // Safety invariant:
    // - When publish succeeds (or fails during delete), we should have attempted deletion.
    // - When publish fails before truncation (append/sync), we should not attempt deletion.
    let del_calls = cfg.lock().unwrap().delete_calls;
    if matches!(fp, FailPoint::None | FailPoint::WalDelete) {
        assert!(del_calls >= 1);
    } else {
        assert_eq!(del_calls, 0);
    }

    // Recoverability invariant: best-effort latest recovery must succeed.
    let after = mgr.recover_latest_best_effort().unwrap();
    let seg1 = after.segments.iter().find(|s| s.segment_id == 1).unwrap();
    assert!(seg1.deleted_docs.contains(&4));

    // Marker presence: only guaranteed when marker append+sync succeeded.
    let records = WalReader::<WalEntry>::new(dir)
        .replay_best_effort()
        .unwrap_or_default();
    let has_marker = records
        .iter()
        .any(|r| matches!(&r.payload, WalEntry::Checkpoint { .. }));
    match fp {
        FailPoint::None | FailPoint::WalDelete => assert!(has_marker),
        FailPoint::WalAppendFile => assert!(!has_marker),
        // If `flush_and_sync` fails due to missing file_path, the marker may have been appended
        // but not proven durable; treat both outcomes as acceptable (the safety invariant is
        // "no truncation + recoverability preserved").
        FailPoint::WalFilePath => {}
    }
}

#[test]
fn publish_fault_matrix() {
    for fp in [
        FailPoint::None,
        FailPoint::WalAppendFile,
        FailPoint::WalFilePath,
        FailPoint::WalDelete,
    ] {
        run_case(fp);
    }
}

#[test]
fn recover_latest_errors_when_committed_checkpoint_file_is_missing_after_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut wal = WalWriter::<WalEntry>::new(dir.clone());
    wal.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 5,
    })
    .unwrap();
    wal.append(&WalEntry::DeleteDocuments {
        deletes: vec![(1, 4)],
    })
    .unwrap();
    wal.flush_and_sync().unwrap();

    drop(wal);
    let mut wal = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
    wal.set_segment_size_limit_bytes(1);

    let mgr = RecoveryManager::new(dir.clone());
    let before = mgr.recover(None).unwrap();
    let ckpt_state = RecoveryManager::to_checkpoint_state(&before);
    let last = before.last_entry_id;

    let published = CheckpointPublisher::new(dir.clone())
        .publish_checkpoint(&mut wal, &ckpt_state, last, "checkpoints/missing.chk")
        .unwrap();
    assert!(published.deleted_wal_segments >= 1);

    dir.delete("checkpoints/missing.chk").unwrap();
    assert!(!dir.exists("wal/wal_1.log"));

    let err = mgr.recover_latest().unwrap_err();
    assert!(err.to_string().contains("missing checkpoint"));
}

#[test]
fn recover_latest_ignores_missing_checkpoint_marker_when_full_wal_remains() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut wal = WalWriter::<WalEntry>::new(dir.clone());
    wal.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 5,
    })
    .unwrap();
    wal.append(&WalEntry::DeleteDocuments {
        deletes: vec![(1, 4)],
    })
    .unwrap();
    wal.append(&WalEntry::Checkpoint {
        checkpoint_path: "checkpoints/missing.chk".to_string(),
        last_entry_id: 2,
    })
    .unwrap();
    wal.flush_and_sync().unwrap();
    drop(wal);

    let recovered = RecoveryManager::new(dir).recover_latest().unwrap();
    assert_eq!(recovered.last_entry_id, 3);
    let seg1 = recovered
        .segments
        .iter()
        .find(|s| s.segment_id == 1)
        .unwrap();
    assert!(seg1.deleted_docs.contains(&4));
}

#[test]
fn recover_latest_errors_when_wal_prefix_is_missing_without_usable_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut wal = WalWriter::<WalEntry>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    wal.set_segment_size_limit_bytes(1);
    for segment_id in 1..=3 {
        wal.append(&WalEntry::AddSegment {
            segment_id,
            doc_count: 1,
        })
        .unwrap();
    }
    wal.flush_and_sync().unwrap();
    drop(wal);

    assert!(dir.exists("wal/wal_2.log"));
    dir.delete("wal/wal_1.log").unwrap();

    let err = RecoveryManager::new(dir).recover_latest().unwrap_err();
    assert!(err.to_string().contains("WAL prefix is unavailable"));
}

#[test]
fn recover_latest_uses_newest_existing_checkpoint_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
    let checkpoints = CheckpointFile::new(dir.clone());

    let mut wal = WalWriter::<WalEntry>::new(dir.clone());
    wal.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 1,
    })
    .unwrap();
    checkpoints
        .write_postcard("checkpoints/older.chk", 1, &checkpoint_state(1, 10, vec![]))
        .unwrap();
    wal.append(&WalEntry::Checkpoint {
        checkpoint_path: "checkpoints/older.chk".to_string(),
        last_entry_id: 1,
    })
    .unwrap();
    wal.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 20,
    })
    .unwrap();
    checkpoints
        .write_postcard(
            "checkpoints/newer.chk",
            3,
            &checkpoint_state(1, 30, vec![7]),
        )
        .unwrap();
    wal.append(&WalEntry::Checkpoint {
        checkpoint_path: "checkpoints/newer.chk".to_string(),
        last_entry_id: 3,
    })
    .unwrap();
    wal.flush_and_sync().unwrap();
    drop(wal);

    let recovered = RecoveryManager::new(dir).recover_latest().unwrap();
    assert_eq!(recovered.last_entry_id, 4);
    let seg1 = recovered
        .segments
        .iter()
        .find(|s| s.segment_id == 1)
        .unwrap();
    assert_eq!(seg1.doc_count, 30);
    assert!(seg1.deleted_docs.contains(&7));
}

#[test]
fn recover_latest_reports_newest_missing_checkpoint_marker_after_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut wal = WalWriter::<WalEntry>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    wal.set_segment_size_limit_bytes(1);
    wal.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 1,
    })
    .unwrap();
    wal.append(&WalEntry::Checkpoint {
        checkpoint_path: "checkpoints/older-missing.chk".to_string(),
        last_entry_id: 1,
    })
    .unwrap();
    wal.append(&WalEntry::AddSegment {
        segment_id: 2,
        doc_count: 1,
    })
    .unwrap();
    wal.append(&WalEntry::Checkpoint {
        checkpoint_path: "checkpoints/newer-missing.chk".to_string(),
        last_entry_id: 3,
    })
    .unwrap();
    wal.flush_and_sync().unwrap();
    drop(wal);

    assert!(dir.exists("wal/wal_2.log"));
    dir.delete("wal/wal_1.log").unwrap();

    let err = RecoveryManager::new(dir).recover_latest().unwrap_err();
    assert!(err.to_string().contains("newer-missing.chk"));
}
