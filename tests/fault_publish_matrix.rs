//! Fault-injection matrix for `CheckpointPublisher`.
//!
//! We vary the failure point and assert the safety invariants:
//! - no truncation unless the WAL checkpoint marker is durably recorded
//! - failures never reduce recoverability (worst case: extra WAL remains)

mod support;

use durability::publish::CheckpointPublisher;
use durability::recover::RecoveryManager;
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;

use support::FaultyDirectory;

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
    let mut wal = WalWriter::new(dir.clone());
    let id1 = wal
        .append(WalEntry::AddSegment {
            entry_id: 0,
            segment_id: 1,
            doc_count: 5,
        })
        .unwrap();
    let id2 = wal
        .append(WalEntry::DeleteDocuments {
            entry_id: 0,
            deletes: vec![(1, 4)],
        })
        .unwrap();
    wal.flush_and_sync().unwrap();

    // Resume so marker append uses append_file path.
    let mut wal = WalWriter::resume(dir.clone()).unwrap();
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
        FailPoint::None => assert!(res.is_ok()),
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
    let entries = WalReader::new(dir).replay_best_effort().unwrap_or_default();
    let has_marker = entries
        .iter()
        .any(|e| matches!(e, WalEntry::Checkpoint { .. }));
    match fp {
        FailPoint::None | FailPoint::WalDelete => assert!(has_marker),
        FailPoint::WalAppendFile => assert!(!has_marker),
        // If `flush_and_sync` fails due to missing file_path, the marker may have been appended
        // but not proven durable; treat both outcomes as acceptable (the safety invariant is
        // “no truncation + recoverability preserved”).
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
