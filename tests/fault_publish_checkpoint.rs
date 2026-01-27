//! Fault-injection tests for `CheckpointPublisher`.
//!
//! These tests focus on the crash-consistency nuance:
//! - do not truncate WAL unless the checkpoint commit marker is durably recorded
//! - failures should never make recovery worse (at worst, leave extra WAL segments)

mod support;

use durability::publish::CheckpointPublisher;
use durability::recover::RecoveryManager;
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalSegmentHeader, WalWriter};
use std::sync::Arc;

use support::FaultyDirectory;

fn write_some_wal(dir: Arc<dyn Directory>) -> u64 {
    let mut wal = WalWriter::new(dir);
    wal.append(WalEntry::AddSegment {
        entry_id: 0,
        segment_id: 1,
        doc_count: 5,
    })
    .unwrap();
    let last = wal
        .append(WalEntry::DeleteDocuments {
            entry_id: 0,
            deletes: vec![(1, 4)],
        })
        .unwrap();
    wal.flush().unwrap();
    last
}

fn write_wal_segment(
    dir: &Arc<dyn Directory>,
    seg_id: u64,
    start_entry_id: u64,
    entries: &[WalEntry],
) {
    dir.create_dir_all("wal").unwrap();
    let mut bytes = Vec::new();
    WalSegmentHeader {
        magic: durability::formats::WAL_MAGIC,
        version: durability::formats::FORMAT_VERSION,
        start_entry_id,
        segment_id: seg_id,
    }
    .write(&mut bytes)
    .unwrap();
    for e in entries {
        let enc = WalEntryOnDisk::encode(e).unwrap();
        bytes.extend_from_slice(&enc);
    }
    dir.atomic_write(&format!("wal/wal_{seg_id}.log"), &bytes)
        .unwrap();
}

#[test]
fn publish_fails_before_wal_marker_and_does_not_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let faulty = FaultyDirectory::new(FsDirectory::new(tmp.path()).unwrap());
    let cfg = faulty.cfg();
    let dir: Arc<dyn Directory> = Arc::new(faulty);

    // Create initial WAL (no faults yet).
    let last = write_some_wal(dir.clone());

    // Resume so `append` needs to call `append_file` (fault-injectable path).
    let mut wal = WalWriter::resume(dir.clone()).unwrap();

    // State for checkpoint.
    let mgr = RecoveryManager::new(dir.clone());
    let before = mgr.recover(None).unwrap();
    let ckpt_state = RecoveryManager::to_checkpoint_state(&before);

    // Fail the WAL append path for the checkpoint marker.
    cfg.lock().unwrap().fail_wal_append_file = true;

    let res = CheckpointPublisher::new(dir.clone()).publish_checkpoint(
        &mut wal,
        &ckpt_state,
        last,
        "checkpoints/c1.chk",
    );
    assert!(res.is_err());

    // Checkpoint may exist (it is written first).
    assert!(dir.exists("checkpoints/c1.chk"));

    // WAL must not contain a checkpoint marker, and no truncation deletions should be attempted.
    let entries = WalReader::new(dir.clone()).replay().unwrap();
    assert!(!entries
        .iter()
        .any(|e| matches!(e, WalEntry::Checkpoint { .. })));
    assert_eq!(cfg.lock().unwrap().delete_calls, 0);
}

#[test]
fn publish_fails_on_wal_durability_proof_and_does_not_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let faulty = FaultyDirectory::new(FsDirectory::new(tmp.path()).unwrap());
    let cfg = faulty.cfg();
    let dir: Arc<dyn Directory> = Arc::new(faulty);

    let last = write_some_wal(dir.clone());
    let mut wal = WalWriter::resume(dir.clone()).unwrap();

    let mgr = RecoveryManager::new(dir.clone());
    let before = mgr.recover(None).unwrap();
    let ckpt_state = RecoveryManager::to_checkpoint_state(&before);

    // Allow writing the marker, but pretend we cannot prove durability (`file_path` unavailable).
    cfg.lock().unwrap().fail_wal_file_path = true;

    let res = CheckpointPublisher::new(dir.clone()).publish_checkpoint(
        &mut wal,
        &ckpt_state,
        last,
        "checkpoints/c1.chk",
    );
    assert!(res.is_err());

    // No truncation should have been attempted.
    assert_eq!(cfg.lock().unwrap().delete_calls, 0);

    // Recovery should still succeed (worst case: checkpoint exists but isn't treated as committed).
    let _ = mgr.recover(None).unwrap();
}

#[test]
fn publish_truncation_failure_does_not_break_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let faulty = FaultyDirectory::new(FsDirectory::new(tmp.path()).unwrap());
    let cfg = faulty.cfg();
    let dir: Arc<dyn Directory> = Arc::new(faulty);

    // Create a multi-segment WAL so truncation has something to delete.
    // Segment 1: entries 1..2.
    write_wal_segment(
        &dir,
        1,
        1,
        &[
            WalEntry::AddSegment {
                entry_id: 1,
                segment_id: 1,
                doc_count: 5,
            },
            WalEntry::DeleteDocuments {
                entry_id: 2,
                deletes: vec![(1, 4)],
            },
        ],
    );
    // Segment 2: entry 3.
    write_wal_segment(
        &dir,
        2,
        3,
        &[WalEntry::AddSegment {
            entry_id: 3,
            segment_id: 2,
            doc_count: 1,
        }],
    );

    // Resume from this WAL; next id will be 4.
    let mut wal = WalWriter::resume(dir.clone()).unwrap();

    let mgr = RecoveryManager::new(dir.clone());
    let before = mgr.recover(None).unwrap();
    let ckpt_state = RecoveryManager::to_checkpoint_state(&before);
    let last = before.last_entry_id;

    // Fail deletion during truncation.
    cfg.lock().unwrap().fail_wal_delete = true;

    let res = CheckpointPublisher::new(dir.clone()).publish_checkpoint(
        &mut wal,
        &ckpt_state,
        last,
        "checkpoints/c1.chk",
    );
    assert!(res.is_err());

    // Even if truncation failed, the WAL should now contain a checkpoint marker (write phase ran).
    let entries = WalReader::new(dir.clone()).replay_best_effort().unwrap();
    assert!(entries
        .iter()
        .any(|e| matches!(e, WalEntry::Checkpoint { .. })));

    // Recovery must still succeed from scratch.
    let after = mgr.recover(None).unwrap();
    let seg1 = after.segments.iter().find(|s| s.segment_id == 1).unwrap();
    assert!(seg1.deleted_docs.contains(&4));
}
