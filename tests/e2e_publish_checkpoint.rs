//! End-to-end test for crash-safe checkpoint publishing.

use durability::checkpointing::CheckpointState;
use durability::publish::CheckpointPublisher;
use durability::recover::RecoveryManager;
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;

#[test]
fn publish_checkpoint_records_commit_and_recovery_matches() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    // Create some WAL state.
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

    // Recover and checkpoint.
    let mgr = RecoveryManager::new(dir.clone());
    let before = mgr.recover(None).unwrap();
    let ckpt_state: CheckpointState = RecoveryManager::to_checkpoint_state(&before);
    let last = before.last_entry_id;

    let pubr = CheckpointPublisher::new(dir.clone())
        .publish_checkpoint(&mut wal, &ckpt_state, last, "checkpoints/c1.chk")
        .unwrap();
    assert_eq!(pubr.checkpoint_last_entry_id, last);
    assert!(pubr.wal_checkpoint_entry_id > last);

    // WAL must contain a checkpoint record with that id.
    let records = WalReader::<WalEntry>::new(dir.clone()).replay().unwrap();
    let has = records.iter().any(|r| matches!(&r.payload, WalEntry::Checkpoint { checkpoint_path, last_entry_id, .. }
        if r.entry_id == pubr.wal_checkpoint_entry_id && checkpoint_path == "checkpoints/c1.chk" && *last_entry_id == last));
    assert!(has);

    // After publish+truncate, recovery should prefer the latest checkpoint marker.
    let from_ckpt = mgr.recover(Some("checkpoints/c1.chk")).unwrap();
    let from_scratch = mgr.recover_latest().unwrap();
    assert_eq!(from_ckpt.last_entry_id, from_scratch.last_entry_id);
    assert_eq!(from_ckpt.segments.len(), from_scratch.segments.len());
    // And specifically the delete should persist.
    let seg1 = from_ckpt
        .segments
        .iter()
        .find(|s| s.segment_id == 1)
        .unwrap();
    assert!(seg1.deleted_docs.contains(&4));

    // Basic monotonicity sanity for the recorded checkpoint entry id.
    let max_id = records.iter().map(|r| r.entry_id).max().unwrap_or(0);
    assert_eq!(max_id, pubr.wal_checkpoint_entry_id);
}
