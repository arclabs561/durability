//! Minimal “consumer” example for `durability`.
//!
//! This does **not** implement an index. It exercises the durability machinery:
//! - WAL append (segment added, deletes)
//! - checkpoint write
//! - recovery from checkpoint + WAL suffix
//!
//! Run:
//! `cargo run -p durability --example segment_lifecycle`

use durability::checkpointing::CheckpointManager;
use durability::recover::RecoveryManager;
use durability::storage::FsDirectory;
use durability::walog::{WalEntry, WalWriter};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let dir: Arc<dyn durability::Directory> = Arc::new(FsDirectory::new(tmp.path())?);

    let mut wal = WalWriter::new(dir.clone());

    // Segment 1 appears.
    let _ = wal.append(WalEntry::AddSegment {
        entry_id: 0,
        segment_id: 1,
        doc_count: 5,
    })?;
    let _ = wal.append(WalEntry::DeleteDocuments {
        entry_id: 0,
        deletes: vec![(1, 4)],
    })?;
    wal.flush()?;

    // Make a checkpoint.
    let rec = RecoveryManager::new(dir.clone()).recover(None)?;
    let ckpt_state = RecoveryManager::to_checkpoint_state(&rec);
    let ckpt_last = rec.last_entry_id;
    let ckpt_path = "checkpoints/example.ckpt";
    CheckpointManager::new(dir.clone()).write_checkpoint(&ckpt_state, ckpt_last, ckpt_path)?;

    // WAL suffix.
    let _ = wal.append(WalEntry::AddSegment {
        entry_id: 0,
        segment_id: 2,
        doc_count: 7,
    })?;
    wal.flush()?;

    // Recover from checkpoint + suffix.
    let rec2 = RecoveryManager::new(dir).recover(Some(ckpt_path))?;
    println!("recovered last_entry_id={}", rec2.last_entry_id);
    for seg in rec2.segments {
        println!(
            "segment {} doc_count={} deletes={}",
            seg.segment_id,
            seg.doc_count,
            seg.deleted_docs.len()
        );
    }

    Ok(())
}
