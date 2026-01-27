//! `durability`: crash-consistent persistence primitives for segment-based indices.
//!
//! Scope:
//! - directory abstraction (`Directory`)
//! - on-disk framing constants (`formats`)
//! - write-ahead log (`walog`)
//! - checkpoints (`checkpointing`)
//! - crash recovery from WAL (`recover`)
//!
//! Non-goal: indexing algorithms or ranking (those belong in crates like `postings` / `jin`).
//!
//! ## Contract (what you can rely on)
//!
//! This crate is designed around two different “strength levels”:
//!
//! - **Crash-consistent + integrity-checked** (default)
//!   - Detects corruption (CRC/magic/version/type mismatches) and errors loudly.
//!   - Supports best-effort recovery of a **torn tail** (partial record write) in the **final**
//!     log segment.
//!   - Guarantees a **prefix property** under best-effort replay: recovered operations are a
//!     prefix of the successfully written operation stream (no garbage / no reordering).
//! - **Stable-storage durability** (opt-in)
//!   - Requires explicit barriers (`fsync`/`sync_all`) and sometimes **parent directory sync**.
//!   - Use [`storage::sync_file`] / [`storage::sync_parent_dir`] and `flush_and_sync()` helpers
//!     where you need “survives power loss after success” semantics.
//!
//! Terminology:
//! - `flush()` is a **visibility boundary**, not a stable-storage guarantee.
//! - “Best-effort” is intentionally narrow; it never masks corruption.
//!
//! Note: this crate intentionally exposes *traits and framing*.
//! Higher-level crates generally decide directory layout, naming, and lifecycle policies,
//! but some primitives (notably `walog`) assume a conventional `wal/` directory.

pub mod checkpoint;
pub mod checkpointing;
pub mod error;
pub mod formats;
pub mod publish;
pub mod recordlog;
pub mod recover;
pub mod replay;
pub mod storage;
pub mod walog;

pub use error::{PersistenceError, PersistenceResult};
pub use publish::{CheckpointPublisher, PublishResult};
pub use storage::{Directory, DurableDirectory, FsDirectory, MemoryDirectory};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpointing::{CheckpointManager, CheckpointState};
    use crate::recover::RecoveryManager;
    use crate::walog::{WalEntry, WalWriter};
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn wal_recovery_roundtrip_in_memory() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        let mut w = WalWriter::new(dir.clone());
        let id1 = w
            .append(WalEntry::AddSegment {
                entry_id: 0,
                segment_id: 10,
                doc_count: 3,
            })
            .unwrap();
        assert_eq!(id1, 1);

        let id2 = w
            .append(WalEntry::DeleteDocuments {
                entry_id: 0,
                deletes: vec![(10, 2)],
            })
            .unwrap();
        assert_eq!(id2, 2);

        w.flush().unwrap();

        let mgr = RecoveryManager::new(dir);
        let state = mgr.recover(None).unwrap();
        assert_eq!(state.last_entry_id, 2);
        assert_eq!(state.segments.len(), 1);
        assert_eq!(state.segments[0].segment_id, 10);
        assert_eq!(state.segments[0].doc_count, 3);

        let dels: HashSet<u32> = state.segments[0].deleted_docs.iter().copied().collect();
        assert!(dels.contains(&2));
        assert!(!dels.contains(&1));
    }

    #[test]
    fn checkpoint_then_wal_suffix_applies() {
        let tmp = tempfile::tempdir().unwrap();
        let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

        // WAL prefix.
        let mut w = WalWriter::new(dir.clone());
        w.append(WalEntry::AddSegment {
            entry_id: 0,
            segment_id: 1,
            doc_count: 5,
        })
        .unwrap();
        w.append(WalEntry::DeleteDocuments {
            entry_id: 0,
            deletes: vec![(1, 4)],
        })
        .unwrap();

        w.flush().unwrap();

        // Build checkpoint from recovered prefix.
        let mgr = RecoveryManager::new(dir.clone());
        let prefix = mgr.recover(None).unwrap();
        let ckpt_state: CheckpointState = RecoveryManager::to_checkpoint_state(&prefix);
        let ckpt_last = prefix.last_entry_id;

        let ckpt_path = "checkpoints/c1.bin";
        let ckpt = CheckpointManager::new(dir.clone());
        ckpt.write_checkpoint(&ckpt_state, ckpt_last, ckpt_path)
            .unwrap();

        // WAL suffix (should apply after checkpoint).
        w.append(WalEntry::AddSegment {
            entry_id: 0,
            segment_id: 2,
            doc_count: 7,
        })
        .unwrap();

        w.flush().unwrap();

        let after = mgr.recover(Some(ckpt_path)).unwrap();
        assert!(after.segments.iter().any(|s| s.segment_id == 1));
        assert!(after.segments.iter().any(|s| s.segment_id == 2));

        let seg1 = after.segments.iter().find(|s| s.segment_id == 1).unwrap();
        assert!(seg1.deleted_docs.contains(&4));
    }
}
