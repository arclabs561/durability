//! Crash-safe checkpoint publishing and WAL truncation helpers.
//!
//! This module exists because “checkpoint + WAL + truncation” is where real systems most
//! often get the durability story wrong. The safe rule is:
//!
//! 1) write checkpoint (ideally with stable-storage barriers),
//! 2) record "checkpoint committed" in the WAL and make THAT durable,
//! 3) only then delete/truncate WAL segments that are fully covered by the checkpoint.

use crate::checkpointing::{CheckpointManager, CheckpointState};
use crate::error::PersistenceResult;
use crate::storage::Directory;
use crate::walog::{WalEntry, WalMaintenance, WalWriter};
use std::sync::Arc;

/// Result of publishing a checkpoint.
#[derive(Debug, Clone)]
pub struct PublishResult {
    /// Path where the checkpoint was written.
    pub checkpoint_path: String,
    /// The last entry id included in the checkpoint (i.e. replay entries with id > this).
    pub checkpoint_last_entry_id: u64,
    /// WAL entry id of the `WalEntry::Checkpoint` record.
    pub wal_checkpoint_entry_id: u64,
    /// Number of WAL segment files deleted as part of truncation.
    pub deleted_wal_segments: usize,
}

/// Coordinates a safe checkpoint publish and optional WAL truncation.
pub struct CheckpointPublisher {
    directory: Arc<dyn Directory>,
}

impl CheckpointPublisher {
    /// Create a checkpoint publisher for a directory backend.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// Publish a checkpoint and (safely) truncate WAL segments covered by it.
    ///
    /// Steps:
    /// 1) write the checkpoint with stable-storage barriers (`write_checkpoint_durable`)
    /// 2) append `WalEntry::Checkpoint` and make that WAL write durable
    /// 3) delete WAL segments whose max entry id is `<= checkpoint_last_entry_id`
    ///
    /// Safety notes:
    /// - If step (1) succeeds but step (2) fails, the checkpoint may exist but is not recorded
    ///   as committed in the WAL. We return an error and do NOT truncate.
    /// - Truncation is best-effort: deletion failures are returned as errors, but directory-sync
    ///   after deletion is not required for correctness (it only affects “durable deletion”).
    pub fn publish_checkpoint(
        &self,
        wal: &mut WalWriter,
        state: &CheckpointState,
        checkpoint_last_entry_id: u64,
        checkpoint_path: &str,
    ) -> PersistenceResult<PublishResult> {
        let mgr = CheckpointManager::new(self.directory.clone());
        mgr.write_checkpoint_durable(state, checkpoint_last_entry_id, checkpoint_path)?;

        let wal_checkpoint_entry_id = wal.append(WalEntry::Checkpoint {
            entry_id: 0,
            checkpoint_path: checkpoint_path.to_string(),
            last_entry_id: checkpoint_last_entry_id,
        })?;
        wal.flush_and_sync()?;

        let deleted_wal_segments = WalMaintenance::new(self.directory.clone())
            .truncate_prefix(checkpoint_last_entry_id)?;

        Ok(PublishResult {
            checkpoint_path: checkpoint_path.to_string(),
            checkpoint_last_entry_id,
            wal_checkpoint_entry_id,
            deleted_wal_segments,
        })
    }
}
