//! Crash-safe checkpoint publishing and WAL truncation helpers.
//!
//! This module exists because "checkpoint + WAL + truncation" is where real systems most
//! often get the durability story wrong. The safe rule is:
//!
//! 1) write checkpoint (ideally with stable-storage barriers),
//! 2) record "checkpoint committed" in the WAL and make THAT durable,
//! 3) only then delete/truncate WAL segments that are fully covered by the checkpoint.

use crate::checkpoint::CheckpointFile;
use crate::error::PersistenceResult;
use crate::recover::CheckpointState;
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
    pub fn publish_checkpoint(
        &self,
        wal: &mut WalWriter<WalEntry>,
        state: &CheckpointState,
        checkpoint_last_entry_id: u64,
        checkpoint_path: &str,
    ) -> PersistenceResult<PublishResult> {
        let ckpt = CheckpointFile::new(self.directory.clone());
        ckpt.write_postcard_durable(checkpoint_path, checkpoint_last_entry_id, state)?;

        let wal_checkpoint_entry_id = wal.append(&WalEntry::Checkpoint {
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
