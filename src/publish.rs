//! Crash-safe checkpoint publishing and WAL truncation helpers.
//!
//! This module exists because "checkpoint + WAL + truncation" is where real systems most
//! often get the durability story wrong. The safe rule is:
//!
//! 1) write checkpoint (ideally with stable-storage barriers),
//! 2) record "checkpoint committed" in the WAL and make THAT durable,
//! 3) only then delete/truncate WAL segments that are fully covered by the checkpoint.
//!
//! Two APIs are provided:
//!
//! - **Generic**: [`CheckpointPublisher::publish`] works with any entry type `E` and
//!   checkpoint state `C`. The caller provides a closure to construct the WAL entry
//!   that records the checkpoint.
//!
//! - **Domain-specific**: [`CheckpointPublisher::publish_checkpoint`] is a convenience
//!   wrapper for `WalEntry`-based segment-index WALs.

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
    /// WAL entry id of the checkpoint-recording WAL record.
    pub wal_checkpoint_entry_id: u64,
    /// Number of WAL segment files deleted as part of truncation.
    pub deleted_wal_segments: usize,
}

/// Coordinates a safe checkpoint publish and optional WAL truncation.
///
/// # Example (generic)
///
/// ```
/// use durability::publish::CheckpointPublisher;
/// use durability::walog::WalWriter;
/// use durability::storage::FsDirectory;
/// use std::sync::Arc;
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Snap { counter: u64 }
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// enum Op { Inc, Dec, Checkpoint { path: String, last_id: u64 } }
///
/// let tmp = tempfile::tempdir().unwrap();
/// let dir = FsDirectory::arc(tmp.path()).unwrap();
/// let mut wal = WalWriter::<Op>::new(dir.clone());
///
/// wal.append(&Op::Inc).unwrap();
/// wal.append(&Op::Inc).unwrap();
/// wal.flush().unwrap();
///
/// let publisher = CheckpointPublisher::new(dir);
/// let result = publisher.publish(
///     &mut wal,
///     &Snap { counter: 2 },
///     2,
///     "snap.bin",
///     |path, last_id| Op::Checkpoint { path: path.to_string(), last_id },
/// ).unwrap();
/// assert_eq!(result.checkpoint_last_entry_id, 2);
/// assert_eq!(result.wal_checkpoint_entry_id, 3);
/// ```
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
    /// Generic over any WAL entry type `E` and checkpoint state `C`.
    /// The `make_wal_entry` closure constructs the WAL entry that records the
    /// checkpoint; it receives `(checkpoint_path, checkpoint_last_entry_id)`.
    pub fn publish<C, E>(
        &self,
        wal: &mut WalWriter<E>,
        state: &C,
        checkpoint_last_entry_id: u64,
        checkpoint_path: &str,
        make_wal_entry: impl FnOnce(&str, u64) -> E,
    ) -> PersistenceResult<PublishResult>
    where
        C: serde::Serialize,
        E: serde::Serialize + serde::de::DeserializeOwned,
    {
        let ckpt = CheckpointFile::new(self.directory.clone());
        ckpt.write_postcard_durable(checkpoint_path, checkpoint_last_entry_id, state)?;

        let entry = make_wal_entry(checkpoint_path, checkpoint_last_entry_id);
        let wal_checkpoint_entry_id = wal.append(&entry)?;
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

    /// Publish a checkpoint for a `WalEntry`-based segment-index WAL.
    ///
    /// Convenience wrapper around [`CheckpointPublisher::publish`] that constructs
    /// a [`WalEntry::Checkpoint`] record automatically.
    pub fn publish_checkpoint(
        &self,
        wal: &mut WalWriter<WalEntry>,
        state: &CheckpointState,
        checkpoint_last_entry_id: u64,
        checkpoint_path: &str,
    ) -> PersistenceResult<PublishResult> {
        self.publish(
            wal,
            state,
            checkpoint_last_entry_id,
            checkpoint_path,
            |path, last_id| WalEntry::Checkpoint {
                checkpoint_path: path.to_string(),
                last_entry_id: last_id,
            },
        )
    }
}
