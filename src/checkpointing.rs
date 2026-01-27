//! Checkpoint files for durable state snapshots.

use crate::checkpoint::CheckpointFile;
use crate::error::PersistenceResult;
use crate::storage::Directory;
use std::sync::Arc;

/// The durable index state stored in a checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointState {
    /// Known segments and their delete sets.
    pub segments: Vec<CheckpointSegment>,
}

/// Per-segment data stored in the checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointSegment {
    /// Segment identifier.
    pub segment_id: u64,
    /// Number of documents in that segment.
    pub doc_count: u32,
    /// Deleted doc ids within that segment.
    pub deleted_docs: Vec<u32>,
}

/// Read/write checkpoint files in a `Directory`.
pub struct CheckpointManager {
    checkpoint: CheckpointFile,
}

impl CheckpointManager {
    /// Create a checkpoint manager for a directory backend.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self {
            checkpoint: CheckpointFile::new(directory),
        }
    }

    /// Write a checkpoint file.
    pub fn write_checkpoint(
        &self,
        state: &CheckpointState,
        last_entry_id: u64,
        checkpoint_path: &str,
    ) -> PersistenceResult<()> {
        self.checkpoint
            .write_postcard(checkpoint_path, last_entry_id, state)
    }

    /// Write a checkpoint and attempt to make it durable on stable storage.
    ///
    /// See [`CheckpointFile::write_postcard_durable`] for semantics.
    pub fn write_checkpoint_durable(
        &self,
        state: &CheckpointState,
        last_entry_id: u64,
        checkpoint_path: &str,
    ) -> PersistenceResult<()> {
        self.checkpoint
            .write_postcard_durable(checkpoint_path, last_entry_id, state)
    }

    /// Read a checkpoint file, returning (state, last_entry_id).
    pub fn read_checkpoint(
        &self,
        checkpoint_path: &str,
    ) -> PersistenceResult<(CheckpointState, u64)> {
        let (last, state) = self.checkpoint.read_postcard(checkpoint_path)?;
        Ok((state, last))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{FsDirectory, MemoryDirectory};
    use std::io::Read;

    #[test]
    fn checkpoint_roundtrip_in_memory() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mgr = CheckpointManager::new(dir);

        let state = CheckpointState {
            segments: vec![
                CheckpointSegment {
                    segment_id: 1,
                    doc_count: 3,
                    deleted_docs: vec![0, 2],
                },
                CheckpointSegment {
                    segment_id: 2,
                    doc_count: 5,
                    deleted_docs: vec![],
                },
            ],
        };

        mgr.write_checkpoint(&state, 42, "checkpoints/c1.chk")
            .unwrap();

        let (state2, last) = mgr.read_checkpoint("checkpoints/c1.chk").unwrap();
        assert_eq!(last, 42);
        assert_eq!(state2.segments.len(), 2);
        assert_eq!(state2.segments[0].segment_id, 1);
        assert_eq!(state2.segments[0].deleted_docs, vec![0, 2]);
        assert_eq!(state2.segments[1].segment_id, 2);
        assert_eq!(state2.segments[1].deleted_docs, Vec::<u32>::new());
    }

    #[test]
    fn checkpoint_durable_requires_fs_backend() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mgr = CheckpointManager::new(dir.clone());
        let state = CheckpointState { segments: vec![] };

        let err = mgr
            .write_checkpoint_durable(&state, 1, "checkpoints/c1.chk")
            .unwrap_err();
        assert!(matches!(err, crate::PersistenceError::NotSupported(_)));
        assert!(!dir.exists("checkpoints/c1.chk"));
    }

    #[test]
    fn checkpoint_durable_roundtrip_fs() {
        let tmp = tempfile::tempdir().unwrap();
        let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
        let mgr = CheckpointManager::new(dir.clone());

        let state = CheckpointState {
            segments: vec![CheckpointSegment {
                segment_id: 1,
                doc_count: 3,
                deleted_docs: vec![2, 0],
            }],
        };

        mgr.write_checkpoint_durable(&state, 9, "checkpoints/c1.chk")
            .unwrap();
        let (state2, last) = mgr.read_checkpoint("checkpoints/c1.chk").unwrap();
        assert_eq!(last, 9);
        assert_eq!(state2.segments.len(), 1);
        assert_eq!(state2.segments[0].segment_id, 1);
        assert_eq!(state2.segments[0].doc_count, 3);
        assert_eq!(state2.segments[0].deleted_docs, vec![2, 0]);
    }

    fn read_all(dir: &Arc<dyn Directory>, path: &str) -> Vec<u8> {
        let mut f = dir.open_file(path).unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn checkpoint_rejects_bad_magic() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mgr = CheckpointManager::new(dir.clone());

        let state = CheckpointState { segments: vec![] };
        mgr.write_checkpoint(&state, 1, "checkpoints/c1.chk")
            .unwrap();

        let mut bytes = read_all(&dir, "checkpoints/c1.chk");
        bytes[0] ^= 0xFF;
        dir.atomic_write("checkpoints/c1.chk", &bytes).unwrap();

        let err = mgr.read_checkpoint("checkpoints/c1.chk").unwrap_err();
        assert!(err.to_string().contains("invalid checkpoint magic"));
    }

    #[test]
    fn checkpoint_rejects_bad_checksum() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mgr = CheckpointManager::new(dir.clone());

        let state = CheckpointState {
            segments: vec![CheckpointSegment {
                segment_id: 1,
                doc_count: 1,
                deleted_docs: vec![0],
            }],
        };
        mgr.write_checkpoint(&state, 1, "checkpoints/c1.chk")
            .unwrap();

        let mut bytes = read_all(&dir, "checkpoints/c1.chk");
        *bytes.last_mut().unwrap() ^= 0xFF;
        dir.atomic_write("checkpoints/c1.chk", &bytes).unwrap();

        let err = mgr.read_checkpoint("checkpoints/c1.chk").unwrap_err();
        assert!(err.to_string().contains("crc mismatch"));
    }

    #[test]
    fn checkpoint_truncation_is_error() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mgr = CheckpointManager::new(dir.clone());

        let state = CheckpointState { segments: vec![] };
        mgr.write_checkpoint(&state, 1, "checkpoints/c1.chk")
            .unwrap();

        let bytes = read_all(&dir, "checkpoints/c1.chk");
        let truncated = &bytes[..bytes.len().saturating_sub(1)];
        dir.atomic_write("checkpoints/c1.chk", truncated).unwrap();

        let err = mgr.read_checkpoint("checkpoints/c1.chk").unwrap_err();
        // Could be I/O or decode/format depending on where it truncates.
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }
}
