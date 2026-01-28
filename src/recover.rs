//! Crash recovery using checkpoint + WAL replay.

use crate::checkpointing::{CheckpointManager, CheckpointSegment, CheckpointState};
use crate::error::PersistenceResult;
use crate::storage::Directory;
use crate::walog::{WalEntry, WalReader};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone)]
/// State recovered from checkpoint + WAL.
pub struct RecoveredState {
    /// Active segments in the recovered view.
    pub segments: Vec<RecoveredSegment>,
    /// Last entry id applied during recovery.
    pub last_entry_id: u64,
}

#[derive(Debug, Clone)]
/// Per-segment recovered state.
pub struct RecoveredSegment {
    /// Segment identifier.
    pub segment_id: u64,
    /// Segment document count (0 if unknown).
    pub doc_count: u32,
    /// Deleted doc ids within the segment.
    pub deleted_docs: HashSet<u32>,
}

/// Performs recovery by applying checkpoint + WAL.
pub struct RecoveryManager {
    directory: Arc<dyn Directory>,
}

impl RecoveryManager {
    /// Create a recovery manager for a directory backend.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// Recover state using an optional checkpoint, then WAL replay.
    ///
    /// If `checkpoint_path` is provided:
    /// - missing checkpoint file is treated as “no checkpoint”
    /// - *present but unreadable/corrupt* checkpoint is an error (strict)
    pub fn recover(&self, checkpoint_path: Option<&str>) -> PersistenceResult<RecoveredState> {
        self.recover_with_mode(checkpoint_path, RecoveryMode::Strict)
    }

    /// Best-effort recovery: if the checkpoint exists but is unreadable/corrupt, ignore it.
    pub fn recover_best_effort(
        &self,
        checkpoint_path: Option<&str>,
    ) -> PersistenceResult<RecoveredState> {
        self.recover_with_mode(checkpoint_path, RecoveryMode::BestEffort)
    }

    /// Recover state using the latest committed checkpoint recorded in the WAL (if any).
    ///
    /// This is the “post-truncation” recovery entrypoint:
    /// after WAL prefix truncation, `recover(None)` no longer has enough information to
    /// reconstruct full state. The system must start from a checkpoint.
    ///
    /// Behavior:
    /// - If the WAL contains no `WalEntry::Checkpoint`, this is equivalent to `recover(None)`.
    /// - If a checkpoint marker exists but the checkpoint file is missing/corrupt, this errors.
    pub fn recover_latest(&self) -> PersistenceResult<RecoveredState> {
        let ckpt = self.latest_checkpoint_from_wal(/*best_effort_wal=*/ false)?;
        self.recover(ckpt.as_deref())
    }

    /// Best-effort variant of [`RecoveryManager::recover_latest`].
    ///
    /// - WAL is scanned in best-effort mode.
    /// - If the latest checkpoint marker points to a missing/corrupt checkpoint, it is ignored
    ///   and we fall back to `recover(None)`.
    pub fn recover_latest_best_effort(&self) -> PersistenceResult<RecoveredState> {
        let ckpt = self.latest_checkpoint_from_wal(/*best_effort_wal=*/ true)?;
        self.recover_best_effort(ckpt.as_deref())
    }

    fn latest_checkpoint_from_wal(
        &self,
        best_effort_wal: bool,
    ) -> PersistenceResult<Option<String>> {
        let wal = WalReader::new(self.directory.clone());
        let entries = if best_effort_wal {
            wal.replay_best_effort()?
        } else {
            wal.replay()?
        };
        let mut best: Option<(u64, String)> = None;
        for e in entries {
            if let WalEntry::Checkpoint {
                entry_id,
                checkpoint_path,
                ..
            } = e
            {
                match &best {
                    None => best = Some((entry_id, checkpoint_path)),
                    Some((prev_id, _)) if entry_id > *prev_id => {
                        best = Some((entry_id, checkpoint_path))
                    }
                    _ => {}
                }
            }
        }
        Ok(best.map(|(_, p)| p))
    }

    fn recover_with_mode(
        &self,
        checkpoint_path: Option<&str>,
        mode: RecoveryMode,
    ) -> PersistenceResult<RecoveredState> {
        let (mut segments, mut last_entry_id) = if let Some(path) = checkpoint_path {
            if !self.directory.exists(path) {
                (Vec::new(), 0)
            } else {
                let ckpt = CheckpointManager::new(self.directory.clone());
                match ckpt.read_checkpoint(path) {
                    Ok((state, last)) => (state.segments, last),
                    Err(e) => match mode {
                        RecoveryMode::Strict => return Err(e),
                        RecoveryMode::BestEffort => (Vec::new(), 0),
                    },
                }
            }
        } else {
            (Vec::new(), 0)
        };

        let mut map: HashMap<u64, RecoveredSegment> = HashMap::new();
        for s in segments.drain(..) {
            map.insert(
                s.segment_id,
                RecoveredSegment {
                    segment_id: s.segment_id,
                    doc_count: s.doc_count,
                    deleted_docs: s.deleted_docs.into_iter().collect(),
                },
            );
        }

        let wal = WalReader::new(self.directory.clone());
        let entries = match mode {
            RecoveryMode::Strict => wal.replay()?,
            RecoveryMode::BestEffort => wal.replay_best_effort()?,
        };
        for entry in entries {
            let entry_id = match &entry {
                WalEntry::AddSegment { entry_id, .. }
                | WalEntry::StartMerge { entry_id, .. }
                | WalEntry::CancelMerge { entry_id, .. }
                | WalEntry::EndMerge { entry_id, .. }
                | WalEntry::DeleteDocuments { entry_id, .. }
                | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
            };
            if entry_id <= last_entry_id {
                continue;
            }
            last_entry_id = entry_id;

            match entry {
                WalEntry::AddSegment {
                    segment_id,
                    doc_count,
                    ..
                } => {
                    map.insert(
                        segment_id,
                        RecoveredSegment {
                            segment_id,
                            doc_count,
                            deleted_docs: HashSet::new(),
                        },
                    );
                }
                WalEntry::DeleteDocuments { deletes, .. } => {
                    for (segment_id, doc_id) in deletes {
                        if let Some(seg) = map.get_mut(&segment_id) {
                            seg.deleted_docs.insert(doc_id);
                        }
                    }
                }
                WalEntry::EndMerge {
                    new_segment_id,
                    old_segment_ids,
                    remapped_deletes,
                    ..
                } => {
                    for old in old_segment_ids {
                        map.remove(&old);
                    }
                    let mut new_seg = RecoveredSegment {
                        segment_id: new_segment_id,
                        doc_count: 0,
                        deleted_docs: HashSet::new(),
                    };
                    for (seg_id, doc_id) in remapped_deletes {
                        if seg_id == new_segment_id {
                            new_seg.deleted_docs.insert(doc_id);
                        }
                    }
                    map.insert(new_segment_id, new_seg);
                }
                WalEntry::StartMerge { .. }
                | WalEntry::CancelMerge { .. }
                | WalEntry::Checkpoint { .. } => {}
            }
        }

        let mut segments: Vec<RecoveredSegment> = map.into_values().collect();
        segments.sort_by_key(|s| s.segment_id);

        Ok(RecoveredState {
            segments,
            last_entry_id,
        })
    }

    /// Convert a recovered state back into a checkpoint payload.
    pub fn to_checkpoint_state(state: &RecoveredState) -> CheckpointState {
        let mut segments: Vec<CheckpointSegment> = state
            .segments
            .iter()
            .map(|s| CheckpointSegment {
                segment_id: s.segment_id,
                doc_count: s.doc_count,
                // Determinism: `HashSet` iteration order is not stable, so sort for a
                // reproducible checkpoint encoding.
                deleted_docs: {
                    let mut v: Vec<u32> = s.deleted_docs.iter().copied().collect();
                    v.sort_unstable();
                    v
                },
            })
            .collect();
        // Determinism: keep segments ordered.
        segments.sort_by_key(|s| s.segment_id);
        CheckpointState { segments }
    }
}

#[derive(Debug, Clone, Copy)]
enum RecoveryMode {
    Strict,
    BestEffort,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryDirectory;
    use crate::walog::{WalEntry, WalWriter};
    use std::io::Read;

    #[test]
    fn recovery_applies_checkpoint_then_wal() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        // Initial checkpoint with segment 7 (one delete) at last_entry_id=0.
        let ckpt = CheckpointManager::new(dir.clone());
        let state = CheckpointState {
            segments: vec![CheckpointSegment {
                segment_id: 7,
                doc_count: 3,
                deleted_docs: vec![0],
            }],
        };
        ckpt.write_checkpoint(&state, 0, "checkpoints/c1.chk")
            .unwrap();

        // WAL entries after the checkpoint.
        let mut wal = WalWriter::new(dir.clone());
        wal.append(WalEntry::DeleteDocuments {
            entry_id: 0,
            deletes: vec![(7, 2)],
        })
        .unwrap();
        wal.append(WalEntry::AddSegment {
            entry_id: 0,
            segment_id: 9,
            doc_count: 5,
        })
        .unwrap();

        wal.flush().unwrap();

        let rec = RecoveryManager::new(dir)
            .recover(Some("checkpoints/c1.chk"))
            .unwrap();
        assert_eq!(rec.last_entry_id, 2);

        assert_eq!(rec.segments.len(), 2);
        assert_eq!(rec.segments[0].segment_id, 7);
        assert!(rec.segments[0].deleted_docs.contains(&0));
        assert!(rec.segments[0].deleted_docs.contains(&2));

        assert_eq!(rec.segments[1].segment_id, 9);
    }

    #[test]
    fn recover_strict_errors_on_corrupt_checkpoint() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        // Write a checkpoint, then corrupt it.
        let ckpt = CheckpointManager::new(dir.clone());
        let state = CheckpointState { segments: vec![] };
        ckpt.write_checkpoint(&state, 0, "checkpoints/c1.chk")
            .unwrap();

        let mut bytes = {
            let mut f = dir.open_file("checkpoints/c1.chk").unwrap();
            let mut buf = Vec::new();
            f.read_to_end(&mut buf).unwrap();
            buf
        };
        bytes[0] ^= 0xFF;
        dir.atomic_write("checkpoints/c1.chk", &bytes).unwrap();

        let err = RecoveryManager::new(dir.clone())
            .recover(Some("checkpoints/c1.chk"))
            .unwrap_err();
        assert!(err.to_string().contains("invalid checkpoint magic"));

        // Best-effort ignores the checkpoint and still succeeds (empty state).
        let ok = RecoveryManager::new(dir)
            .recover_best_effort(Some("checkpoints/c1.chk"))
            .unwrap();
        assert_eq!(ok.segments.len(), 0);
        assert_eq!(ok.last_entry_id, 0);
    }

    #[test]
    fn to_checkpoint_state_is_deterministic() {
        let mk_state = |delete_insert_order: &[u32], segment_order: &[u64]| {
            let mut segments = Vec::new();
            for &seg_id in segment_order {
                let mut dels = HashSet::new();
                for &d in delete_insert_order {
                    dels.insert(d);
                }
                segments.push(RecoveredSegment {
                    segment_id: seg_id,
                    doc_count: 10,
                    deleted_docs: dels,
                });
            }
            RecoveredState {
                segments,
                last_entry_id: 123,
            }
        };

        let s1 = mk_state(&[3, 1, 2], &[9, 7]);
        let s2 = mk_state(&[2, 3, 1], &[7, 9]);

        let c1 = RecoveryManager::to_checkpoint_state(&s1);
        let c2 = RecoveryManager::to_checkpoint_state(&s2);
        assert_eq!(c1.segments.len(), 2);
        assert_eq!(c2.segments.len(), 2);

        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let ckpt = CheckpointManager::new(dir.clone());
        ckpt.write_checkpoint(&c1, 123, "checkpoints/a.chk")
            .unwrap();
        ckpt.write_checkpoint(&c2, 123, "checkpoints/b.chk")
            .unwrap();

        let mut a = Vec::new();
        dir.open_file("checkpoints/a.chk")
            .unwrap()
            .read_to_end(&mut a)
            .unwrap();
        let mut b = Vec::new();
        dir.open_file("checkpoints/b.chk")
            .unwrap()
            .read_to_end(&mut b)
            .unwrap();
        assert_eq!(a, b);
    }
}
