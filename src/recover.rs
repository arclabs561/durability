//! Crash recovery using checkpoint + WAL replay.

use crate::checkpointing::{CheckpointManager, CheckpointSegment, CheckpointState};
use crate::error::PersistenceResult;
use crate::storage::Directory;
use crate::walog::{WalEntry, WalReader};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// State recovered from checkpoint + WAL.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecoveredState {
    /// Active segments in the recovered view.
    pub segments: Vec<RecoveredSegment>,
    /// Last entry id applied during recovery.
    pub last_entry_id: u64,
}

/// Per-segment recovered state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    pub fn recover_latest(&self) -> PersistenceResult<RecoveredState> {
        let ckpt = self.latest_checkpoint_from_wal(/*best_effort_wal=*/ false)?;
        self.recover(ckpt.as_deref())
    }

    /// Best-effort variant of [`RecoveryManager::recover_latest`].
    pub fn recover_latest_best_effort(&self) -> PersistenceResult<RecoveredState> {
        let ckpt = self.latest_checkpoint_from_wal(/*best_effort_wal=*/ true)?;
        self.recover_best_effort(ckpt.as_deref())
    }

    fn latest_checkpoint_from_wal(
        &self,
        best_effort_wal: bool,
    ) -> PersistenceResult<Option<String>> {
        let wal = WalReader::<WalEntry>::new(self.directory.clone());
        let records = if best_effort_wal {
            wal.replay_best_effort()?
        } else {
            wal.replay()?
        };
        let mut best: Option<(u64, String)> = None;
        for r in records {
            if let WalEntry::Checkpoint {
                checkpoint_path, ..
            } = r.payload
            {
                let entry_id = r.entry_id;
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

        let wal = WalReader::<WalEntry>::new(self.directory.clone());
        let records = match mode {
            RecoveryMode::Strict => wal.replay()?,
            RecoveryMode::BestEffort => wal.replay_best_effort()?,
        };

        for record in records {
            let entry_id = record.entry_id;
            if entry_id <= last_entry_id {
                continue;
            }
            last_entry_id = entry_id;

            match record.payload {
                WalEntry::AddSegment {
                    segment_id,
                    doc_count,
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
                WalEntry::DeleteDocuments { deletes } => {
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
                deleted_docs: {
                    let mut v: Vec<u32> = s.deleted_docs.iter().copied().collect();
                    v.sort_unstable();
                    v
                },
            })
            .collect();
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

        let mut wal = WalWriter::<WalEntry>::new(dir.clone());
        wal.append(&WalEntry::DeleteDocuments {
            deletes: vec![(7, 2)],
        })
        .unwrap();
        wal.append(&WalEntry::AddSegment {
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
