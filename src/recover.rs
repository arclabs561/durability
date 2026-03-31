//! Crash recovery using checkpoint + WAL replay.
//!
//! Two levels of API:
//!
//! - **Generic**: [`recover_with_wal`] coordinates checkpoint loading and WAL replay for
//!   any entry type `E` and checkpoint state `C`. Callers provide an `init` closure
//!   (to convert checkpoint state into working state) and an `apply` closure (to fold
//!   each WAL entry).
//!
//! - **Segment-specific**: [`RecoveryManager`] is a concrete implementation for
//!   segment-index WALs using [`WalEntry`].

use crate::checkpoint::CheckpointFile;
use crate::error::PersistenceResult;
use crate::storage::Directory;
use crate::walog::{WalEntry, WalReader, WalReplayMode};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Generic recovery
// ---------------------------------------------------------------------------

/// Options controlling recovery behavior.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryOptions {
    /// How to handle WAL tail corruption.
    pub wal_mode: WalReplayMode,
    /// If true, treat an unreadable/corrupt checkpoint as empty state
    /// rather than returning an error.
    pub ignore_corrupt_checkpoint: bool,
    /// If set, stop replaying WAL entries after this entry ID.
    ///
    /// Enables point-in-time recovery: replay only entries up to (and including)
    /// the specified ID. Entries with higher IDs are ignored.
    pub up_to_entry_id: Option<u64>,
}

impl RecoveryOptions {
    /// Strict: any corruption is an error, replay all entries.
    pub fn strict() -> Self {
        Self {
            wal_mode: WalReplayMode::Strict,
            ignore_corrupt_checkpoint: false,
            up_to_entry_id: None,
        }
    }

    /// Best-effort: tolerate torn WAL tail and corrupt checkpoints.
    pub fn best_effort() -> Self {
        Self {
            wal_mode: WalReplayMode::BestEffortTail,
            ignore_corrupt_checkpoint: true,
            up_to_entry_id: None,
        }
    }

    /// Point-in-time: replay entries up to (and including) `entry_id`.
    pub fn up_to(entry_id: u64) -> Self {
        Self {
            wal_mode: WalReplayMode::Strict,
            ignore_corrupt_checkpoint: false,
            up_to_entry_id: Some(entry_id),
        }
    }
}

/// Result of a generic recovery operation.
#[derive(Debug, Clone)]
pub struct Recovery<S> {
    /// The recovered working state.
    pub state: S,
    /// The last WAL entry ID applied during recovery (0 if no entries).
    pub last_entry_id: u64,
}

/// Recover state from an optional checkpoint file and WAL replay.
///
/// Generic over:
/// - `C`: the checkpoint state type (deserialized from the checkpoint file).
/// - `E`: the WAL entry type.
/// - `W`: the working state built during recovery.
///
/// The `init` closure receives the deserialized checkpoint (or `None` if no
/// checkpoint exists) and returns the initial working state.
///
/// The `apply` closure folds each WAL entry (with entry_id > checkpoint's
/// `last_applied_id`) into the working state.
///
/// # Example
///
/// ```
/// use durability::recover::{recover_with_wal, RecoveryOptions};
/// use durability::walog::WalWriter;
/// use durability::storage::MemoryDirectory;
///
/// #[derive(Default, serde::Serialize, serde::Deserialize)]
/// struct Snap { counter: u64 }
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// enum Op { Inc, Dec }
///
/// let dir = MemoryDirectory::arc();
///
/// // Write some entries.
/// let mut w = WalWriter::<Op>::new(dir.clone());
/// w.append(&Op::Inc).unwrap();
/// w.append(&Op::Inc).unwrap();
/// w.append(&Op::Dec).unwrap();
/// w.flush().unwrap();
/// drop(w);
///
/// // Recover state by replaying the WAL.
/// let result = recover_with_wal::<Snap, Op, _>(
///     &dir,
///     None,
///     RecoveryOptions::strict(),
///     |ckpt| ckpt.unwrap_or_default().counter,
///     |counter, _entry_id, entry| {
///         match entry {
///             Op::Inc => *counter += 1,
///             Op::Dec => *counter = counter.saturating_sub(1),
///         }
///     },
/// ).unwrap();
/// assert_eq!(result.state, 1); // 0 + 1 + 1 - 1
/// assert_eq!(result.last_entry_id, 3);
/// ```
pub fn recover_with_wal<C, E, W>(
    dir: &Arc<dyn Directory>,
    checkpoint_path: Option<&str>,
    options: RecoveryOptions,
    init: impl FnOnce(Option<C>) -> W,
    mut apply: impl FnMut(&mut W, u64, E),
) -> PersistenceResult<Recovery<W>>
where
    C: serde::de::DeserializeOwned,
    E: serde::de::DeserializeOwned,
{
    // Step 1: load checkpoint (if any).
    let (mut state, mut last_entry_id) = match checkpoint_path {
        Some(path) if dir.exists(path) => {
            let ckpt = CheckpointFile::new(dir.clone());
            match ckpt.read_postcard::<C>(path) {
                Ok((last_id, checkpoint_state)) => (init(Some(checkpoint_state)), last_id),
                Err(e) if options.ignore_corrupt_checkpoint => {
                    // Best-effort: skip corrupt checkpoint, start from empty.
                    let _ = e; // consumed
                    (init(None), 0u64)
                }
                Err(e) => return Err(e),
            }
        }
        _ => (init(None), 0u64),
    };

    // Step 2: replay WAL entries with entry_id > last_entry_id.
    let wal = WalReader::<E>::new(dir.clone());
    let since = last_entry_id;
    let ceiling = options.up_to_entry_id;

    // Use a sentinel error to break out of replay_each when past the ceiling.
    // This avoids decoding entries we'll never apply.
    let result = wal.replay_each_with_mode(options.wal_mode, |record| {
        if record.entry_id > since {
            if let Some(max) = ceiling {
                if record.entry_id > max {
                    return Err(crate::error::PersistenceError::InvalidState(
                        "@@early_stop@@".into(),
                    ));
                }
            }
            last_entry_id = record.entry_id;
            apply(&mut state, record.entry_id, record.payload);
        }
        Ok(())
    });
    match result {
        Ok(_) => {}
        Err(crate::error::PersistenceError::InvalidState(ref msg)) if msg == "@@early_stop@@" => {}
        Err(e) => return Err(e),
    }

    Ok(Recovery {
        state,
        last_entry_id,
    })
}

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

/// State recovered from checkpoint + WAL.
#[derive(Debug, Clone)]
pub struct RecoveredState {
    /// Active segments in the recovered view.
    pub segments: Vec<RecoveredSegment>,
    /// Last entry id applied during recovery.
    pub last_entry_id: u64,
}

/// Per-segment recovered state.
#[derive(Debug, Clone)]
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
        self.recover_with_options(checkpoint_path, RecoveryOptions::strict())
    }

    /// Best-effort recovery: if the checkpoint exists but is unreadable/corrupt, ignore it.
    pub fn recover_best_effort(
        &self,
        checkpoint_path: Option<&str>,
    ) -> PersistenceResult<RecoveredState> {
        self.recover_with_options(checkpoint_path, RecoveryOptions::best_effort())
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

    fn recover_with_options(
        &self,
        checkpoint_path: Option<&str>,
        options: RecoveryOptions,
    ) -> PersistenceResult<RecoveredState> {
        let result = recover_with_wal::<CheckpointState, WalEntry, _>(
            &self.directory,
            checkpoint_path,
            options,
            |ckpt| {
                let mut map: HashMap<u64, RecoveredSegment> = HashMap::new();
                if let Some(state) = ckpt {
                    for s in state.segments {
                        map.insert(
                            s.segment_id,
                            RecoveredSegment {
                                segment_id: s.segment_id,
                                doc_count: s.doc_count,
                                deleted_docs: s.deleted_docs.into_iter().collect(),
                            },
                        );
                    }
                }
                map
            },
            |map, _entry_id, entry| {
                Self::apply_entry(map, entry);
            },
        )?;

        let mut segments: Vec<RecoveredSegment> = result.state.into_values().collect();
        segments.sort_by_key(|s| s.segment_id);

        Ok(RecoveredState {
            segments,
            last_entry_id: result.last_entry_id,
        })
    }

    /// Apply a single WAL entry to the segment map.
    fn apply_entry(map: &mut HashMap<u64, RecoveredSegment>, entry: WalEntry) {
        match entry {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryDirectory;
    use crate::walog::{WalEntry, WalWriter};
    use std::io::Read;

    #[test]
    fn recovery_applies_checkpoint_then_wal() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        let ckpt = CheckpointFile::new(dir.clone());
        let state = CheckpointState {
            segments: vec![CheckpointSegment {
                segment_id: 7,
                doc_count: 3,
                deleted_docs: vec![0],
            }],
        };
        ckpt.write_postcard("checkpoints/c1.chk", 0, &state)
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

        let ckpt = CheckpointFile::new(dir.clone());
        let state = CheckpointState { segments: vec![] };
        ckpt.write_postcard("checkpoints/c1.chk", 0, &state)
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
        let ckpt = CheckpointFile::new(dir.clone());
        ckpt.write_postcard("checkpoints/a.chk", 123, &c1).unwrap();
        ckpt.write_postcard("checkpoints/b.chk", 123, &c2).unwrap();

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
