//! Property-based tests for the checkpoint publish lifecycle.
//!
//! Goal: stress the “what really matters” contract:
//! - checkpoint publish records a commit marker in the WAL
//! - truncation never breaks recoverability (at worst leaves extra WAL segments)
//! - recovery from checkpoint matches recovery from scratch after publish

use durability::checkpointing::{CheckpointSegment, CheckpointState};
use durability::publish::CheckpointPublisher;
use durability::recover::{RecoveredState, RecoveryManager};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalMaintenance, WalWriter};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum Op {
    AddSeg { seg: u64, doc_count: u32 },
    Del { seg: u64, doc: u32 },
}

fn apply_ops(ops: &[Op]) -> HashMap<u64, (u32, HashSet<u32>)> {
    let mut m: HashMap<u64, (u32, HashSet<u32>)> = HashMap::new();
    for op in ops {
        match *op {
            Op::AddSeg { seg, doc_count } => {
                m.insert(seg, (doc_count, HashSet::new()));
            }
            Op::Del { seg, doc } => {
                if let Some((_dc, dels)) = m.get_mut(&seg) {
                    dels.insert(doc);
                }
            }
        }
    }
    m
}

fn recovered_to_model(state: &RecoveredState) -> HashMap<u64, (u32, HashSet<u32>)> {
    let mut m: HashMap<u64, (u32, HashSet<u32>)> = HashMap::new();
    for s in &state.segments {
        m.insert(s.segment_id, (s.doc_count, s.deleted_docs.clone()));
    }
    m
}

fn arb_ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            (1u64..40, 0u32..500).prop_map(|(seg, dc)| Op::AddSeg { seg, doc_count: dc }),
            (1u64..40, 0u32..500).prop_map(|(seg, doc)| Op::Del { seg, doc }),
        ],
        1..250,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 96,
        .. ProptestConfig::default()
    })]

    #[test]
    fn publish_then_truncate_preserves_recoverability(
        ops in arb_ops(),
        split in 0usize..250
    ) {
        let split = split.min(ops.len());

        let tmp = tempfile::tempdir().unwrap();
        let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

        // Write the full op stream to WAL.
        let mut wal = WalWriter::new(dir.clone());
        let mut last_id: u64 = 0;
        for op in &ops {
            match *op {
                Op::AddSeg { seg, doc_count } => {
                    last_id = wal.append(WalEntry::AddSegment { entry_id: 0, segment_id: seg, doc_count }).unwrap();
                }
                Op::Del { seg, doc } => {
                    last_id = wal.append(WalEntry::DeleteDocuments { entry_id: 0, deletes: vec![(seg, doc)] }).unwrap();
                }
            }
        }
        wal.flush_and_sync().unwrap();

        // Create a checkpoint state corresponding to the prefix.
        let prefix_model = apply_ops(&ops[..split]);
        let ckpt_state = CheckpointState {
            segments: prefix_model
                .iter()
                .map(|(&seg, &(dc, ref dels))| CheckpointSegment {
                    segment_id: seg,
                    doc_count: dc,
                    deleted_docs: dels.iter().copied().collect(),
                })
                .collect(),
        };
        let ckpt_last_entry_id = split as u64; // WALWriter assigns ids 1..n
        let ckpt_path = "checkpoints/prop.chk";

        // Publish checkpoint and truncate.
        let pubr = CheckpointPublisher::new(dir.clone())
            .publish_checkpoint(&mut wal, &ckpt_state, ckpt_last_entry_id, ckpt_path)
            .unwrap();

        // A marker must exist in WAL, and truncation must not remove segments incorrectly.
        prop_assert!(pubr.wal_checkpoint_entry_id > last_id);

        // Recovery from checkpoint should equal recovery from scratch after publish.
        let mgr = RecoveryManager::new(dir.clone());
        let from_ckpt = mgr.recover(Some(ckpt_path)).unwrap();
        let from_scratch = mgr.recover(None).unwrap();
        prop_assert_eq!(recovered_to_model(&from_ckpt), recovered_to_model(&from_scratch));

        // And both should match the full reference model.
        let want = apply_ops(&ops);
        prop_assert_eq!(recovered_to_model(&from_scratch), want);

        // Sanity: WAL ranges must be strictly decodable after publish.
        let ranges = WalMaintenance::new(dir.clone()).segment_ranges_strict().unwrap();
        prop_assert!(!ranges.is_empty());
    }
}
