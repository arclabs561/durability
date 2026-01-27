//! End-to-end property tests for checkpoint + WAL recovery.

use durability::checkpointing::{CheckpointManager, CheckpointSegment, CheckpointState};
use durability::recover::{RecoveredState, RecoveryManager};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalWriter};
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
    // Keep numbers small so we get collisions (deletes for missing segments),
    // exercising “ignore delete for unknown segment”.
    prop::collection::vec(
        prop_oneof![
            (1u64..20, 0u32..200).prop_map(|(seg, dc)| Op::AddSeg { seg, doc_count: dc }),
            (1u64..20, 0u32..200).prop_map(|(seg, doc)| Op::Del { seg, doc }),
        ],
        0..200,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    // E2E: checkpoint in the middle, then WAL suffix, recover == reference model.
    #[test]
    fn e2e_checkpoint_then_wal_recovery_matches_reference(
        ops in arb_ops(),
        split in 0usize..200
    ) {
        let split = split.min(ops.len());

        let tmp = tempfile::tempdir().unwrap();
        let dir = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);

        let ckpt_mgr = CheckpointManager::new(dir.clone());

        // Append prefix ops into WAL; record the last entry id applied so the checkpoint
        // can correctly skip replaying the prefix.
        let mut wal = WalWriter::new(dir.clone());
        let mut last_prefix_entry_id: u64 = 0;
        for op in &ops[..split] {
            match *op {
                Op::AddSeg { seg, doc_count } => {
                    last_prefix_entry_id = wal.append(WalEntry::AddSegment { entry_id: 0, segment_id: seg, doc_count }).unwrap();
                }
                Op::Del { seg, doc } => {
                    last_prefix_entry_id = wal.append(WalEntry::DeleteDocuments { entry_id: 0, deletes: vec![(seg, doc)] }).unwrap();
                }
            }
        }

        // Write checkpoint at the split point.
        let before_model = apply_ops(&ops[..split]);
        let ckpt_state = CheckpointState {
            segments: before_model
                .iter()
                .map(|(&seg, &(dc, ref dels))| CheckpointSegment {
                    segment_id: seg,
                    doc_count: dc,
                    deleted_docs: dels.iter().copied().collect(),
                })
                .collect(),
        };
        let checkpoint_path = "checkpoints/c1.chk";
        ckpt_mgr
            .write_checkpoint(&ckpt_state, last_prefix_entry_id, checkpoint_path)
            .unwrap();

        // Append suffix ops (post-checkpoint) into WAL.
        for op in &ops[split..] {
            match *op {
                Op::AddSeg { seg, doc_count } => {
                    wal.append(WalEntry::AddSegment { entry_id: 0, segment_id: seg, doc_count }).unwrap();
                }
                Op::Del { seg, doc } => {
                    wal.append(WalEntry::DeleteDocuments { entry_id: 0, deletes: vec![(seg, doc)] }).unwrap();
                }
            }
        }

        wal.flush().unwrap();

        let rec = RecoveryManager::new(dir.clone()).recover(Some(checkpoint_path)).unwrap();
        let got = recovered_to_model(&rec);
        let want = apply_ops(&ops);

        prop_assert_eq!(got, want);
    }
}
