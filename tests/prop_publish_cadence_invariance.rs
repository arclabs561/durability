//! Metamorphic property: checkpoint cadence must not change the final recovered state.
//!
//! We run the same operation stream twice:
//! - cadence A: checkpoint every `k_a` ops
//! - cadence B: checkpoint every `k_b` ops
//!
//! In both cases, we publish+truncate and then assert `recover_latest()` matches
//! the reference model.

use durability::checkpointing::{CheckpointSegment, CheckpointState};
use durability::publish::CheckpointPublisher;
use durability::recover::RecoveryManager;
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

fn recovered_to_model(
    rec: &durability::recover::RecoveredState,
) -> HashMap<u64, (u32, HashSet<u32>)> {
    let mut m: HashMap<u64, (u32, HashSet<u32>)> = HashMap::new();
    for s in &rec.segments {
        m.insert(s.segment_id, (s.doc_count, s.deleted_docs.clone()));
    }
    m
}

fn model_to_ckpt(model: &HashMap<u64, (u32, HashSet<u32>)>) -> CheckpointState {
    let mut segments: Vec<CheckpointSegment> = model
        .iter()
        .map(|(&seg, &(dc, ref dels))| CheckpointSegment {
            segment_id: seg,
            doc_count: dc,
            deleted_docs: dels.iter().copied().collect(),
        })
        .collect();
    segments.sort_by_key(|s| s.segment_id);
    CheckpointState { segments }
}

fn arb_ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            (1u64..50, 0u32..500).prop_map(|(seg, dc)| Op::AddSeg { seg, doc_count: dc }),
            (1u64..50, 0u32..500).prop_map(|(seg, doc)| Op::Del { seg, doc }),
        ],
        1..300,
    )
}

fn run_with_cadence(ops: &[Op], cadence: usize) -> HashMap<u64, (u32, HashSet<u32>)> {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut wal = WalWriter::new(dir.clone());
    wal.set_segment_size_limit_bytes(1024); // encourage multi-segment behavior

    let mut model: HashMap<u64, (u32, HashSet<u32>)> = HashMap::new();
    #[allow(unused_assignments)]
    let mut last_id: u64 = 0;
    let mut ckpt_gen: u64 = 0;

    for (i, op) in ops.iter().enumerate() {
        match *op {
            Op::AddSeg { seg, doc_count } => {
                let e = WalEntry::AddSegment {
                    entry_id: 0,
                    segment_id: seg,
                    doc_count,
                };
                last_id = wal.append(e.clone()).unwrap();
                // apply to model
                model.insert(seg, (doc_count, HashSet::new()));
            }
            Op::Del { seg, doc } => {
                let e = WalEntry::DeleteDocuments {
                    entry_id: 0,
                    deletes: vec![(seg, doc)],
                };
                last_id = wal.append(e.clone()).unwrap();
                if let Some((_dc, dels)) = model.get_mut(&seg) {
                    dels.insert(doc);
                }
            }
        }

        if cadence > 0 && (i + 1) % cadence == 0 {
            ckpt_gen += 1;
            let ckpt_state = model_to_ckpt(&model);
            let ckpt_path = format!("checkpoints/cadence_{cadence}_{ckpt_gen}.chk");
            let _ = CheckpointPublisher::new(dir.clone())
                .publish_checkpoint(&mut wal, &ckpt_state, last_id, &ckpt_path)
                .unwrap();
        }
    }

    // Ensure buffered tail entries are visible before the final recovery.
    wal.flush().unwrap();

    let mgr = RecoveryManager::new(dir.clone());
    let rec = mgr.recover_latest_best_effort().unwrap();
    recovered_to_model(&rec)
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 96,
        .. ProptestConfig::default()
    })]

    #[test]
    fn checkpoint_cadence_does_not_change_final_state(ops in arb_ops(),
                                                     k_a in 1usize..40,
                                                     k_b in 1usize..40) {
        let want = apply_ops(&ops);
        let got_a = run_with_cadence(&ops, k_a);
        let got_b = run_with_cadence(&ops, k_b);

        prop_assert_eq!(&got_a, &want);
        prop_assert_eq!(&got_b, &want);
        prop_assert_eq!(got_a, got_b);
    }
}
