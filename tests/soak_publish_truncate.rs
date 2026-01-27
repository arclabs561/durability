//! Bounded “soak” test: repeated publish + truncation cycles preserve correctness.
//!
//! This is not an unbounded stress test. It’s a deterministic, medium-sized run that
//! catches lifecycle bugs: rotation, truncation, and recovery drift.

use durability::checkpointing::{CheckpointSegment, CheckpointState};
use durability::publish::CheckpointPublisher;
use durability::recover::RecoveryManager;
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalMaintenance, WalWriter};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

fn apply_entry(model: &mut HashMap<u64, (u32, HashSet<u32>)>, e: &WalEntry) {
    match e {
        WalEntry::AddSegment {
            segment_id,
            doc_count,
            ..
        } => {
            model.insert(*segment_id, (*doc_count, HashSet::new()));
        }
        WalEntry::DeleteDocuments { deletes, .. } => {
            for (seg, doc) in deletes {
                if let Some((_dc, dels)) = model.get_mut(seg) {
                    dels.insert(*doc);
                }
            }
        }
        WalEntry::EndMerge { .. }
        | WalEntry::StartMerge { .. }
        | WalEntry::CancelMerge { .. }
        | WalEntry::Checkpoint { .. } => {}
    }
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

fn recovered_to_model(
    rec: &durability::recover::RecoveredState,
) -> HashMap<u64, (u32, HashSet<u32>)> {
    let mut m = HashMap::new();
    for s in &rec.segments {
        m.insert(s.segment_id, (s.doc_count, s.deleted_docs.clone()));
    }
    m
}

#[test]
fn soak_publish_truncate_cycle() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut wal = WalWriter::new(dir.clone());
    wal.set_segment_size_limit_bytes(1024); // force multi-segment behavior for truncation

    let mut model: HashMap<u64, (u32, HashSet<u32>)> = HashMap::new();
    #[allow(unused_assignments)]
    let mut last_id: u64 = 0;
    let mut ckpt_gen: u64 = 0;
    let mut last_ckpt_path: Option<String> = None;

    // Deterministic sequence of operations (no RNG), but with enough variety.
    for step in 0..400u64 {
        let seg = (step % 30) + 1;
        if step % 7 == 0 {
            let e = WalEntry::AddSegment {
                entry_id: 0,
                segment_id: seg,
                doc_count: (step as u32) % 500,
            };
            last_id = wal.append(e.clone()).unwrap();
            apply_entry(&mut model, &e);
        } else {
            let e = WalEntry::DeleteDocuments {
                entry_id: 0,
                deletes: vec![(seg, (step as u32) % 200)],
            };
            last_id = wal.append(e.clone()).unwrap();
            apply_entry(&mut model, &e);
        }

        // Publish a checkpoint every 50 steps.
        if step > 0 && step % 50 == 0 {
            ckpt_gen += 1;
            let ckpt_state = model_to_ckpt(&model);
            let ckpt_path = format!("checkpoints/soak_{ckpt_gen}.chk");
            let pubr = CheckpointPublisher::new(dir.clone())
                .publish_checkpoint(&mut wal, &ckpt_state, last_id, &ckpt_path)
                .unwrap();
            last_ckpt_path = Some(pubr.checkpoint_path);

            // After publish+truncate, recovery must use the latest checkpoint marker.
            let mgr = RecoveryManager::new(dir.clone());
            let rec = mgr.recover_latest().unwrap();
            assert_eq!(recovered_to_model(&rec), model);
        }
    }

    // Ensure any buffered tail entries are visible to readers before final recovery checks.
    wal.flush().unwrap();

    // Final sanity: recovery from latest checkpoint (if any) matches scratch and model.
    let mgr = RecoveryManager::new(dir.clone());
    let from_scratch = mgr.recover_latest().unwrap();
    assert_eq!(recovered_to_model(&from_scratch), model);
    if let Some(p) = last_ckpt_path.as_deref() {
        let from_ckpt = mgr.recover(Some(p)).unwrap();
        assert_eq!(recovered_to_model(&from_ckpt), model);
    }

    // WAL should not grow unbounded: after truncation cycles we should have a small number of segments.
    let segs = WalMaintenance::new(dir).segment_ranges_strict().unwrap();
    assert!(segs.len() <= 40, "too many wal segments: {}", segs.len());
}
