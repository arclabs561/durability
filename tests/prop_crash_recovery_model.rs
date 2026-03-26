//! Model-based property test for WAL crash recovery.
//!
//! Strategy (inspired by sled's quickcheck model testing):
//! 1. Generate a random sequence of WAL operations (append, flush, drop+resume).
//! 2. Track a reference model of what should be durable after each flush.
//! 3. At each "crash point" (drop without flush), verify that recovery
//!    returns a prefix of the flushed entries.

use durability::storage::MemoryDirectory;
use durability::walog::{WalEntry, WalReader, WalWriter};
use proptest::prelude::*;

/// Operations that can be applied to the WAL.
#[derive(Debug, Clone)]
enum Op {
    /// Append an entry (the u64 is the segment_id for AddSegment).
    Append(u64),
    /// Flush the writer.
    Flush,
    /// Simulate crash: drop writer without flushing, then resume.
    CrashAndResume,
}

fn arb_ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            8 => (1..1000u64).prop_map(Op::Append),
            3 => Just(Op::Flush),
            1 => Just(Op::CrashAndResume),
        ],
        1..40,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn crash_recovery_preserves_flushed_prefix(ops in arb_ops()) {
        let dir = MemoryDirectory::arc();

        // Model: entries that have been flushed (guaranteed durable).
        let mut model_flushed: Vec<u64> = Vec::new(); // entry_ids
        // Pending: entries appended but not yet flushed.
        let mut pending_ids: Vec<u64> = Vec::new();

        let mut writer: Option<WalWriter<WalEntry>> = Some(WalWriter::new(dir.clone()));

        for op in &ops {
            match op {
                Op::Append(seg_id) => {
                    if let Some(ref mut w) = writer {
                        if let Ok(id) = w.append(&WalEntry::AddSegment {
                            segment_id: *seg_id,
                            doc_count: 0,
                        }) {
                            pending_ids.push(id);
                        }
                    }
                }
                Op::Flush => {
                    if let Some(ref mut w) = writer {
                        if w.flush().is_ok() {
                            // After flush, pending entries become durable.
                            model_flushed.append(&mut pending_ids);
                        }
                    }
                }
                Op::CrashAndResume => {
                    // Drop writer (simulates crash -- unflushed entries may be lost).
                    drop(writer.take());
                    pending_ids.clear();

                    // Verify: recovery must return exactly the flushed entries.
                    let reader = WalReader::<WalEntry>::new(dir.clone());
                    let recovered = reader.replay_best_effort().unwrap();

                    // Recovered entries must be a prefix of model_flushed
                    // (in MemoryDirectory, writes are immediately visible, so
                    // we may actually recover more than "flushed" -- but we must
                    // never see entries that weren't at least appended).
                    prop_assert!(
                        recovered.len() >= model_flushed.len(),
                        "recovered {} entries but expected at least {} flushed",
                        recovered.len(),
                        model_flushed.len()
                    );

                    // The first N entries must match the model.
                    for (idx, flushed_id) in model_flushed.iter().enumerate() {
                        prop_assert_eq!(
                            recovered[idx].entry_id, *flushed_id
                        );
                    }

                    // Entry IDs in recovered must be strictly increasing.
                    for w in recovered.windows(2) {
                        prop_assert!(
                            w[1].entry_id > w[0].entry_id,
                            "entry_id not increasing: {} >= {}",
                            w[1].entry_id,
                            w[0].entry_id,
                        );
                    }

                    // Update model to include any extra recovered entries
                    // (MemoryDirectory makes unflushed writes visible).
                    model_flushed = recovered.iter().map(|r| r.entry_id).collect();

                    // Resume the writer.
                    writer = Some(WalWriter::resume(dir.clone()).unwrap());
                }
            }
        }

        // Final verification: drop and read.
        drop(writer);
        let reader = WalReader::<WalEntry>::new(dir);
        let final_recovered = reader.replay_best_effort().unwrap();

        // Must contain at least the flushed entries.
        prop_assert!(
            final_recovered.len() >= model_flushed.len(),
            "final: recovered {} but expected at least {}",
            final_recovered.len(),
            model_flushed.len()
        );

        // Entry IDs strictly increasing.
        for w in final_recovered.windows(2) {
            prop_assert!(
                w[1].entry_id > w[0].entry_id,
                "final: entry_id not increasing",
            );
        }
    }
}
