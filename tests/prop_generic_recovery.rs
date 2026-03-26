//! Property tests for generic `recover_with_wal`.
//!
//! Validates the reuse-seam invariant: recovery produces the same final state
//! regardless of when checkpoints happen. Also validates point-in-time recovery
//! produces a valid prefix of the full recovery.

use durability::checkpoint::CheckpointFile;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::MemoryDirectory;
use durability::walog::WalWriter;
use proptest::prelude::*;
use std::collections::BTreeMap;

// -- Custom counter domain for property testing ------------------------------

/// Simple counter operations -- easy to verify final state.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum CounterOp {
    Add(i64),
    Set(i64),
}

/// Checkpoint just stores the counter value.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct CounterCheckpoint {
    value: i64,
}

fn counter_init(ckpt: Option<CounterCheckpoint>) -> i64 {
    ckpt.map(|c| c.value).unwrap_or(0)
}

fn counter_apply(state: &mut i64, _entry_id: u64, op: CounterOp) {
    match op {
        CounterOp::Add(n) => *state += n,
        CounterOp::Set(n) => *state = n,
    }
}

// -- Multi-key domain for richer property tests ------------------------------

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum KvOp {
    Put(String, i64),
    Delete(String),
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct KvCheckpoint {
    entries: Vec<(String, i64)>,
}

fn kv_init(ckpt: Option<KvCheckpoint>) -> BTreeMap<String, i64> {
    match ckpt {
        Some(c) => c.entries.into_iter().collect(),
        None => BTreeMap::new(),
    }
}

fn kv_apply(state: &mut BTreeMap<String, i64>, _entry_id: u64, op: KvOp) {
    match op {
        KvOp::Put(k, v) => {
            state.insert(k, v);
        }
        KvOp::Delete(k) => {
            state.remove(&k);
        }
    }
}

fn kv_to_checkpoint(state: &BTreeMap<String, i64>) -> KvCheckpoint {
    KvCheckpoint {
        entries: state.iter().map(|(k, v)| (k.clone(), *v)).collect(),
    }
}

// -- Strategies --------------------------------------------------------------

fn counter_op_strategy() -> impl Strategy<Value = CounterOp> {
    prop_oneof![
        (-100i64..=100i64).prop_map(CounterOp::Add),
        (-1000i64..=1000i64).prop_map(CounterOp::Set),
    ]
}

fn kv_op_strategy() -> impl Strategy<Value = KvOp> {
    let key = prop_oneof![
        Just("a".to_string()),
        Just("b".to_string()),
        Just("c".to_string()),
        Just("d".to_string()),
    ];
    prop_oneof![
        (key.clone(), -100i64..=100i64).prop_map(|(k, v)| KvOp::Put(k, v)),
        key.prop_map(KvOp::Delete),
    ]
}

// -- Property tests ----------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Checkpoint cadence does not change final state.
    ///
    /// Write N ops. Optionally checkpoint at position K. Recover with and
    /// without checkpoint. Final states must match.
    #[test]
    fn counter_checkpoint_cadence_invariance(
        ops in prop::collection::vec(counter_op_strategy(), 1..30),
        checkpoint_at in prop::option::of(0usize..30),
    ) {
        // -- Full recovery (no checkpoint) --
        let dir_full = MemoryDirectory::arc();
        {
            let mut w = WalWriter::<CounterOp>::new(dir_full.clone());
            for op in &ops {
                w.append(op).unwrap();
            }
            w.flush().unwrap();
        }

        let full = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
            &dir_full,
            None,
            RecoveryOptions::strict(),
            counter_init,
            counter_apply,
        ).unwrap();

        // -- Recovery with checkpoint at K --
        if let Some(k) = checkpoint_at {
            if k < ops.len() {
                let dir_ckpt = MemoryDirectory::arc();
                let mut w = WalWriter::<CounterOp>::new(dir_ckpt.clone());
                for op in &ops {
                    w.append(op).unwrap();
                }
                w.flush().unwrap();
                drop(w);

                // Recover up to K, checkpoint, then recover from checkpoint.
                let partial = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
                    &dir_ckpt,
                    None,
                    RecoveryOptions::up_to((k + 1) as u64),
                    counter_init,
                    counter_apply,
                ).unwrap();

                let ckpt_state = CounterCheckpoint { value: partial.state };
                let ckpt = CheckpointFile::new(dir_ckpt.clone());
                ckpt.write_postcard("ckpt.bin", partial.last_entry_id, &ckpt_state).unwrap();

                let from_ckpt = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
                    &dir_ckpt,
                    Some("ckpt.bin"),
                    RecoveryOptions::strict(),
                    counter_init,
                    counter_apply,
                ).unwrap();

                prop_assert_eq!(full.state, from_ckpt.state,
                    "Checkpoint at {} changed final state: full={}, ckpt={}",
                    k, full.state, from_ckpt.state);
                prop_assert_eq!(full.last_entry_id, from_ckpt.last_entry_id);
            }
        }
    }

    /// Point-in-time recovery is a prefix of full recovery.
    ///
    /// For a KV store: recovering up to entry K produces a state that is
    /// consistent with applying exactly the first K operations.
    #[test]
    fn point_in_time_is_prefix(
        ops in prop::collection::vec(kv_op_strategy(), 1..20),
        cutoff_frac in 0.1f64..1.0,
    ) {
        let dir = MemoryDirectory::arc();
        let mut w = WalWriter::<KvOp>::new(dir.clone());
        for op in &ops {
            w.append(op).unwrap();
        }
        w.flush().unwrap();
        drop(w);

        let cutoff = ((ops.len() as f64 * cutoff_frac) as u64).max(1);

        // Recover up to cutoff.
        let partial = recover_with_wal::<KvCheckpoint, KvOp, _>(
            &dir,
            None,
            RecoveryOptions::up_to(cutoff),
            kv_init,
            kv_apply,
        ).unwrap();

        // Build reference state by applying first `cutoff` ops manually.
        let mut reference = BTreeMap::new();
        for (i, op) in ops.iter().enumerate() {
            if (i as u64 + 1) > cutoff {
                break;
            }
            kv_apply(&mut reference, i as u64 + 1, op.clone());
        }

        prop_assert_eq!(partial.state, reference,
            "Point-in-time recovery at entry {} diverged from reference",
            cutoff);
        prop_assert_eq!(partial.last_entry_id, cutoff);
    }

    /// Multiple checkpoints produce the same result as no checkpoints.
    ///
    /// Write ops in three phases with checkpoints between each phase.
    /// Final recovery from last checkpoint should match full replay.
    #[test]
    fn multi_checkpoint_equivalence(
        phase1 in prop::collection::vec(kv_op_strategy(), 1..10),
        phase2 in prop::collection::vec(kv_op_strategy(), 1..10),
        phase3 in prop::collection::vec(kv_op_strategy(), 1..10),
    ) {
        let dir = MemoryDirectory::arc();
        let ckpt = CheckpointFile::new(dir.clone());
        let mut w = WalWriter::<KvOp>::new(dir.clone());

        // Phase 1: write + checkpoint.
        for op in &phase1 {
            w.append(op).unwrap();
        }
        w.flush().unwrap();

        let r1 = recover_with_wal::<KvCheckpoint, KvOp, _>(
            &dir, None, RecoveryOptions::strict(), kv_init, kv_apply,
        ).unwrap();
        ckpt.write_postcard("ckpt1.bin", r1.last_entry_id, &kv_to_checkpoint(&r1.state)).unwrap();

        // Phase 2: write + checkpoint.
        for op in &phase2 {
            w.append(op).unwrap();
        }
        w.flush().unwrap();

        let r2 = recover_with_wal::<KvCheckpoint, KvOp, _>(
            &dir, Some("ckpt1.bin"), RecoveryOptions::strict(), kv_init, kv_apply,
        ).unwrap();
        ckpt.write_postcard("ckpt2.bin", r2.last_entry_id, &kv_to_checkpoint(&r2.state)).unwrap();

        // Phase 3: write (no checkpoint).
        for op in &phase3 {
            w.append(op).unwrap();
        }
        w.flush().unwrap();
        drop(w);

        // Recover from checkpoint 2.
        let from_ckpt2 = recover_with_wal::<KvCheckpoint, KvOp, _>(
            &dir, Some("ckpt2.bin"), RecoveryOptions::strict(), kv_init, kv_apply,
        ).unwrap();

        // Recover from scratch (no checkpoint).
        let from_scratch = recover_with_wal::<KvCheckpoint, KvOp, _>(
            &dir, None, RecoveryOptions::strict(), kv_init, kv_apply,
        ).unwrap();

        prop_assert_eq!(from_ckpt2.state, from_scratch.state,
            "Recovery from checkpoint 2 diverged from scratch recovery");
        prop_assert_eq!(from_ckpt2.last_entry_id, from_scratch.last_entry_id);
    }
}
