//! Property tests for generic `recover_with_wal`.
//!
//! Validates the reuse-seam invariant: recovery produces the same final state
//! regardless of when checkpoints happen. Also validates point-in-time recovery
//! produces a valid prefix of the full recovery.
#![cfg(feature = "postcard")]

use durability::checkpoint::CheckpointFile;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::{Directory, MemoryDirectory};
use durability::walog::{WalReader, WalSegmentHeader, WalWriter};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::io::Read;

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

fn read_u32_le(bytes: &[u8], offset: usize) -> u32 {
    let b: [u8; 4] = bytes[offset..offset + 4].try_into().unwrap();
    u32::from_le_bytes(b)
}

fn corrupt_payload_of_entry(
    dir: &std::sync::Arc<dyn Directory>,
    entry_index: usize,
    flip_idx: usize,
    flip_mask: u8,
) {
    let mut bytes = Vec::new();
    dir.open_file("wal/wal_1.log")
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();

    let mut frame = WalSegmentHeader::SIZE;
    for _ in 1..entry_index {
        let frame_len = read_u32_le(&bytes, frame) as usize;
        frame += frame_len;
    }

    let frame_len = read_u32_le(&bytes, frame) as usize;
    let payload_len = frame_len - 16;
    let payload_idx = frame + 16 + (flip_idx % payload_len);
    bytes[payload_idx] ^= flip_mask | 0x01;

    dir.atomic_write("wal/wal_1.log", &bytes).unwrap();
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

#[test]
fn recovery_from_checkpoint_skips_checkpointed_entry_id() {
    let dir = MemoryDirectory::arc();
    let mut w = WalWriter::<CounterOp>::new(dir.clone());
    w.append(&CounterOp::Add(10)).unwrap();
    w.append(&CounterOp::Add(1)).unwrap();
    w.flush().unwrap();
    drop(w);

    let ckpt = CheckpointFile::new(dir.clone());
    ckpt.write_postcard("counter.chk", 1, &CounterCheckpoint { value: 10 })
        .unwrap();

    let recovered = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
        &dir,
        Some("counter.chk"),
        RecoveryOptions::strict(),
        counter_init,
        counter_apply,
    )
    .unwrap();

    assert_eq!(recovered.state, 11);
    assert_eq!(recovered.last_entry_id, 2);
}

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

    #[test]
    fn point_in_time_ignores_corrupt_first_entry_after_cutoff(
        ops in prop::collection::vec(kv_op_strategy(), 2..20),
        cutoff_seed in any::<usize>(),
        flip_idx in any::<usize>(),
        flip_mask in any::<u8>(),
    ) {
        let cutoff = 1 + (cutoff_seed % (ops.len() - 1));

        let dir = MemoryDirectory::arc();
        let mut w = WalWriter::<KvOp>::new(dir.clone());
        for op in &ops {
            w.append(op).unwrap();
        }
        w.flush().unwrap();
        drop(w);

        corrupt_payload_of_entry(&dir, cutoff + 1, flip_idx, flip_mask);
        prop_assert!(WalReader::<KvOp>::new(dir.clone()).replay().is_err());

        let partial = recover_with_wal::<KvCheckpoint, KvOp, _>(
            &dir,
            None,
            RecoveryOptions::up_to(cutoff as u64),
            kv_init,
            kv_apply,
        ).unwrap();

        let mut reference = BTreeMap::new();
        for (i, op) in ops.iter().take(cutoff).enumerate() {
            kv_apply(&mut reference, i as u64 + 1, op.clone());
        }

        prop_assert_eq!(partial.state, reference);
        prop_assert_eq!(partial.last_entry_id, cutoff as u64);
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
