//! Key-value store example using generic `recover_with_wal`.
//!
//! Demonstrates the generic recovery API with a fully custom domain --
//! no dependence on the built-in `WalEntry` type.
//!
//! Run:
//! `cargo run -p durability --example kv_store`

use durability::checkpoint::CheckpointFile;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::FsDirectory;
use durability::walog::WalWriter;
use std::collections::BTreeMap;

/// Custom WAL entry type for a key-value store.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum KvOp {
    Put { key: String, value: String },
    Delete { key: String },
}

/// Checkpoint: sorted key-value pairs.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct KvSnapshot {
    pairs: Vec<(String, String)>,
}

type KvState = BTreeMap<String, String>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let dir = FsDirectory::arc(tmp.path())?;

    // -- Phase 1: write some operations --
    let mut wal = WalWriter::<KvOp>::new(dir.clone());
    wal.append(&KvOp::Put {
        key: "city".into(),
        value: "Tokyo".into(),
    })?;
    wal.append(&KvOp::Put {
        key: "lang".into(),
        value: "Rust".into(),
    })?;
    wal.append(&KvOp::Delete { key: "lang".into() })?;
    wal.append(&KvOp::Put {
        key: "year".into(),
        value: "2026".into(),
    })?;
    wal.flush()?;

    // -- Phase 2: recover and checkpoint --
    let recovered = recover_with_wal::<KvSnapshot, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::strict(),
        |ckpt| match ckpt {
            Some(s) => s.pairs.into_iter().collect(),
            None => KvState::new(),
        },
        |state, _id, op| match op {
            KvOp::Put { key, value } => {
                state.insert(key, value);
            }
            KvOp::Delete { key } => {
                state.remove(&key);
            }
        },
    )?;

    println!("After recovery (no checkpoint):");
    for (k, v) in &recovered.state {
        println!("  {k} = {v}");
    }
    println!("  last_entry_id = {}", recovered.last_entry_id);

    // Write checkpoint.
    let snap = KvSnapshot {
        pairs: recovered
            .state
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    };
    CheckpointFile::new(dir.clone()).write_postcard(
        "checkpoints/kv.chk",
        recovered.last_entry_id,
        &snap,
    )?;

    // -- Phase 3: write more ops after checkpoint --
    wal.append(&KvOp::Put {
        key: "lang".into(),
        value: "Zig".into(),
    })?;
    wal.flush()?;
    drop(wal);

    // -- Phase 4: recover from checkpoint --
    let final_state = recover_with_wal::<KvSnapshot, KvOp, _>(
        &dir,
        Some("checkpoints/kv.chk"),
        RecoveryOptions::strict(),
        |ckpt| match ckpt {
            Some(s) => s.pairs.into_iter().collect(),
            None => KvState::new(),
        },
        |state, _id, op| match op {
            KvOp::Put { key, value } => {
                state.insert(key, value);
            }
            KvOp::Delete { key } => {
                state.remove(&key);
            }
        },
    )?;

    println!("\nAfter recovery (from checkpoint):");
    for (k, v) in &final_state.state {
        println!("  {k} = {v}");
    }
    println!("  last_entry_id = {}", final_state.last_entry_id);

    // -- Phase 5: point-in-time recovery --
    let pit = recover_with_wal::<KvSnapshot, KvOp, _>(
        &dir,
        None,
        RecoveryOptions::up_to(2), // only first 2 entries
        |ckpt| match ckpt {
            Some(s) => s.pairs.into_iter().collect(),
            None => KvState::new(),
        },
        |state, _id, op| match op {
            KvOp::Put { key, value } => {
                state.insert(key, value);
            }
            KvOp::Delete { key } => {
                state.remove(&key);
            }
        },
    )?;

    println!("\nPoint-in-time recovery (up to entry 2):");
    for (k, v) in &pit.state {
        println!("  {k} = {v}");
    }

    Ok(())
}
