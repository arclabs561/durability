//! Fault injection tests for generic `recover_with_wal`.
//!
//! Uses CountdownDirectory to simulate crashes at various points during
//! WAL writes, then verifies recovery produces a valid prefix.
#![cfg(feature = "postcard")]

mod support;

use durability::checkpoint::CheckpointFile;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::{Directory, FsDirectory, MemoryDirectory};
use durability::walog::WalWriter;
use std::sync::Arc;
use support::faulty_directory::CountdownDirectory;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct CounterOp {
    delta: i64,
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct CounterCheckpoint {
    value: i64,
}

fn counter_init(ckpt: Option<CounterCheckpoint>) -> i64 {
    ckpt.map(|c| c.value).unwrap_or(0)
}

fn counter_apply(state: &mut i64, _id: u64, op: CounterOp) {
    *state += op.delta;
}

/// Write N entries, crash at write operation K, recover with best-effort.
/// Verify recovered state is a prefix of the intended state.
#[test]
fn countdown_crash_produces_valid_prefix() {
    let entries: Vec<CounterOp> = (1..=20).map(|i| CounterOp { delta: i }).collect();

    // Compute expected prefix sums for verification.
    let mut prefix_sums = vec![0i64];
    let mut acc = 0i64;
    for e in &entries {
        acc += e.delta;
        prefix_sums.push(acc);
    }

    // Try crashing at each write operation count.
    for crash_at in 1..30u64 {
        let inner = MemoryDirectory::arc();
        let countdown = Arc::new(CountdownDirectory::new(inner.clone()));

        // Write entries with countdown armed.
        countdown.arm(crash_at);
        let mut w = WalWriter::<CounterOp>::with_flush_policy(
            countdown.clone() as Arc<dyn Directory>,
            durability::storage::FlushPolicy::PerAppend,
        );
        let mut written = 0usize;
        for e in &entries {
            match w.append(e) {
                Ok(_) => written += 1,
                Err(_) => break,
            }
            if w.flush().is_err() {
                break;
            }
        }

        // Disarm and recover.
        countdown.disarm();
        drop(w);

        let result = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
            &(countdown.clone() as Arc<dyn Directory>),
            None,
            RecoveryOptions::best_effort(),
            counter_init,
            counter_apply,
        )
        .unwrap();

        // Recovered state must be one of the valid prefix sums.
        let recovered_count = result.last_entry_id as usize;
        assert!(
            recovered_count <= written,
            "crash_at={crash_at}: recovered {recovered_count} entries but only wrote {written}"
        );
        assert!(
            recovered_count < prefix_sums.len(),
            "crash_at={crash_at}: recovered_count {recovered_count} out of range"
        );
        assert_eq!(
            result.state, prefix_sums[recovered_count],
            "crash_at={crash_at}: recovered state {} != expected prefix_sum[{}]={}",
            result.state, recovered_count, prefix_sums[recovered_count]
        );
    }
}

/// Checkpoint + crash during WAL suffix: recovery from checkpoint produces
/// correct state even if the WAL suffix is incomplete.
#[test]
fn checkpoint_survives_wal_suffix_crash() {
    let inner = MemoryDirectory::arc();
    let countdown = Arc::new(CountdownDirectory::new(inner.clone()));
    let dir: Arc<dyn Directory> = countdown.clone();

    // Phase 1: write entries 1-5, checkpoint.
    {
        let mut w = WalWriter::<CounterOp>::new(dir.clone());
        for i in 1..=5 {
            w.append(&CounterOp { delta: i }).unwrap();
        }
        w.flush().unwrap();

        let mid = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
            &dir,
            None,
            RecoveryOptions::strict(),
            counter_init,
            counter_apply,
        )
        .unwrap();
        assert_eq!(mid.state, 15); // 1+2+3+4+5

        let ckpt_state = CounterCheckpoint { value: mid.state };
        CheckpointFile::new(dir.clone())
            .write_postcard("ckpt.bin", mid.last_entry_id, &ckpt_state)
            .unwrap();

        // Phase 2: write entries 6-10, crash mid-way.
        countdown.arm(3); // crash after 3 more write ops
        for i in 6..=10 {
            if w.append(&CounterOp { delta: i }).is_err() {
                break;
            }
            if w.flush().is_err() {
                break;
            }
        }
        countdown.disarm();
        drop(w);
    }

    // Recover from checkpoint -- must get at least the checkpoint state (15).
    let result = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
        &dir,
        Some("ckpt.bin"),
        RecoveryOptions::best_effort(),
        counter_init,
        counter_apply,
    )
    .unwrap();

    // Must be >= 15 (checkpoint value) and any additional entries must be correct.
    assert!(
        result.state >= 15,
        "recovered state {} is less than checkpoint value 15",
        result.state
    );
    assert!(result.last_entry_id >= 5);
}

/// Resume after crash: WalWriter::resume repairs torn tail, then new
/// entries can be appended and recovered.
#[test]
fn resume_after_crash_then_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    // Write entries 1-5.
    {
        let mut w = WalWriter::<CounterOp>::new(dir.clone());
        for i in 1..=5 {
            w.append(&CounterOp { delta: i }).unwrap();
        }
        w.flush().unwrap();
    }

    // Simulate torn write: truncate the last 3 bytes of the WAL file.
    let wal_files = dir.list_dir("wal").unwrap();
    let seg_file = wal_files.iter().find(|f| f.ends_with(".log")).unwrap();
    let wal_path = format!("wal/{seg_file}");
    let fs_path = dir.file_path(&wal_path).unwrap();
    let mut bytes = std::fs::read(&fs_path).unwrap();
    bytes.truncate(bytes.len().saturating_sub(3));
    std::fs::write(&fs_path, &bytes).unwrap();

    // resume() should repair the torn tail.
    let mut w = WalWriter::<CounterOp>::resume(dir.clone()).unwrap();

    // Append more entries.
    for i in 100..=103 {
        w.append(&CounterOp { delta: i }).unwrap();
    }
    w.flush().unwrap();
    drop(w);

    // Recover with generic function.
    let result = recover_with_wal::<CounterCheckpoint, CounterOp, _>(
        &dir,
        None,
        RecoveryOptions::strict(),
        counter_init,
        counter_apply,
    )
    .unwrap();

    // Entries: some prefix of [1,2,3,4,5] (one may be lost) + [100,101,102,103].
    // The sum of 100..103 = 406. The prefix sum of 1..5 is at most 15.
    assert!(
        result.state >= 406,
        "recovered state {} too low",
        result.state
    );
    assert!(result.last_entry_id >= 4, "too few entries recovered");
}
