//! Exhaustive single-point failure test (inspired by growth-ring's rand_fail).
//!
//! Strategy: write a fixed WAL session (N entries across multiple segments),
//! count total I/O operations, then iterate over ALL failure points.
//! At each failure point, verify that best-effort recovery produces a valid
//! prefix of the original entries.
#![cfg(feature = "postcard")]

mod support;

use durability::storage::{Directory, FlushPolicy, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;
use support::faulty_directory::CountdownDirectory;

/// The reference entries we write in each test run.
fn reference_entries() -> Vec<WalEntry> {
    (1..=10u64)
        .map(|i| WalEntry::AddSegment {
            segment_id: i,
            doc_count: i as u32 * 100,
        })
        .collect()
}

/// Count total write I/O operations in a clean run.
fn count_write_ops(entries: &[WalEntry]) -> u64 {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();
    let countdown = Arc::new(CountdownDirectory::new(Arc::new(fs) as Arc<dyn Directory>));
    let dir: Arc<dyn Directory> = countdown.clone();

    let mut w = WalWriter::<WalEntry>::with_options(dir, FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(150); // small segments -> multiple rotations
    for e in entries {
        w.append(e).unwrap();
    }
    drop(w);

    // The countdown was never armed, so remaining is still u64::MAX.
    // We need to actually count. Do a second run with a very high arm value.
    let tmp2 = tempfile::tempdir().unwrap();
    let fs2 = FsDirectory::new(tmp2.path()).unwrap();
    let countdown2 = Arc::new(CountdownDirectory::new(Arc::new(fs2) as Arc<dyn Directory>));
    let dir2: Arc<dyn Directory> = countdown2.clone();

    // Arm with a huge number so it never triggers but we can see how many ops were consumed.
    countdown2.arm(10_000);
    let mut w2 = WalWriter::<WalEntry>::with_options(dir2, FlushPolicy::PerAppend, 0);
    w2.set_segment_size_limit_bytes(150);
    for e in entries {
        w2.append(e).unwrap();
    }
    drop(w2);

    // remaining = 10_000 - ops_used
    let remaining = countdown2.remaining();
    10_000 - remaining
}

/// Run one iteration: write entries with failure at `fail_at`, then recover.
/// Returns (entries_recovered, total_entries).
fn run_with_failure_at(entries: &[WalEntry], fail_at: u64) -> (usize, usize) {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();
    let countdown = Arc::new(CountdownDirectory::new(Arc::new(fs) as Arc<dyn Directory>));
    let dir: Arc<dyn Directory> = countdown.clone();

    countdown.arm(fail_at);
    let mut w = WalWriter::<WalEntry>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(150);

    let mut written = 0usize;
    for e in entries {
        match w.append(e) {
            Ok(_) => written += 1,
            Err(_) => break,
        }
    }
    // Try to flush any remaining buffered data (may fail).
    let _ = w.flush();
    drop(w);

    // Disarm and recover.
    countdown.disarm();
    let reader = WalReader::<WalEntry>::new(dir);
    let recovered = reader.replay_best_effort().unwrap();

    // Verify the prefix property: recovered entries must be a prefix of the
    // reference entries (same order, same content).
    for (i, rec) in recovered.iter().enumerate() {
        assert_eq!(
            rec.entry_id,
            (i as u64) + 1,
            "fail_at={fail_at}: entry {i} has wrong id (got {}, expected {})",
            rec.entry_id,
            i + 1
        );
        assert_eq!(
            rec.payload, entries[i],
            "fail_at={fail_at}: entry {i} payload mismatch"
        );
    }

    // recovered.len() <= written (some writes may not have been flushed/synced)
    assert!(
        recovered.len() <= written,
        "fail_at={fail_at}: recovered {} entries but only wrote {} before failure",
        recovered.len(),
        written,
    );

    (recovered.len(), entries.len())
}

#[test]
fn exhaustive_single_point_failure_produces_valid_prefix() {
    let entries = reference_entries();
    let total_ops = count_write_ops(&entries);

    // Iterate over every possible failure point.
    let mut any_partial = false;
    for fail_at in 1..=total_ops {
        let (recovered, _total) = run_with_failure_at(&entries, fail_at);
        if recovered < entries.len() {
            any_partial = true;
        }
    }

    // At least some failure points should produce partial recovery
    // (otherwise the test isn't exercising failure paths).
    assert!(
        any_partial,
        "no failure point produced partial recovery -- test may not be injecting faults correctly"
    );
}
