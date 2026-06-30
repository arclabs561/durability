//! I/O countdown crash-recovery test (inspired by redb's FuzzerBackend).
//!
//! Strategy: write N entries to a WAL, arm the countdown so writes fail at
//! various points, then verify recovery produces a valid prefix.
#![cfg(feature = "postcard")]

mod support;

use durability::storage::{Directory, FsDirectory, MemoryDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;
use support::faulty_directory::CountdownDirectory;

/// Write entries with countdown fault injection, returning (dir, flushed_ids).
fn write_with_countdown(fail_after: u64) -> (Arc<dyn Directory>, Vec<u64>) {
    let mem = MemoryDirectory::arc();
    let countdown = Arc::new(CountdownDirectory::new(mem));
    let dir: Arc<dyn Directory> = countdown.clone();

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    let mut flushed_ids = Vec::new();

    for i in 0..20u64 {
        let entry = WalEntry::AddSegment {
            segment_id: i + 1,
            doc_count: (i as u32) * 10,
        };

        if i == 0 {
            countdown.arm(fail_after);
        }

        match w.append(&entry) {
            Ok(id) => match w.flush() {
                Ok(()) => flushed_ids.push(id),
                Err(_) => break,
            },
            Err(_) => break,
        }
    }

    drop(w);
    countdown.disarm();
    (dir, flushed_ids)
}

#[test]
fn countdown_recovery_produces_valid_prefix() {
    for fail_after in 1..30u64 {
        let (dir, flushed_ids) = write_with_countdown(fail_after);

        // Read back from the WAL and verify recovery matches flushed entries.
        let reader = WalReader::<WalEntry>::new(dir);
        let recovered = reader.replay_best_effort().unwrap();

        // Recovered entries must be a superset of flushed entries
        // (MemoryDirectory writes are visible even without fsync).
        assert!(
            recovered.len() >= flushed_ids.len(),
            "fail_after={}: recovered {} but flushed {}",
            fail_after,
            recovered.len(),
            flushed_ids.len()
        );

        // The flushed IDs must appear at the start of recovery.
        for (idx, &flushed_id) in flushed_ids.iter().enumerate() {
            assert_eq!(
                recovered[idx].entry_id, flushed_id,
                "fail_after={}: mismatch at index {}",
                fail_after, idx
            );
        }

        // All recovered entry IDs must be strictly increasing from 1.
        for (i, r) in recovered.iter().enumerate() {
            assert_eq!(
                r.entry_id,
                (i as u64) + 1,
                "fail_after={}: entry_id gap at index {}",
                fail_after,
                i
            );
        }
    }
}

#[test]
fn countdown_recovery_on_fs_produces_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();
    let countdown = Arc::new(CountdownDirectory::new(Arc::new(fs) as Arc<dyn Directory>));
    let dir: Arc<dyn Directory> = countdown.clone();

    // Write 10 entries with per-append flush
    let mut w = WalWriter::<WalEntry>::with_flush_policy(
        dir.clone(),
        durability::storage::FlushPolicy::PerAppend,
    );
    let mut written_ids = Vec::new();
    for i in 0..10u64 {
        let id = w
            .append(&WalEntry::AddSegment {
                segment_id: i + 1,
                doc_count: (i as u32) * 10,
            })
            .unwrap();
        written_ids.push(id);
    }

    // All 10 should have been written and flushed
    assert_eq!(written_ids.len(), 10);

    // Drop the writer to close the file handle, then arm the countdown.
    // This forces the next writer to re-open via the Directory, hitting the countdown.
    drop(w);
    countdown.arm(0);

    // A new writer (resume) should fail because it needs to open files
    let resume_result = WalWriter::<WalEntry>::resume(dir.clone());
    assert!(
        resume_result.is_err() || {
            // If resume succeeds (it reads existing segments), the next append should fail
            let mut w2 = resume_result.unwrap();
            w2.append(&WalEntry::AddSegment {
                segment_id: 99,
                doc_count: 0,
            })
            .is_err()
        }
    );

    // Disarm and verify recovery sees exactly 10 entries
    countdown.disarm();

    let reader = WalReader::<WalEntry>::new(dir.clone());
    let recovered = reader.replay().unwrap();
    assert_eq!(recovered.len(), 10);
    for (i, r) in recovered.iter().enumerate() {
        assert_eq!(r.entry_id, (i as u64) + 1);
        match &r.payload {
            WalEntry::AddSegment {
                segment_id,
                doc_count,
            } => {
                assert_eq!(*segment_id, (i as u64) + 1);
                assert_eq!(*doc_count, (i as u32) * 10);
            }
            other => panic!("unexpected entry: {other:?}"),
        }
    }

    // Also test streaming replay returns the same count
    let mut streaming_count = 0u64;
    let stream_result = reader.replay_each(|_record| {
        streaming_count += 1;
        Ok(())
    });
    assert_eq!(stream_result.unwrap(), 10);
    assert_eq!(streaming_count, 10);
}

/// Write entries with fault injection during multi-segment rotation.
/// Arms the countdown after writing some entries, forcing failure during
/// segment rotation. Recovery must return a valid prefix.
#[test]
fn countdown_mid_segment_rotation_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();
    let countdown = Arc::new(CountdownDirectory::new(Arc::new(fs) as Arc<dyn Directory>));
    let dir: Arc<dyn Directory> = countdown.clone();

    let mut w = WalWriter::<WalEntry>::with_flush_policy(
        dir.clone(),
        durability::storage::FlushPolicy::PerAppend,
    );
    // Small segments to force rotation
    w.set_segment_size_limit_bytes(100);

    // Write a few entries successfully first.
    for i in 0..5u64 {
        w.append(&WalEntry::AddSegment {
            segment_id: i + 1,
            doc_count: i as u32,
        })
        .unwrap();
    }

    // Arm countdown: fail after 3 more write ops (mid-rotation).
    countdown.arm(3);

    let mut extra = 0u64;
    for i in 5..50u64 {
        match w.append(&WalEntry::AddSegment {
            segment_id: i + 1,
            doc_count: i as u32,
        }) {
            Ok(_) => extra += 1,
            Err(_) => break,
        }
    }
    drop(w);

    // Disarm and verify recovery returns a valid prefix.
    countdown.disarm();
    let reader = WalReader::<WalEntry>::new(dir.clone());
    let replayed = reader.replay_best_effort().unwrap();

    // Must have at least the 5 pre-fault entries.
    assert!(
        replayed.len() >= 5,
        "expected at least 5 entries, got {}",
        replayed.len()
    );
    // Must not have more than 5 + extra (everything that succeeded).
    assert!(
        replayed.len() <= 5 + extra as usize,
        "recovered {} but only {} succeeded",
        replayed.len(),
        5 + extra
    );
    // Entry IDs must be strictly increasing from 1.
    for (i, r) in replayed.iter().enumerate() {
        assert_eq!(r.entry_id, (i as u64) + 1);
    }
}
