//! Crash-during-recovery test (growth-ring "two_failures" pattern).
//!
//! Scenario: write entries, simulate crash (torn tail), then inject a failure
//! during resume()'s repair phase. Verify that a subsequent resume() succeeds.

mod support;

use durability::storage::{Directory, FlushPolicy, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;
use support::faulty_directory::CountdownDirectory;

#[test]
fn crash_during_resume_repair_is_recoverable() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();

    // Step 1: Write entries and flush.
    {
        let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
        let mut w = WalWriter::<WalEntry>::with_options(dir, FlushPolicy::PerAppend, 0);
        for i in 1..=5u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i,
                doc_count: i as u32,
            })
            .unwrap();
        }
    }

    // Step 2: Simulate torn tail by truncating the segment file.
    let path = "wal/wal_1.log";
    let fs_path = fs.file_path(path).unwrap();
    let mut bytes = std::fs::read(&fs_path).unwrap();
    let original_len = bytes.len();
    bytes.truncate(original_len.saturating_sub(7)); // tear the last entry
    std::fs::write(&fs_path, &bytes).unwrap();

    // Remove stale lockfile from prior writer.
    let _ = fs.delete("wal/.lock");

    // Step 3: resume() with failures during the repair phase.
    // resume() acquires the lock, reads the segment, scans for a valid prefix,
    // then calls atomic_write() to truncate the file.
    //
    // First, do a counting run to find how many write ops resume uses.
    {
        std::fs::write(&fs_path, &bytes).unwrap();
        let _ = std::fs::remove_file(tmp.path().join("wal/.lock"));
        let countdown = Arc::new(CountdownDirectory::new(Arc::new(
            FsDirectory::new(tmp.path()).unwrap(),
        ) as Arc<dyn Directory>));
        countdown.arm(10_000);
        let _ = WalWriter::<WalEntry>::resume(countdown.clone() as Arc<dyn Directory>);
        let ops_used = 10_000 - countdown.remaining();
        assert!(ops_used > 0, "resume used no write ops -- test is invalid");

        // Now iterate over each failure point.
        // arm(0) = fail immediately, arm(1) = allow 1 op then fail, etc.
        let mut any_failure_injected = false;
        for fail_at in 0..ops_used {
            std::fs::write(&fs_path, &bytes).unwrap();
            let _ = std::fs::remove_file(tmp.path().join("wal/.lock"));
            // Also remove any .tmp left from prior failed atomic_write.
            let _ = std::fs::remove_file(tmp.path().join("wal/wal_1.log.tmp"));

            let countdown2 = Arc::new(CountdownDirectory::new(Arc::new(
                FsDirectory::new(tmp.path()).unwrap(),
            ) as Arc<dyn Directory>));
            countdown2.arm(fail_at);

            match WalWriter::<WalEntry>::resume(countdown2.clone() as Arc<dyn Directory>) {
                Ok(_w) => {
                    // resume succeeded at this failure point.
                }
                Err(_) => {
                    any_failure_injected = true;
                    // resume() failed. Now do a clean resume and verify it succeeds.
                    let _ = std::fs::remove_file(tmp.path().join("wal/.lock"));
                    let clean_dir: Arc<dyn Directory> =
                        Arc::new(FsDirectory::new(tmp.path()).unwrap());
                    let w2 = WalWriter::<WalEntry>::resume(clean_dir.clone());
                    assert!(
                        w2.is_ok(),
                        "resume() should succeed after crash at fail_at={fail_at}, got: {:?}",
                        w2.err()
                    );

                    // Verify recovery sees a valid prefix.
                    let r = WalReader::<WalEntry>::new(clean_dir);
                    let records = r.replay_best_effort().unwrap();
                    for (i, rec) in records.iter().enumerate() {
                        assert_eq!(
                            rec.entry_id,
                            (i as u64) + 1,
                            "fail_at={fail_at}: entry_id mismatch at index {i}"
                        );
                    }
                    drop(w2);
                }
            }
        }

        assert!(
            any_failure_injected,
            "no failure was injected during resume (ops_used={ops_used})"
        );
    }
}
