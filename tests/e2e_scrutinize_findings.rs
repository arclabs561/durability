//! Tests derived from deep scrutiny findings (2026-03-30).
//!
//! Each test validates a specific edge case or bug fix identified during
//! implementation review.
#![cfg(feature = "postcard")]

use durability::storage::{Directory, FsDirectory, MemoryDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Bug 4.2: Preallocated trailing zeros must not prevent recovery
// ---------------------------------------------------------------------------

#[test]
fn preallocation_crash_without_truncation_allows_best_effort_replay() {
    // Simulate: writer preallocates, writes entries, crashes before drop
    // truncates the file. The file has trailing zero bytes.
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    {
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        w.set_preallocate_bytes(64 * 1024);
        for i in 1..=5u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i,
                doc_count: i as u32,
            })
            .unwrap();
        }
        w.flush().unwrap();

        // Simulate crash: do NOT drop the writer (which would truncate).
        // Instead, leak the file handle and leave the preallocated zeros.
        std::mem::forget(w);
    }

    // The segment file should still have the preallocated size (64 KiB).
    let segments = dir.list_dir("wal").unwrap();
    let seg = segments.iter().find(|s| s.ends_with(".log")).unwrap();
    let path = format!("wal/{seg}");
    let fs_path = dir.file_path(&path).unwrap();
    let size = std::fs::metadata(&fs_path).unwrap().len();
    assert!(
        size >= 64 * 1024,
        "expected preallocated file, got {size} bytes"
    );

    // Best-effort replay should succeed despite trailing zeros.
    let r = WalReader::<WalEntry>::new(dir.clone());
    let records = r.replay_best_effort().unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[4].entry_id, 5);

    // Clean up the leaked lockfile so resume() can proceed.
    let _ = dir.delete("wal/.lock");

    // resume() should also succeed (it uses BestEffortTail internally).
    let mut w = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
    let id = w
        .append(&WalEntry::AddSegment {
            segment_id: 6,
            doc_count: 6,
        })
        .unwrap();
    assert_eq!(id, 6);
    w.flush().unwrap();
    drop(w);

    let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
    assert_eq!(records.len(), 6);
}

// ---------------------------------------------------------------------------
// Recovery: first segment with torn tail
// ---------------------------------------------------------------------------

#[test]
fn resume_repairs_first_and_only_segment_with_torn_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    {
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 5,
        })
        .unwrap();
        w.append(&WalEntry::DeleteDocuments {
            deletes: vec![(1, 3)],
        })
        .unwrap();
        w.flush().unwrap();
    }

    // Truncate last 5 bytes to simulate torn tail.
    let path = "wal/wal_1.log";
    let fs_path = dir.file_path(path).unwrap();
    let mut bytes = std::fs::read(&fs_path).unwrap();
    bytes.truncate(bytes.len().saturating_sub(5));
    std::fs::write(&fs_path, &bytes).unwrap();

    // Strict replay fails.
    let r = WalReader::<WalEntry>::new(dir.clone());
    assert!(r.replay().is_err());

    // resume() should repair and allow continuation.
    let mut w = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
    let id = w
        .append(&WalEntry::AddSegment {
            segment_id: 2,
            doc_count: 7,
        })
        .unwrap();
    // Entry 2 was lost by truncation, so next id is 2.
    assert_eq!(id, 2);
    w.flush().unwrap();
    drop(w);

    // Strict replay should now work.
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].entry_id, 1); // survived truncation
    assert_eq!(records[1].entry_id, 2); // new entry
}

// ---------------------------------------------------------------------------
// Recovery: header-only first segment (all entries lost)
// ---------------------------------------------------------------------------

#[test]
fn resume_handles_header_only_first_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    {
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        w.flush().unwrap();
    }

    // Truncate to header only.
    let path = "wal/wal_1.log";
    let fs_path = dir.file_path(path).unwrap();
    let bytes = std::fs::read(&fs_path).unwrap();
    // WAL header is 24 bytes.
    std::fs::write(&fs_path, &bytes[..24]).unwrap();

    let mut w = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
    // All entries lost; next entry_id should be 1.
    let id = w
        .append(&WalEntry::AddSegment {
            segment_id: 99,
            doc_count: 99,
        })
        .unwrap();
    assert_eq!(id, 1);
    w.flush().unwrap();
    drop(w);

    let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].entry_id, 1);
}

// ---------------------------------------------------------------------------
// Recovery: orphaned checkpoint (WAL Checkpoint entry not flushed)
// ---------------------------------------------------------------------------

#[test]
fn orphaned_checkpoint_file_does_not_corrupt_recovery() {
    use durability::checkpoint::CheckpointFile;
    use durability::recover::{CheckpointState, RecoveryManager};

    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

    // Write WAL entries.
    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 10,
    })
    .unwrap();
    w.append(&WalEntry::DeleteDocuments {
        deletes: vec![(1, 5)],
    })
    .unwrap();
    w.flush().unwrap();

    // Write a checkpoint file but do NOT append WalEntry::Checkpoint.
    // This simulates a crash between checkpoint write and WAL marker.
    let ckpt_state = CheckpointState {
        segments: vec![durability::recover::CheckpointSegment {
            segment_id: 1,
            doc_count: 10,
            deleted_docs: vec![5],
        }],
    };
    CheckpointFile::new(dir.clone())
        .write_postcard("checkpoints/c1.bin", 2, &ckpt_state)
        .unwrap();

    // recover_latest() should NOT find the checkpoint (no WAL marker).
    let mgr = RecoveryManager::new(dir.clone());
    let state = mgr.recover_latest().unwrap();
    assert_eq!(state.last_entry_id, 2);
    assert_eq!(state.segments.len(), 1);

    // Explicit checkpoint path should work.
    let state2 = mgr.recover(Some("checkpoints/c1.bin")).unwrap();
    assert_eq!(state2.last_entry_id, 2);
    assert_eq!(state2.segments.len(), 1);
    assert!(state2.segments[0].deleted_docs.contains(&5));
    drop(w);
}

// ---------------------------------------------------------------------------
// Reader sees updates after writer flush (sequential, not concurrent)
// ---------------------------------------------------------------------------

#[test]
fn reader_sees_new_entries_after_writer_flush() {
    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 1,
    })
    .unwrap();
    w.flush().unwrap();

    // Reader replays after writer flush.
    let r = WalReader::<WalEntry>::new(dir.clone());
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 1);

    // Writer appends more.
    w.append(&WalEntry::AddSegment {
        segment_id: 2,
        doc_count: 2,
    })
    .unwrap();
    w.flush().unwrap();

    // Reader sees updated state (MemoryDirectory snapshots on open_file).
    let records2 = r.replay().unwrap();
    assert_eq!(records2.len(), 2);

    drop(w);
}

// ---------------------------------------------------------------------------
// MemoryDirectory: atomic_rename overwrites destination
// ---------------------------------------------------------------------------

#[test]
fn memory_directory_atomic_rename_overwrites_existing() {
    let dir = MemoryDirectory::new();
    dir.atomic_write("a.bin", b"original").unwrap();
    dir.atomic_write("b.bin", b"to_overwrite").unwrap();

    dir.atomic_rename("a.bin", "b.bin").unwrap();

    assert!(!dir.exists("a.bin"));
    assert!(dir.exists("b.bin"));

    let mut buf = Vec::new();
    use std::io::Read;
    dir.open_file("b.bin")
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    assert_eq!(buf, b"original");
}

// ---------------------------------------------------------------------------
// MemoryDirectory: delete on nonexistent path is silent
// ---------------------------------------------------------------------------

#[test]
fn memory_directory_delete_nonexistent_is_ok() {
    let dir = MemoryDirectory::new();
    // Should not error.
    dir.delete("does_not_exist").unwrap();
    dir.delete("also/does/not/exist").unwrap();
}
