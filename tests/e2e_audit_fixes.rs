//! Regression tests for audit findings (2026-04-05).
//!
//! C-1: decode_raw length<16 check moved before reading entry_id/checksum
//! C-2: rotate_if_needed poisons writer on segment creation failure
//! C-3: early-stop sentinel replaced with skip logic
#![cfg(feature = "postcard")]

use durability::error::{PersistenceError, PersistenceResult};
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::FlushPolicy;
use durability::storage::{Directory, MemoryDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// C-1: corrupt length field in [1,15] must error without desyncing the stream
// ---------------------------------------------------------------------------

#[test]
fn corrupt_length_field_under_16_errors_in_strict_mode() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 10,
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    let files = dir.list_dir("wal").unwrap();
    let wal_file = files.iter().find(|f| f.ends_with(".log")).unwrap();
    let wal_path = format!("wal/{wal_file}");

    let mut data = Vec::new();
    dir.open_file(&wal_path)
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();

    // Append a corrupt frame: length=8 (less than minimum 16)
    data.extend_from_slice(&8u32.to_le_bytes());
    data.extend_from_slice(&[0xAA; 16]);
    dir.atomic_write(&wal_path, &data).unwrap();

    let reader = WalReader::<WalEntry>::new(dir);
    let err = reader.replay().unwrap_err();
    match err {
        PersistenceError::Format(msg) => {
            assert!(
                msg.contains("length < header"),
                "expected length < header error, got: {msg}"
            );
        }
        other => panic!("expected Format error, got: {other}"),
    }
}

#[test]
fn corrupt_length_field_under_16_errors_in_best_effort() {
    // In BestEffortTail mode, a corrupt length in [1,15] should still be
    // a format error. Only length==0 is treated as EOF (preallocation padding).
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 10,
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    let files = dir.list_dir("wal").unwrap();
    let wal_file = files.iter().find(|f| f.ends_with(".log")).unwrap();
    let wal_path = format!("wal/{wal_file}");

    let mut data = Vec::new();
    dir.open_file(&wal_path)
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();

    // Append a corrupt frame with length=4
    data.extend_from_slice(&4u32.to_le_bytes());
    dir.atomic_write(&wal_path, &data).unwrap();

    let reader = WalReader::<WalEntry>::new(dir);
    let err = reader.replay_best_effort().unwrap_err();
    match err {
        PersistenceError::Format(msg) => {
            assert!(msg.contains("length < header"));
        }
        other => panic!("expected Format error, got: {other}"),
    }
}

#[test]
fn zero_length_is_eof_in_best_effort_but_error_in_strict() {
    // length==0 is EOF in BestEffortTail, error in Strict.
    // Ensures C-1 fix didn't break the zero-padding path.
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 10,
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    let files = dir.list_dir("wal").unwrap();
    let wal_file = files.iter().find(|f| f.ends_with(".log")).unwrap();
    let wal_path = format!("wal/{wal_file}");

    let mut data = Vec::new();
    dir.open_file(&wal_path)
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();

    // Trailing zeros (simulates preallocation)
    data.extend_from_slice(&[0u8; 16]);
    dir.atomic_write(&wal_path, &data).unwrap();

    // Best-effort: treated as EOF, returns 1 entry
    let reader = WalReader::<WalEntry>::new(dir.clone());
    let entries = reader.replay_best_effort().unwrap();
    assert_eq!(entries.len(), 1);

    // Strict: error (EOF mid-frame)
    let reader2 = WalReader::<WalEntry>::new(dir);
    assert!(reader2.replay().is_err());
}

#[test]
fn resume_propagates_corrupt_length_in_last_segment() {
    // resume() calls scan_last_segment_prefix which uses BestEffortTail.
    // A corrupt non-zero length < 16 is a format error, not a torn tail.
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 10,
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    let files = dir.list_dir("wal").unwrap();
    let wal_file = files.iter().find(|f| f.ends_with(".log")).unwrap();
    let wal_path = format!("wal/{wal_file}");

    let mut data = Vec::new();
    dir.open_file(&wal_path)
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();

    // Append a corrupt frame with length=12
    data.extend_from_slice(&12u32.to_le_bytes());
    data.extend_from_slice(&[0xFF; 20]);
    dir.atomic_write(&wal_path, &data).unwrap();

    let err = WalWriter::<WalEntry>::resume(dir);
    assert!(
        err.is_err(),
        "resume should error on corrupt non-zero length in tail"
    );
}

// ---------------------------------------------------------------------------
// C-2: rotate_if_needed poisons writer on segment creation failure
// ---------------------------------------------------------------------------

/// A directory wrapper that fails create_file after a trigger is set.
struct FailingCreateDir {
    inner: Arc<dyn Directory>,
    fail_create: AtomicBool,
}

impl Directory for FailingCreateDir {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        if self.fail_create.load(Ordering::SeqCst) && path.contains("wal_") {
            return Err(PersistenceError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected",
            )));
        }
        self.inner.create_file(path)
    }

    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        self.inner.append_file(path)
    }

    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read + Send>> {
        self.inner.open_file(path)
    }

    fn exists(&self, path: &str) -> bool {
        self.inner.exists(path)
    }

    fn delete(&self, path: &str) -> PersistenceResult<()> {
        self.inner.delete(path)
    }

    fn atomic_rename(&self, from: &str, to: &str) -> PersistenceResult<()> {
        self.inner.atomic_rename(from, to)
    }

    fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>> {
        self.inner.list_dir(path)
    }

    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        self.inner.atomic_write(path, data)
    }

    fn create_dir_all(&self, path: &str) -> PersistenceResult<()> {
        self.inner.create_dir_all(path)
    }

    fn file_path(&self, _path: &str) -> Option<PathBuf> {
        None
    }
}

#[test]
fn writer_poisoned_after_rotation_failure() {
    let inner = MemoryDirectory::arc();
    let failing_dir = Arc::new(FailingCreateDir {
        inner: inner.clone(),
        fail_create: AtomicBool::new(false),
    });

    let dir: Arc<dyn Directory> = failing_dir.clone();
    // Minimum segment size to force rotation on second entry.
    // min = header(24) + 16 = 40. PerAppend + buffer 0 = immediate drain.
    let mut w = WalWriter::<WalEntry>::with_options(dir, FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(40);

    // First entry fits (header + entry ~ 50 bytes, but current_offset must be > header
    // for rotation to trigger, so first entry always goes into segment 1).
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 100,
    })
    .unwrap();

    // Trigger create_file failure for the new segment
    failing_dir.fail_create.store(true, Ordering::SeqCst);

    // Second entry should trigger rotation (current_offset > header, projected > 40)
    // and fail on create_file for segment 2.
    let err = w.append(&WalEntry::AddSegment {
        segment_id: 99,
        doc_count: 1,
    });
    assert!(
        err.is_err(),
        "append should fail when segment creation fails"
    );

    // Writer should now be poisoned
    failing_dir.fail_create.store(false, Ordering::SeqCst);
    let err2 = w.append(&WalEntry::AddSegment {
        segment_id: 100,
        doc_count: 1,
    });
    match err2 {
        Err(PersistenceError::InvalidState(msg)) => {
            assert!(
                msg.contains("poisoned"),
                "expected poisoned error, got: {msg}"
            );
        }
        other => panic!("expected poisoned error, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// C-3: early-stop sentinel replaced — point-in-time recovery still works
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize)]
enum CountOp {
    Set(u64),
    Inc,
}

#[test]
fn point_in_time_recovery_stops_at_ceiling() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<CountOp>::new(dir.clone());
    for i in 1..=10u64 {
        w.append(&CountOp::Set(i)).unwrap();
    }
    w.flush().unwrap();
    drop(w);

    let result = recover_with_wal::<(), CountOp, _>(
        &dir,
        None,
        RecoveryOptions::up_to(5),
        |_| 0u64,
        |state, _id, entry| {
            if let CountOp::Set(v) = entry {
                *state = v;
            }
        },
    )
    .unwrap();

    assert_eq!(result.state, 5, "should have applied entries 1..=5");
    assert_eq!(result.last_entry_id, 5);
}

#[test]
fn point_in_time_recovery_with_no_ceiling_applies_all() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<CountOp>::new(dir.clone());
    for _ in 0..7 {
        w.append(&CountOp::Inc).unwrap();
    }
    w.flush().unwrap();
    drop(w);

    let result = recover_with_wal::<(), CountOp, _>(
        &dir,
        None,
        RecoveryOptions::strict(),
        |_| 0u64,
        |state, _id, entry| {
            if let CountOp::Inc = entry {
                *state += 1;
            }
        },
    )
    .unwrap();

    assert_eq!(result.state, 7);
    assert_eq!(result.last_entry_id, 7);
}

#[test]
fn point_in_time_recovery_ceiling_zero_applies_nothing() {
    let dir = MemoryDirectory::arc();

    let mut w = WalWriter::<CountOp>::new(dir.clone());
    for _ in 0..5 {
        w.append(&CountOp::Inc).unwrap();
    }
    w.flush().unwrap();
    drop(w);

    let result = recover_with_wal::<(), CountOp, _>(
        &dir,
        None,
        RecoveryOptions::up_to(0),
        |_| 0u64,
        |state, _id, entry| {
            if let CountOp::Inc = entry {
                *state += 1;
            }
        },
    )
    .unwrap();

    assert_eq!(result.state, 0, "ceiling 0 should apply no entries");
    assert_eq!(result.last_entry_id, 0);
}
