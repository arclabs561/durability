//! E2E tests for WAL segment preallocation.
#![cfg(feature = "postcard")]

use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;

#[test]
fn preallocated_segment_is_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    w.set_preallocate_bytes(64 * 1024); // 64 KiB

    for i in 1..=10u64 {
        w.append(&WalEntry::AddSegment {
            segment_id: i,
            doc_count: i as u32 * 10,
        })
        .unwrap();
    }
    w.flush().unwrap();
    drop(w); // drop truncates preallocated space

    let r = WalReader::<WalEntry>::new(dir.clone());
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 10);
    assert_eq!(records[0].entry_id, 1);
    assert_eq!(records[9].entry_id, 10);

    // Verify file is truncated to actual size (not 64 KiB).
    let segments = dir.list_dir("wal").unwrap();
    for seg in segments.iter().filter(|s| s.ends_with(".log")) {
        let path = format!("wal/{seg}");
        let fs_path = dir.file_path(&path).unwrap();
        let size = std::fs::metadata(&fs_path).unwrap().len();
        assert!(
            size < 64 * 1024,
            "segment {seg} should be truncated but is {size} bytes"
        );
    }
}

#[test]
fn preallocated_segment_rotation_produces_correct_files() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut w = WalWriter::<WalEntry>::with_options(
        dir.clone(),
        durability::storage::FlushPolicy::PerAppend,
        0,
    );
    w.set_segment_size_limit_bytes(128); // tiny segments to force rotation
    w.set_preallocate_bytes(4096);

    for i in 1..=50u64 {
        w.append(&WalEntry::AddSegment {
            segment_id: i,
            doc_count: i as u32,
        })
        .unwrap();
    }
    w.flush().unwrap();
    drop(w);

    // Should have multiple segments.
    let segments = dir.list_dir("wal").unwrap();
    let log_files: Vec<_> = segments.iter().filter(|s| s.ends_with(".log")).collect();
    assert!(
        log_files.len() > 1,
        "expected multiple segments, got {}",
        log_files.len()
    );

    // All entries should replay correctly.
    let r = WalReader::<WalEntry>::new(dir.clone());
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 50);

    // Verify no segment is still at preallocated size.
    for seg in &log_files {
        let path = format!("wal/{seg}");
        let fs_path = dir.file_path(&path).unwrap();
        let size = std::fs::metadata(&fs_path).unwrap().len();
        assert!(
            size < 4096,
            "segment {seg} not truncated: {size} bytes (prealloc was 4096)"
        );
    }
}

#[test]
fn preallocation_disabled_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    // Don't set preallocation -- default is 0 (disabled).
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 1,
    })
    .unwrap();
    w.flush().unwrap();

    // Segment should be small (header + one entry).
    let segments = dir.list_dir("wal").unwrap();
    let seg = segments.iter().find(|s| s.ends_with(".log")).unwrap();
    let path = format!("wal/{seg}");
    let fs_path = dir.file_path(&path).unwrap();
    let size = std::fs::metadata(&fs_path).unwrap().len();
    assert!(
        size < 200,
        "segment should be small without preallocation, got {size} bytes"
    );
    drop(w);
}

#[test]
fn resume_after_preallocated_write() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    {
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        w.set_preallocate_bytes(32 * 1024);
        for i in 1..=5u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i,
                doc_count: i as u32,
            })
            .unwrap();
        }
        w.flush().unwrap();
        // drop truncates preallocated space
    }

    // resume should work on the truncated file.
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

    let r = WalReader::<WalEntry>::new(dir);
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 6);
}
