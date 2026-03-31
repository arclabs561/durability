//! Tests derived from WAL implementation research (2026-03-30).
//!
//! Patterns sourced from okaywal, growth-ring, and seglog.

use durability::storage::{Directory, FlushPolicy, FsDirectory, MemoryDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalSegmentHeader, WalWriter};
use std::io::Write;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Pattern 5: Rapid segment rotation stress (from okaywal always_checkpointing)
// ---------------------------------------------------------------------------

#[test]
fn rapid_segment_rotation_preserves_all_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    let mut w = WalWriter::<WalEntry>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    // Tiny segment limit: forces rotation every 1-2 entries.
    w.set_segment_size_limit_bytes(64);

    for i in 1..=100u64 {
        w.append(&WalEntry::AddSegment {
            segment_id: i,
            doc_count: i as u32,
        })
        .unwrap();
    }
    drop(w);

    let segments = dir.list_dir("wal").unwrap();
    let log_count = segments.iter().filter(|s| s.ends_with(".log")).count();
    assert!(
        log_count >= 10,
        "expected many segments from tiny limit, got {log_count}"
    );

    let r = WalReader::<WalEntry>::new(dir);
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 100);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.entry_id, (i as u64) + 1);
    }
}

// ---------------------------------------------------------------------------
// Pattern 4: Aborted entry after valid commit (from okaywal)
// ---------------------------------------------------------------------------

#[test]
fn aborted_entry_after_valid_is_discarded_by_best_effort() {
    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

    // Write one valid entry via the writer.
    let mut w = WalWriter::<WalEntry>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.append(&WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 10,
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    // Now manually append a partial frame (length header only, no CRC or payload).
    // This simulates a crash after writing the length prefix of the next entry.
    {
        let mut f = dir.append_file("wal/wal_1.log").unwrap();
        // Write a valid-looking length (100 bytes) but nothing after it.
        f.write_all(&100u32.to_le_bytes()).unwrap();
        f.flush().unwrap();
    }

    // Strict replay should fail (truncated entry).
    let r = WalReader::<WalEntry>::new(dir.clone());
    assert!(r.replay().is_err());

    // Best-effort replay should recover only the valid entry.
    let records = r.replay_best_effort().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].entry_id, 1);
}

// ---------------------------------------------------------------------------
// Pattern 6: All-zeros payload roundtrip
// ---------------------------------------------------------------------------

#[test]
fn all_zeros_payload_roundtrips_correctly() {
    // A payload that serializes to all zeros should not be confused with
    // EOF/truncation markers or preallocation padding.
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct ZeroPayload {
        a: u64,
        b: u64,
    }

    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
    let mut w = WalWriter::<ZeroPayload>::new(dir.clone());
    w.append(&ZeroPayload { a: 0, b: 0 }).unwrap();
    w.append(&ZeroPayload { a: 0, b: 0 }).unwrap();
    w.flush().unwrap();
    drop(w);

    let r = WalReader::<ZeroPayload>::new(dir);
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, ZeroPayload { a: 0, b: 0 });
    assert_eq!(records[1].payload, ZeroPayload { a: 0, b: 0 });
}

// ---------------------------------------------------------------------------
// Pattern: Open/close cycling (write one entry, close, reopen, repeat)
// ---------------------------------------------------------------------------

#[test]
fn open_close_cycling_preserves_all_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    // First write with new().
    {
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        w.flush().unwrap();
    }

    // Then 19 more writes, each with resume().
    for i in 2..=20u64 {
        let mut w = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
        w.append(&WalEntry::AddSegment {
            segment_id: i,
            doc_count: i as u32,
        })
        .unwrap();
        w.flush().unwrap();
    }

    let r = WalReader::<WalEntry>::new(dir);
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 20);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.entry_id, (i as u64) + 1);
    }
}

// ---------------------------------------------------------------------------
// Pattern: Segment boundary entry (entry that exactly fills a segment)
// ---------------------------------------------------------------------------

#[test]
fn entry_at_exact_segment_boundary() {
    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

    // Figure out the size of one entry to set the limit precisely.
    let test_entry = WalEntry::AddSegment {
        segment_id: 1,
        doc_count: 1,
    };
    let encoded = WalEntryOnDisk::encode(1, &test_entry).unwrap();
    let entry_size = encoded.len() as u64;
    let header_size = WalSegmentHeader::SIZE as u64;

    // Set limit to exactly header + one entry. The second entry must trigger rotation.
    let limit = header_size + entry_size;

    let mut w = WalWriter::<WalEntry>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(limit);

    for i in 1..=5u64 {
        w.append(&WalEntry::AddSegment {
            segment_id: i,
            doc_count: i as u32,
        })
        .unwrap();
    }
    w.flush().unwrap();
    drop(w);

    // Should have multiple segments (first has 1 entry, rest have 1 each).
    let segments = dir.list_dir("wal").unwrap();
    let log_count = segments.iter().filter(|s| s.ends_with(".log")).count();
    assert!(
        log_count >= 2,
        "expected multiple segments at exact boundary, got {log_count}"
    );

    let r = WalReader::<WalEntry>::new(dir);
    let records = r.replay().unwrap();
    assert_eq!(records.len(), 5);
}
