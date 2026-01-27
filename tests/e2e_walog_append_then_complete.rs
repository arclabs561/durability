//! E2E test: simulate a crash mid-record, then later completion.
//!
//! This models “tailing”/retry behavior: best-effort should return the valid prefix
//! while the record is torn, and strict replay should succeed once the record is completed.

use durability::formats::{FORMAT_VERSION, WAL_MAGIC};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalSegmentHeader};
use std::io::Write;
use std::sync::Arc;

#[test]
fn wal_append_then_complete_allows_eventual_strict_replay() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::new(tmp.path()).unwrap();
    let dir: Arc<dyn Directory> = Arc::new(dir);
    dir.create_dir_all("wal").unwrap();

    // Create a single segment file manually.
    let seg_id = 1u64;
    let seg_path = "wal/wal_1.log";

    // Header + one committed entry.
    let entry1 = WalEntry::AddSegment {
        entry_id: 1,
        segment_id: 7,
        doc_count: 3,
    };
    let bytes1 = WalEntryOnDisk::encode(&entry1).unwrap();

    // Second entry that we'll write partially, then complete.
    let entry2 = WalEntry::DeleteDocuments {
        entry_id: 2,
        deletes: vec![(7, 1)],
    };
    let bytes2 = WalEntryOnDisk::encode(&entry2).unwrap();

    let mut file_bytes = Vec::new();
    WalSegmentHeader {
        magic: WAL_MAGIC,
        version: FORMAT_VERSION,
        start_entry_id: 1,
        segment_id: seg_id,
    }
    .write(&mut file_bytes)
    .unwrap();
    file_bytes.extend_from_slice(&bytes1);

    // Write header + entry1 + only a prefix of entry2 (torn record).
    let partial_len = 3.min(bytes2.len()); // ensure we tear inside the length prefix / header
    file_bytes.extend_from_slice(&bytes2[..partial_len]);
    dir.atomic_write(seg_path, &file_bytes).unwrap();

    let r = WalReader::new(dir.clone());
    assert!(r.replay().is_err());
    let prefix = r.replay_best_effort().unwrap();
    assert_eq!(prefix.len(), 1);

    // Now “complete” the record by appending the remaining bytes.
    let Some(fs_path) = dir.file_path(seg_path) else {
        panic!("FsDirectory must return file_path()");
    };
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&fs_path)
        .unwrap();
    f.write_all(&bytes2[partial_len..]).unwrap();
    f.flush().unwrap();

    // Now strict replay should succeed and return both entries.
    let out = r.replay().unwrap();
    assert_eq!(out.len(), 2);
}
