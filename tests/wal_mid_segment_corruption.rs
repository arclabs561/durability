//! A corrupt length field in the middle of the last WAL segment must not be
//! treated as a torn tail.
//!
//! Raising one record's length makes its payload appear to run past EOF,
//! which used to look exactly like a torn final write. `resume` then cut the
//! file back to that record, deleting every later synced record and reusing
//! their ids. A torn tail has no valid record after it; mid-segment
//! corruption does, so resume must refuse and leave the file untouched.
#![cfg(feature = "postcard")]

use durability::storage::{Directory, MemoryDirectory};

type Dir = Arc<dyn Directory>;
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::io::Read;
use std::sync::Arc;

const SEGMENT_HEADER_SIZE: usize = 24;

fn write_entries(dir: &Dir, n: u64) -> String {
    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    for i in 0..n {
        w.append(&WalEntry::AddSegment {
            segment_id: i,
            doc_count: 10,
        })
        .unwrap();
    }
    w.flush().unwrap();
    drop(w);
    let files = dir.list_dir("wal").unwrap();
    let wal_file = files.iter().find(|f| f.ends_with(".log")).unwrap();
    format!("wal/{wal_file}")
}

fn read(dir: &Dir, path: &str) -> Vec<u8> {
    let mut data = Vec::new();
    dir.open_file(path).unwrap().read_to_end(&mut data).unwrap();
    data
}

/// Byte offset of the `index`-th frame, walking the length prefixes.
fn frame_offset(data: &[u8], index: usize) -> usize {
    let mut off = SEGMENT_HEADER_SIZE;
    for _ in 0..index {
        let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        off += len;
    }
    off
}

#[test]
fn resume_refuses_to_truncate_when_valid_records_follow_a_corrupt_length() {
    let dir = MemoryDirectory::arc();
    let wal_path = write_entries(&dir, 10);
    let mut data = read(&dir, &wal_path);

    // Flip bit 16 of record 3's length: its payload now "runs past EOF".
    let off = frame_offset(&data, 3);
    data[off + 2] ^= 0x01;
    dir.atomic_write(&wal_path, &data).unwrap();

    let result = WalWriter::<WalEntry>::resume(dir.clone());
    assert!(
        result.is_err(),
        "resume must report mid-segment corruption, not truncate it as a torn tail"
    );
    assert_eq!(
        read(&dir, &wal_path),
        data,
        "resume must not rewrite the WAL when it refuses"
    );
}

#[test]
fn resume_still_repairs_a_genuinely_torn_tail() {
    let dir = MemoryDirectory::arc();
    let wal_path = write_entries(&dir, 10);
    let data = read(&dir, &wal_path);

    // Cut the last record in half: nothing valid follows the damage.
    let last = frame_offset(&data, 9);
    let torn = &data[..last + (data.len() - last) / 2];
    dir.atomic_write(&wal_path, torn).unwrap();

    let mut w = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
    w.append(&WalEntry::AddSegment {
        segment_id: 99,
        doc_count: 1,
    })
    .unwrap();
    w.flush().unwrap();
    drop(w);

    let entries = WalReader::<WalEntry>::new(dir).replay().unwrap();
    assert_eq!(entries.len(), 10, "9 surviving records plus the new one");
}
