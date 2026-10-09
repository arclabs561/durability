//! A final WAL segment cut inside its 24-byte header is a torn tail.
//!
//! A crash right after a segment file is created can leave fewer bytes than
//! the segment header. That segment holds no acknowledged record, so
//! best-effort replay must return the earlier records, and `resume` must be
//! able to repair it and keep appending.
#![cfg(feature = "postcard")]

use durability::storage::{Directory, MemoryDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::io::Read;
use std::sync::Arc;

type Dir = Arc<dyn Directory>;

fn entry(i: u64) -> WalEntry {
    WalEntry::AddSegment {
        segment_id: i,
        doc_count: 1,
    }
}

/// Two segments with entries 1..=3 in the first and 4 in the last.
fn two_segments(dir: &Dir) -> String {
    let mut w = WalWriter::<WalEntry>::new(dir.clone());
    for i in 0..3 {
        w.append(&entry(i)).unwrap();
    }
    w.flush().unwrap();
    w.set_segment_size_limit_bytes(1);
    w.append(&entry(3)).unwrap();
    w.flush().unwrap();
    drop(w);
    let mut files: Vec<String> = dir
        .list_dir("wal")
        .unwrap()
        .into_iter()
        .filter(|f| f.ends_with(".log"))
        .collect();
    files.sort();
    assert_eq!(files.len(), 2, "expected two segments, got {files:?}");
    format!("wal/{}", files[1])
}

fn cut(dir: &Dir, path: &str, len: usize) {
    let mut data = Vec::new();
    dir.open_file(path).unwrap().read_to_end(&mut data).unwrap();
    dir.atomic_write(path, &data[..len]).unwrap();
}

#[test]
fn best_effort_replay_ignores_a_last_segment_shorter_than_its_header() {
    for len in 0..24 {
        let dir = MemoryDirectory::arc();
        let last = two_segments(&dir);
        cut(&dir, &last, len);
        let ids: Vec<u64> = WalReader::<WalEntry>::new(dir.clone())
            .replay_best_effort()
            .unwrap_or_else(|e| panic!("cut at {len}: {e}"))
            .iter()
            .map(|r| r.entry_id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3], "cut at {len}");
    }
}

#[test]
fn resume_then_append_after_a_last_segment_shorter_than_its_header() {
    for len in 0..24 {
        let dir = MemoryDirectory::arc();
        let last = two_segments(&dir);
        cut(&dir, &last, len);
        let mut w = WalWriter::<WalEntry>::resume(dir.clone())
            .unwrap_or_else(|e| panic!("cut at {len}: resume: {e}"));
        w.append(&entry(9)).unwrap();
        w.flush().unwrap();
        drop(w);
        let ids: Vec<u64> = WalReader::<WalEntry>::new(dir.clone())
            .replay()
            .unwrap_or_else(|e| panic!("cut at {len}: replay: {e}"))
            .iter()
            .map(|r| r.entry_id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4], "cut at {len}");
    }
}
