//! Crash-consistency: truncate a record log at every byte offset and verify
//! recovery is always a clean committed prefix.
//!
//! The durability contract: BestEffort recovery returns the longest valid PREFIX
//! of committed records, never a corrupted record, never a panic, never an error.
//! This exhaustively simulates a crash at every offset, the integration-level
//! property per-record unit tests do not cover.
#![cfg(feature = "postcard")]

use std::fs::OpenOptions;

use durability::recordlog::{RecordLogReadMode, RecordLogReader, RecordLogWriter};
use durability::storage::FsDirectory;

#[test]
fn truncation_at_every_offset_recovers_clean_prefix() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = FsDirectory::arc(tmp.path()).expect("fs directory");
    let path = "log.bin";

    const N: usize = 100;
    let committed: Vec<String> = (0..N).map(|i| format!("record-{i:05}-payload")).collect();
    {
        let mut w = RecordLogWriter::new(dir.clone(), path);
        for r in &committed {
            w.append_postcard(r).expect("append");
        }
        w.flush_and_sync().expect("flush_and_sync");
    }

    let file = tmp.path().join(path);
    let full_len = std::fs::metadata(&file).expect("metadata").len();

    let mut max_recovered = 0usize;
    for len in (0..=full_len).rev() {
        OpenOptions::new()
            .write(true)
            .open(&file)
            .expect("open")
            .set_len(len)
            .expect("set_len");

        let reader = RecordLogReader::new(dir.clone(), path);
        let recovered = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reader.read_all_postcard::<String>(RecordLogReadMode::BestEffort)
        })) {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => panic!("BestEffort errored on torn tail at len {len}: {e:?}"),
            Err(_) => panic!("recovery panicked at truncation len {len}"),
        };

        max_recovered = max_recovered.max(recovered.len());
        assert!(
            recovered.len() <= committed.len(),
            "recovered more than committed at len {len}"
        );
        for (i, r) in recovered.iter().enumerate() {
            assert_eq!(
                r, &committed[i],
                "record {i} corrupted at truncation len {len}"
            );
        }
    }

    assert_eq!(max_recovered, N, "full-length recovery dropped records");
}
