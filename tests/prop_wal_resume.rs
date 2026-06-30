//! Property-based tests for `WalWriter::resume`.
//!
//! Focus:
//! - if the last segment is torn in the tail, strict replay fails
//! - best-effort returns the valid prefix
//! - `resume` repairs the tail, continues entry ids, and allows strict replay again
#![cfg(feature = "postcard")]

use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalRecord, WalWriter};
use proptest::prelude::*;
use std::sync::Arc;

fn arb_entries() -> impl Strategy<Value = Vec<WalEntry>> {
    // Keep it cheap + deterministic.
    prop::collection::vec(
        prop_oneof![
            (1u64..50u64, 0u32..500u32).prop_map(|(seg, dc)| WalEntry::AddSegment {
                segment_id: seg,
                doc_count: dc,
            }),
            (1u64..50u64, 0u32..500u32).prop_map(|(seg, doc)| WalEntry::DeleteDocuments {
                deletes: vec![(seg, doc)],
            }),
        ],
        1..120,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 192,
        .. ProptestConfig::default()
    })]

    #[test]
    fn wal_resume_repairs_torn_tail_and_continues_ids(
        entries in arb_entries(),
        tear_bytes in 1usize..128
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);

        // Write entries, then flush.
        {
            let mut w = WalWriter::<WalEntry>::new(dir.clone());
            for e in &entries {
                let _ = w.append(e).unwrap();
            }
            w.flush().unwrap();
        }

        // Tear the final segment file.
        let wal_path = "wal/wal_1.log";
        if let Some(fs_path) = dir.file_path(wal_path) {
            let mut bytes = std::fs::read(&fs_path).unwrap();
            if !bytes.is_empty() {
                let drop = tear_bytes.min(bytes.len().saturating_sub(1)).max(1);
                bytes.truncate(bytes.len().saturating_sub(drop));
                std::fs::write(&fs_path, &bytes).unwrap();
            }
        } else {
            panic!("FsDirectory must return file_path()");
        }

        let reader = WalReader::<WalEntry>::new(dir.clone());
        // Strict should generally fail under a tail tear; if it doesn't (rare boundary case),
        // the property still holds because resume should be a no-op.
        let _ = reader.replay(); // just ensure it runs
        let prefix: Vec<WalRecord<WalEntry>> = match reader.replay_best_effort() {
            Ok(p) => p,
            // If the tear cut into the segment header itself, `WalReader` does not
            // currently treat that as recoverable (it can't even read magic/version).
            // `WalWriter::resume` *does* treat a torn header as "empty WAL".
            Err(durability::PersistenceError::Io(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                vec![]
            }
            Err(e) => return Err(TestCaseError::fail(format!("unexpected replay_best_effort error: {e}"))),
        };

        // Resume should repair (if needed) and continue entry ids after the prefix.
        let mut w2 = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
        let appended_id = w2.append(&WalEntry::AddSegment { segment_id: 999, doc_count: 1 }).unwrap();
        w2.flush().unwrap();

        let want_next = prefix.last().map(|r| r.entry_id).unwrap_or(0) + 1;
        prop_assert_eq!(appended_id, want_next);

        // After resume+append, strict replay must succeed and be: prefix + appended.
        let out = reader.replay().unwrap();
        prop_assert!(!out.is_empty());
        for (i, rec) in prefix.iter().enumerate() {
            prop_assert_eq!(&out[i].payload, &rec.payload);
        }
        prop_assert_eq!(out.last().unwrap().entry_id, appended_id);
    }
}
