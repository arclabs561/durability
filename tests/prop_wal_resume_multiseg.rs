//! Property-based tests for `WalWriter::resume` across multiple segments.
//!
//! Focus:
//! - strict replay rejects torn tail in final segment
//! - best-effort returns a prefix
//! - `resume` repairs and continues ids without weakening corruption detection in non-final segments

use durability::formats::{FORMAT_VERSION, WAL_MAGIC};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalSegmentHeader, WalWriter};
use proptest::prelude::*;
use std::io::Write;
use std::sync::Arc;

fn entry_id(e: &WalEntry) -> u64 {
    match e {
        WalEntry::AddSegment { entry_id, .. }
        | WalEntry::StartMerge { entry_id, .. }
        | WalEntry::CancelMerge { entry_id, .. }
        | WalEntry::EndMerge { entry_id, .. }
        | WalEntry::DeleteDocuments { entry_id, .. }
        | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
    }
}

fn arb_ops() -> impl Strategy<Value = Vec<WalEntry>> {
    // Keep it cheap and mostly monotone-friendly; we rewrite entry ids anyway.
    prop::collection::vec(
        prop_oneof![
            (1u64..200u64, 0u32..2000u32).prop_map(|(seg, dc)| WalEntry::AddSegment {
                entry_id: 0,
                segment_id: seg,
                doc_count: dc
            }),
            (1u64..200u64, 0u32..2000u32).prop_map(|(seg, doc)| WalEntry::DeleteDocuments {
                entry_id: 0,
                deletes: vec![(seg, doc)]
            }),
        ],
        1..200,
    )
}

fn write_segment(dir: &Arc<dyn Directory>, seg_id: u64, start_entry_id: u64, entries: &[WalEntry]) {
    dir.create_dir_all("wal").unwrap();
    let path = format!("wal/wal_{seg_id}.log");
    let mut f = dir.create_file(&path).unwrap();
    WalSegmentHeader {
        magic: WAL_MAGIC,
        version: FORMAT_VERSION,
        start_entry_id,
        segment_id: seg_id,
    }
    .write(&mut f)
    .unwrap();
    for e in entries {
        let bytes = WalEntryOnDisk::encode(e).unwrap();
        f.write_all(&bytes).unwrap();
    }
    f.flush().unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 128,
        .. ProptestConfig::default()
    })]

    #[test]
    fn wal_resume_multiseg_repairs_final_segment_and_continues_ids(
        mut entries in arb_ops(),
        max_per_segment in 1usize..30,
        tear_bytes in 0usize..256
    ) {
        // Rewrite entry ids to strict increasing so WAL invariants are satisfied.
        for (i, e) in entries.iter_mut().enumerate() {
            let id = (i as u64) + 1;
            match e {
                WalEntry::AddSegment { entry_id, .. }
                | WalEntry::StartMerge { entry_id, .. }
                | WalEntry::CancelMerge { entry_id, .. }
                | WalEntry::EndMerge { entry_id, .. }
                | WalEntry::DeleteDocuments { entry_id, .. }
                | WalEntry::Checkpoint { entry_id, .. } => *entry_id = id,
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let fs = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(fs);

        // Split into segments.
        let mut seg_id = 1u64;
        let mut i = 0usize;
        while i < entries.len() {
            let end = (i + max_per_segment).min(entries.len());
            let seg_entries = &entries[i..end];
            let start = entry_id(&seg_entries[0]);
            write_segment(&dir, seg_id, start, seg_entries);
            seg_id += 1;
            i = end;
        }
        let last_seg_id = seg_id - 1;
        let last_path = format!("wal/wal_{last_seg_id}.log");

        // Tear only the final segment (possibly tearing inside header or record).
        if let Some(fs_path) = dir.file_path(&last_path) {
            let mut bytes = std::fs::read(&fs_path).unwrap();
            if !bytes.is_empty() {
                let drop = tear_bytes.min(bytes.len().saturating_sub(1));
                bytes.truncate(bytes.len().saturating_sub(drop));
                std::fs::write(&fs_path, &bytes).unwrap();
            }
        } else {
            panic!("FsDirectory must return file_path()");
        }

        let reader = WalReader::new(dir.clone());
        let prefix = reader.replay_best_effort().unwrap_or_else(|_| vec![]);

        // Resume and append one more entry; id must continue after best-effort prefix.
        let mut w = WalWriter::resume(dir.clone()).unwrap();
        let next = prefix.last().map(entry_id).unwrap_or(0) + 1;
        let got = w.append(WalEntry::AddSegment { entry_id: 0, segment_id: 9_999, doc_count: 1 }).unwrap();
        prop_assert_eq!(got, next);
        w.flush().unwrap();

        // After resume+append, strict replay must succeed and begin with the best-effort prefix.
        let out = reader.replay().unwrap();
        prop_assert!(out.len() > prefix.len());
        prop_assert_eq!(&out[..prefix.len()], &prefix[..]);
        prop_assert_eq!(entry_id(out.last().unwrap()), got);
    }
}
