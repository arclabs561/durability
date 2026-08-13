//! Exhaustive byte-cut tests for WAL batch crash semantics.
#![cfg(feature = "postcard")]

use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalSegmentHeader, WalWriter};
use std::io::Read;
use std::sync::Arc;

fn batch() -> Vec<WalEntry> {
    vec![
        WalEntry::AddSegment {
            segment_id: 11,
            doc_count: 101,
        },
        WalEntry::AddSegment {
            segment_id: 22,
            doc_count: 202,
        },
        WalEntry::DeleteDocuments {
            deletes: vec![(11, 3), (22, 7)],
        },
    ]
}

#[test]
fn batch_byte_cut_recovery_is_a_prefix_not_a_transaction() {
    let source_tmp = tempfile::tempdir().unwrap();
    let source = Arc::new(FsDirectory::new(source_tmp.path()).unwrap());
    {
        let mut writer = WalWriter::new(source.clone() as Arc<dyn Directory>);
        writer.append_batch(&batch()).unwrap();
    }

    let segment_name = source
        .list_dir("wal")
        .unwrap()
        .into_iter()
        .find(|name| name.ends_with(".log"))
        .unwrap();
    let segment_path = format!("wal/{segment_name}");
    let mut complete = Vec::new();
    source
        .open_file(&segment_path)
        .unwrap()
        .read_to_end(&mut complete)
        .unwrap();

    let expected = batch();
    let mut observed_lengths = std::collections::BTreeSet::new();

    // Every possible persisted prefix at or after the complete segment header
    // models a crash/torn tail at that byte boundary.
    for cut in WalSegmentHeader::SIZE..=complete.len() {
        let crash_tmp = tempfile::tempdir().unwrap();
        let crash_dir = Arc::new(FsDirectory::new(crash_tmp.path()).unwrap());
        crash_dir.create_dir_all("wal").unwrap();
        crash_dir
            .atomic_write(&segment_path, &complete[..cut])
            .unwrap();

        let recovered = WalReader::<WalEntry>::new(crash_dir as Arc<dyn Directory>)
            .replay_best_effort()
            .unwrap();
        observed_lengths.insert(recovered.len());

        assert!(recovered.len() <= expected.len(), "cut={cut}");
        for (index, record) in recovered.iter().enumerate() {
            assert_eq!(record.entry_id, index as u64 + 1, "cut={cut}");
            assert_eq!(record.payload, expected[index], "cut={cut}");
        }
    }

    assert_eq!(
        observed_lengths,
        [0, 1, 2, 3].into_iter().collect(),
        "a recovered batch can be absent, partial, or complete depending on the crash boundary"
    );
}
