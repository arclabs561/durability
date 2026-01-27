//! Metamorphic/property test: WAL segmentation boundaries must not change semantics.
//!
//! We build the same logical WAL stream under different segmentations and assert that
//! strict replay yields identical entry sequences.

use durability::formats::{FORMAT_VERSION, WAL_MAGIC};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalSegmentHeader};
use proptest::prelude::*;
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

fn arb_entries() -> impl Strategy<Value = Vec<WalEntry>> {
    prop::collection::vec(
        prop_oneof![
            (1u64..80u64, 0u32..500u32).prop_map(|(seg, dc)| WalEntry::AddSegment {
                entry_id: 0,
                segment_id: seg,
                doc_count: dc,
            }),
            (1u64..80u64, 0u32..500u32).prop_map(|(seg, doc)| WalEntry::DeleteDocuments {
                entry_id: 0,
                deletes: vec![(seg, doc)],
            }),
        ],
        1..200,
    )
}

fn write_segments(dir: &Arc<dyn Directory>, entries: &[WalEntry], cut_points: &[usize]) {
    dir.create_dir_all("wal").unwrap();

    // Build slices according to cut points.
    let mut segs: Vec<&[WalEntry]> = Vec::new();
    let mut start = 0usize;
    for &c in cut_points {
        let c = c.min(entries.len());
        if c > start {
            segs.push(&entries[start..c]);
            start = c;
        }
    }
    if start < entries.len() {
        segs.push(&entries[start..]);
    }
    if segs.is_empty() {
        return;
    }

    for (i, seg_entries) in segs.iter().enumerate() {
        let seg_id = (i as u64) + 1;
        let start_entry_id = entry_id(&seg_entries[0]);

        let mut bytes = Vec::new();
        WalSegmentHeader {
            magic: WAL_MAGIC,
            version: FORMAT_VERSION,
            start_entry_id,
            segment_id: seg_id,
        }
        .write(&mut bytes)
        .unwrap();

        for e in *seg_entries {
            let enc = WalEntryOnDisk::encode(e).unwrap();
            bytes.extend_from_slice(&enc);
        }
        dir.atomic_write(&format!("wal/wal_{seg_id}.log"), &bytes)
            .unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 128,
        .. ProptestConfig::default()
    })]

    #[test]
    fn wal_segmentation_does_not_change_replay(entries in arb_entries(),
                                               mut cuts_a in prop::collection::vec(0usize..200, 0..25),
                                               mut cuts_b in prop::collection::vec(0usize..200, 0..25)) {
        // Rewrite entry ids to strict increasing to satisfy WAL invariants.
        let mut entries = entries;
        for (i, e) in entries.iter_mut().enumerate() {
            let id = (i as u64) + 1;
            match e {
                WalEntry::AddSegment{entry_id, ..}
                | WalEntry::StartMerge{entry_id, ..}
                | WalEntry::CancelMerge{entry_id, ..}
                | WalEntry::EndMerge{entry_id, ..}
                | WalEntry::DeleteDocuments{entry_id, ..}
                | WalEntry::Checkpoint{entry_id, ..} => *entry_id = id,
            }
        }

        // Normalize cut points to be sorted and in-range.
        for c in cuts_a.iter_mut() { *c = (*c).min(entries.len()); }
        for c in cuts_b.iter_mut() { *c = (*c).min(entries.len()); }
        cuts_a.sort_unstable();
        cuts_b.sort_unstable();

        // Scenario A
        let tmp_a = tempfile::tempdir().unwrap();
        let dir_a: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp_a.path()).unwrap());
        write_segments(&dir_a, &entries, &cuts_a);
        let out_a = WalReader::new(dir_a).replay().unwrap();

        // Scenario B
        let tmp_b = tempfile::tempdir().unwrap();
        let dir_b: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp_b.path()).unwrap());
        write_segments(&dir_b, &entries, &cuts_b);
        let out_b = WalReader::new(dir_b).replay().unwrap();

        prop_assert_eq!(&out_a, &entries);
        prop_assert_eq!(&out_b, &entries);
        // Metamorphic assertion: different segmentation => identical replay stream.
        prop_assert_eq!(out_a, out_b);
    }
}
