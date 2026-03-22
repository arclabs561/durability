//! Metamorphic/property test: WAL segmentation boundaries must not change semantics.
//!
//! We build the same logical WAL stream under different segmentations and assert that
//! strict replay yields identical entry sequences.

use durability::formats::{WAL_FORMAT_VERSION, WAL_MAGIC};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalRecord, WalSegmentHeader};
use proptest::prelude::*;
use std::sync::Arc;

fn arb_entries() -> impl Strategy<Value = Vec<WalEntry>> {
    prop::collection::vec(
        prop_oneof![
            (1u64..80u64, 0u32..500u32).prop_map(|(seg, dc)| WalEntry::AddSegment {
                segment_id: seg,
                doc_count: dc,
            }),
            (1u64..80u64, 0u32..500u32).prop_map(|(seg, doc)| WalEntry::DeleteDocuments {
                deletes: vec![(seg, doc)],
            }),
        ],
        1..200,
    )
}

fn write_segments(
    dir: &Arc<dyn Directory>,
    entries: &[WalEntry],
    entry_ids: &[u64],
    cut_points: &[usize],
) {
    dir.create_dir_all("wal").unwrap();

    // Build slices according to cut points.
    let mut segs: Vec<(usize, usize)> = Vec::new();
    let mut start = 0usize;
    for &c in cut_points {
        let c = c.min(entries.len());
        if c > start {
            segs.push((start, c));
            start = c;
        }
    }
    if start < entries.len() {
        segs.push((start, entries.len()));
    }
    if segs.is_empty() {
        return;
    }

    for (i, &(seg_start, seg_end)) in segs.iter().enumerate() {
        let seg_id = (i as u64) + 1;
        let start_entry_id = entry_ids[seg_start];

        let mut bytes = Vec::new();
        WalSegmentHeader {
            magic: WAL_MAGIC,
            version: WAL_FORMAT_VERSION,
            start_entry_id,
            segment_id: seg_id,
        }
        .write(&mut bytes)
        .unwrap();

        for idx in seg_start..seg_end {
            let enc = WalEntryOnDisk::encode(entry_ids[idx], &entries[idx]).unwrap();
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
        // Assign strict increasing entry ids.
        let entry_ids: Vec<u64> = (1..=(entries.len() as u64)).collect();

        // Normalize cut points to be sorted and in-range.
        for c in cuts_a.iter_mut() { *c = (*c).min(entries.len()); }
        for c in cuts_b.iter_mut() { *c = (*c).min(entries.len()); }
        cuts_a.sort_unstable();
        cuts_b.sort_unstable();

        // Scenario A
        let tmp_a = tempfile::tempdir().unwrap();
        let dir_a: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp_a.path()).unwrap());
        write_segments(&dir_a, &entries, &entry_ids, &cuts_a);
        let out_a: Vec<WalRecord<WalEntry>> = WalReader::new(dir_a).replay().unwrap();

        // Scenario B
        let tmp_b = tempfile::tempdir().unwrap();
        let dir_b: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp_b.path()).unwrap());
        write_segments(&dir_b, &entries, &entry_ids, &cuts_b);
        let out_b: Vec<WalRecord<WalEntry>> = WalReader::new(dir_b).replay().unwrap();

        // Compare payloads.
        let payloads_a: Vec<&WalEntry> = out_a.iter().map(|r| &r.payload).collect();
        let payloads_b: Vec<&WalEntry> = out_b.iter().map(|r| &r.payload).collect();
        prop_assert_eq!(&payloads_a, &entries.iter().collect::<Vec<_>>());
        prop_assert_eq!(&payloads_b, &entries.iter().collect::<Vec<_>>());
        // Metamorphic assertion: different segmentation => identical replay stream.
        prop_assert_eq!(payloads_a, payloads_b);
    }
}
