//! Property tests for WAL prefix recycling followed by resume.
//!
//! The important invariant is not just that recycled files can be reused, but
//! that after the recycle pool has been consumed, replay and resume observe a
//! clean contiguous WAL suffix with freshly-written recycled segment headers.

use durability::storage::{Directory, FlushPolicy, FsDirectory};
use durability::walog::{WalEntry, WalMaintenance, WalReader, WalWriter};
use proptest::prelude::*;
use std::sync::Arc;

fn add_segment(segment_id: u64, doc_count: u32) -> WalEntry {
    WalEntry::AddSegment {
        segment_id,
        doc_count,
    }
}

fn payloads(records: &[durability::walog::WalRecord<WalEntry>]) -> Vec<WalEntry> {
    records
        .iter()
        .map(|record| record.payload.clone())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 96,
        .. ProptestConfig::default()
    })]

    #[test]
    fn recycling_prefix_then_resume_preserves_clean_suffix(
        doc_counts in prop::collection::vec(0u32..10_000, 8..80),
        cutoff_pick in any::<usize>(),
        recycle_capacity in 0usize..6,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

        let mut wal = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            FlushPolicy::PerAppend,
            0,
        );
        wal.set_segment_size_limit_bytes(40);
        wal.set_recycle_capacity(recycle_capacity);

        let mut initial = Vec::with_capacity(doc_counts.len());
        for (idx, doc_count) in doc_counts.iter().copied().enumerate() {
            let entry = add_segment(idx as u64 + 1, doc_count);
            wal.append(&entry).unwrap();
            initial.push(entry);
        }
        wal.flush_and_sync().unwrap();

        let maint = WalMaintenance::new(dir.clone());
        let ranges = maint.segment_ranges_strict().unwrap();
        prop_assert_eq!(ranges.len(), initial.len());

        let cutoff_idx = cutoff_pick % (ranges.len() - 1);
        let cutoff = ranges[cutoff_idx].end_entry_id.unwrap();
        let recyclable = maint.truncate_to_recycle(cutoff).unwrap();
        prop_assert_eq!(recyclable.len(), cutoff_idx + 1);
        let recyclable_count = recyclable.len();

        let surviving_before: Vec<_> = WalReader::<WalEntry>::new(dir.clone())
            .replay()
            .unwrap()
            .into_iter()
            .filter(|record| record.entry_id > cutoff)
            .collect();

        for path in &recyclable {
            wal.recycle_segment(path.clone());
        }
        prop_assert_eq!(
            recyclable
                .iter()
                .filter(|path| dir.exists(path))
                .count(),
            recycle_capacity.min(recyclable_count),
        );

        let pooled_paths = recycle_capacity.min(recyclable_count);
        let tail_len = pooled_paths + 8;
        let mut tail = Vec::with_capacity(tail_len);
        for idx in 0..tail_len {
            let entry = add_segment(10_000 + idx as u64, idx as u32);
            wal.append(&entry).unwrap();
            tail.push(entry);
        }
        wal.flush_and_sync().unwrap();
        drop(wal);

        let resumed_entry = add_segment(99_999, 1);
        let mut resumed = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
        let resumed_id = resumed.append(&resumed_entry).unwrap();
        resumed.flush_and_sync().unwrap();
        drop(resumed);

        let records = WalReader::<WalEntry>::new(dir.clone()).replay().unwrap();
        prop_assert_eq!(records.first().unwrap().entry_id, cutoff + 1);
        prop_assert_eq!(records.last().unwrap().entry_id, resumed_id);
        for pair in records.windows(2) {
            prop_assert_eq!(pair[1].entry_id, pair[0].entry_id + 1);
        }

        let mut expected = payloads(&surviving_before);
        expected.extend(tail);
        expected.push(resumed_entry);
        prop_assert_eq!(payloads(&records), expected);

        let post_ranges = WalMaintenance::new(dir).segment_ranges_strict().unwrap();
        prop_assert!(post_ranges
            .iter()
            .all(|range| range.start_entry_id > cutoff));
    }
}
