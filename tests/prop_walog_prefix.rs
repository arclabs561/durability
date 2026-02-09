//! Property-based “fuzz-like” tests for `walog` replay semantics.
//!
//! Focus: multi-segment WALs + tail truncation handling.

use durability::formats::{FORMAT_VERSION, WAL_MAGIC};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalSegmentHeader};
use proptest::prelude::*;
use std::sync::Arc;

fn diverse_doc_counts() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0u32), Just(1u32), 2u32..1000u32]
}

fn arb_entries() -> impl Strategy<Value = Vec<WalEntry>> {
    // Keep it to entry types that are cheap and deterministic.
    // Ensure at least one entry so we exercise “suffix” behavior.
    prop::collection::vec(
        (1u64..50u64, 1u64..50u64, diverse_doc_counts()).prop_map(|(entry_id, seg_id, dc)| {
            WalEntry::AddSegment {
                entry_id,
                segment_id: seg_id,
                doc_count: dc,
            }
        }),
        1..80,
    )
}

fn write_segment(seg_id: u64, start_entry_id: u64, entries: &[WalEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    WalSegmentHeader {
        magic: WAL_MAGIC,
        version: FORMAT_VERSION,
        start_entry_id,
        segment_id: seg_id,
    }
    .write(&mut buf)
    .unwrap();
    for e in entries {
        let bytes = WalEntryOnDisk::encode(e).unwrap();
        buf.extend_from_slice(&bytes);
    }
    buf
}

fn split_into_segments(entries: &[WalEntry], max_per_segment: usize) -> Vec<Vec<WalEntry>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < entries.len() {
        let end = (i + max_per_segment).min(entries.len());
        out.push(entries[i..end].to_vec());
        i = end;
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Integration-test crate; disable persistence to avoid SourceParallel issues.
        failure_persistence: None,
        cases: 96,
        .. ProptestConfig::default()
    })]

    #[test]
    fn wal_best_effort_returns_prefix_under_tail_truncation(
        mut entries in arb_entries(),
        max_per_segment in 1usize..20,
        tear_body_bytes in 0usize..256
    ) {
        // Enforce strict monotone entry_ids so WalReader strict checks don't reject the fixture.
        entries.sort_by_key(|e| match e { WalEntry::AddSegment{entry_id, ..} => *entry_id, _ => 0 });
        for (i, e) in entries.iter_mut().enumerate() {
            // rewrite entry_id to strict increasing (1..n) regardless of sampled value
            let id = (i as u64) + 1;
            if let WalEntry::AddSegment { entry_id, .. } = e {
                *entry_id = id;
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let dir = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);
        dir.create_dir_all("wal").unwrap();

        let segs = split_into_segments(&entries, max_per_segment.max(1));
        for (i, seg_entries) in segs.iter().enumerate() {
            let seg_id = (i as u64) + 1;
            let start_entry_id = match &seg_entries[0] {
                WalEntry::AddSegment{entry_id, ..} => *entry_id,
                _ => 1
            };
            let mut bytes = write_segment(seg_id, start_entry_id, seg_entries);

            // Only tear the final segment, and only after the segment header.
            if i + 1 == segs.len() {
                let header = WalSegmentHeader::SIZE;
                if bytes.len() > header {
                    let body_len = bytes.len() - header;
                    let tear = tear_body_bytes.min(body_len);
                    bytes.truncate(bytes.len().saturating_sub(tear));
                }
            }

            let path = format!("wal/wal_{seg_id}.log");
            dir.atomic_write(&path, &bytes).unwrap();
        }

        let reader = WalReader::new(dir.clone());

        // Strict replay may error on torn records, but if the truncation happens to land
        // exactly on a record boundary, strict cannot distinguish that from a shorter WAL.
        let strict = reader.replay();

        let out = reader.replay_best_effort().unwrap();

        // Best-effort must return a prefix of the intended entries.
        prop_assert!(out.len() <= entries.len());
        prop_assert_eq!(&entries[..out.len()], &out[..]);

        // Strict must never return garbage: if it succeeds, it must also be a prefix.
        if let Ok(strict_out) = strict {
            prop_assert!(strict_out.len() <= entries.len());
            prop_assert_eq!(&entries[..strict_out.len()], &strict_out[..]);
        }
    }
}
