//! Property-based negative tests for WAL: corruption in non-final segments must not be ignored.

use durability::formats::{FORMAT_VERSION, WAL_MAGIC};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReader, WalSegmentHeader};
use proptest::prelude::*;
use std::sync::Arc;

fn read_u32_le(bytes: &[u8], offset: usize) -> u32 {
    let b: [u8; 4] = bytes[offset..offset + 4].try_into().unwrap();
    u32::from_le_bytes(b)
}

fn write_segment_bytes(seg_id: u64, start_entry_id: u64, entries: &[WalEntry]) -> Vec<u8> {
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
        let b = WalEntryOnDisk::encode(e).unwrap();
        buf.extend_from_slice(&b);
    }
    buf
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 96,
        .. ProptestConfig::default()
    })]

    #[test]
    fn wal_best_effort_does_not_mask_corruption_in_non_last_segment(
        flip_idx in 0usize..2048,
        flip_mask in any::<u8>()
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);
        dir.create_dir_all("wal").unwrap();

        // Two segments so segment 1 is non-final.
        let s1_entries = vec![
            WalEntry::AddSegment { entry_id: 1, segment_id: 1, doc_count: 1 },
            WalEntry::DeleteDocuments { entry_id: 2, deletes: vec![(1, 0)] },
        ];
        let s2_entries = vec![
            WalEntry::AddSegment { entry_id: 3, segment_id: 2, doc_count: 1 },
        ];

        let mut s1 = write_segment_bytes(1, 1, &s1_entries);
        let s2 = write_segment_bytes(2, 3, &s2_entries);

        // Flip a byte in the *payload* of the first record in segment 1 so CRC mismatch is guaranteed.
        //
        // Layout:
        //   [WalSegmentHeader][len:u32][type:u8][crc:u32][payload...]
        let seg_header = WalSegmentHeader::SIZE;
        prop_assume!(s1.len() > seg_header + 9);
        let len = read_u32_le(&s1, seg_header) as usize;
        prop_assume!(len >= 9);
        prop_assume!(s1.len() >= seg_header + len);
        let payload_len = len - 9;
        prop_assume!(payload_len > 0);
        let payload_start = seg_header + 9;
        let idx = payload_start + (flip_idx % payload_len);
        s1[idx] ^= flip_mask | 0x01; // ensure non-zero flip

        dir.atomic_write("wal/wal_1.log", &s1).unwrap();
        dir.atomic_write("wal/wal_2.log", &s2).unwrap();

        let r = WalReader::new(dir);
        // Both strict and best-effort should reject corruption in non-final segments.
        prop_assert!(r.replay().is_err());
        prop_assert!(r.replay_best_effort().is_err());
    }
}
