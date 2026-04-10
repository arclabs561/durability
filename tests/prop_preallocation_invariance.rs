//! Property test: preallocation does not change WAL content.
//!
//! WAL segments written with preallocation must produce identical replay
//! results to segments written without preallocation.

use durability::storage::{Directory, FsDirectory, MemoryDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use proptest::prelude::*;
use std::sync::Arc;

fn wal_entry_strategy() -> impl Strategy<Value = WalEntry> {
    prop_oneof![
        (1u64..100, 0u32..1000).prop_map(|(sid, dc)| WalEntry::AddSegment {
            segment_id: sid,
            doc_count: dc,
        }),
        prop::collection::vec((1u64..100, 0u32..1000), 1..5)
            .prop_map(|v| WalEntry::DeleteDocuments { deletes: v }),
    ]
}

fn replay_entries(dir: &Arc<dyn Directory>) -> Vec<(u64, WalEntry)> {
    let r = WalReader::<WalEntry>::new(dir.clone());
    r.replay()
        .unwrap()
        .into_iter()
        .map(|r| (r.entry_id, r.payload))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn preallocation_does_not_change_replay(
        entries in prop::collection::vec(wal_entry_strategy(), 1..50),
        prealloc_kb in 1u64..256,
    ) {
        // Write without preallocation.
        let dir_plain = MemoryDirectory::arc();
        {
            let mut w = WalWriter::<WalEntry>::new(dir_plain.clone());
            for e in &entries {
                w.append(e).unwrap();
            }
            w.flush().unwrap();
        }

        // Write with preallocation on real filesystem.
        let tmp = tempfile::tempdir().unwrap();
        let dir_prealloc: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
        {
            let mut w = WalWriter::<WalEntry>::new(dir_prealloc.clone());
            w.set_preallocate_bytes(prealloc_kb * 1024);
            for e in &entries {
                w.append(e).unwrap();
            }
            w.flush().unwrap();
        }

        let plain = replay_entries(&dir_plain);
        let prealloc = replay_entries(&dir_prealloc);

        prop_assert_eq!(plain.len(), prealloc.len(),
            "Entry count mismatch: plain={} prealloc={}", plain.len(), prealloc.len());
        for (i, (p, a)) in plain.iter().zip(prealloc.iter()).enumerate() {
            prop_assert_eq!(p.0, a.0, "Entry ID mismatch at index {}", i);
            prop_assert_eq!(&p.1, &a.1, "Payload mismatch at index {}", i);
        }
    }

    #[test]
    fn preallocation_with_segment_rotation(
        entries in prop::collection::vec(wal_entry_strategy(), 10..80),
    ) {
        // Small segment limit to force rotation.
        let tmp = tempfile::tempdir().unwrap();
        let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
        {
            let mut w = WalWriter::<WalEntry>::new(dir.clone());
            w.set_segment_size_limit_bytes(512); // tiny segments
            w.set_preallocate_bytes(4096);
            for e in &entries {
                w.append(e).unwrap();
            }
            w.flush().unwrap();
        }

        let replayed = replay_entries(&dir);
        prop_assert_eq!(replayed.len(), entries.len());

        // Verify entry IDs are strictly monotonic.
        for (i, (id, _)) in replayed.iter().enumerate() {
            prop_assert_eq!(*id, (i + 1) as u64);
        }

        // Verify segment files are truncated to actual size (not left at prealloc size).
        let segments = dir.list_dir("wal").unwrap();
        for seg in segments.iter().filter(|s| s.ends_with(".log")) {
            let path = format!("wal/{seg}");
            let fs_path = dir.file_path(&path).unwrap();
            let metadata = std::fs::metadata(&fs_path).unwrap();
            prop_assert!(metadata.len() >= 24, "Segment {} too small: {} bytes", seg, metadata.len());
            // Segments must be smaller than the prealloc size (4096 bytes).
            // The writer truncates on rotation and drop.
            prop_assert!(
                metadata.len() < 4096,
                "Segment {} not truncated: {} bytes (prealloc was 4096)",
                seg,
                metadata.len()
            );
        }
    }
}
