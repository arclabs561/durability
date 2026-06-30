//! Property test: streaming replay (replay_each) and batch replay (replay)
//! must produce identical results for any sequence of entries.
#![cfg(feature = "postcard")]

use durability::storage::MemoryDirectory;
use durability::walog::{WalEntry, WalReader, WalRecord, WalWriter};
use proptest::prelude::*;

fn arb_wal_entry() -> impl Strategy<Value = WalEntry> {
    prop_oneof![
        (1..100u64, 0..10000u32).prop_map(|(seg, cnt)| WalEntry::AddSegment {
            segment_id: seg,
            doc_count: cnt
        }),
        prop::collection::vec((1..100u64, 0..10000u32), 0..10)
            .prop_map(|v| WalEntry::DeleteDocuments { deletes: v }),
        (1..100u64,).prop_map(|(tid,)| WalEntry::StartMerge {
            transaction_id: tid,
            segment_ids: vec![tid],
        }),
        (1..100u64,).prop_map(|(tid,)| WalEntry::EndMerge {
            transaction_id: tid,
            new_segment_id: tid + 100,
            old_segment_ids: vec![tid],
            remapped_deletes: vec![],
        }),
        Just(WalEntry::Checkpoint {
            checkpoint_path: "c.bin".to_string(),
            last_entry_id: 0,
        }),
    ]
}

proptest! {
    #[test]
    fn streaming_equals_batch(
        entries in prop::collection::vec(arb_wal_entry(), 0..50),
    ) {
        let dir = MemoryDirectory::arc();

        // Write entries
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        for e in &entries {
            w.append(e).unwrap();
        }
        w.flush().unwrap();
        drop(w);

        let reader = WalReader::<WalEntry>::new(dir);

        // Batch replay
        let batch = reader.replay().unwrap();

        // Streaming replay
        let mut streaming: Vec<WalRecord<WalEntry>> = Vec::new();
        let count = reader.replay_each(|r| {
            streaming.push(r);
            Ok(())
        }).unwrap();

        // Must be identical
        prop_assert_eq!(batch.len(), streaming.len());
        prop_assert_eq!(count, batch.len() as u64);
        for (b, s) in batch.iter().zip(streaming.iter()) {
            prop_assert_eq!(b.entry_id, s.entry_id);
            prop_assert_eq!(&b.payload, &s.payload);
        }
    }

    #[test]
    fn streaming_best_effort_equals_batch(
        entries in prop::collection::vec(arb_wal_entry(), 0..50),
    ) {
        let dir = MemoryDirectory::arc();

        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        for e in &entries {
            w.append(e).unwrap();
        }
        w.flush().unwrap();
        drop(w);

        let reader = WalReader::<WalEntry>::new(dir);

        let batch = reader.replay_best_effort().unwrap();

        let mut streaming: Vec<WalRecord<WalEntry>> = Vec::new();
        let count = reader.replay_each_best_effort(|r| {
            streaming.push(r);
            Ok(())
        }).unwrap();

        prop_assert_eq!(batch.len(), streaming.len());
        prop_assert_eq!(count, batch.len() as u64);
        for (b, s) in batch.iter().zip(streaming.iter()) {
            prop_assert_eq!(b.entry_id, s.entry_id);
            prop_assert_eq!(&b.payload, &s.payload);
        }
    }
}
