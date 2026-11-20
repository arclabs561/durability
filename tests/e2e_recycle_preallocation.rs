//! Recycled segments must not lose frames when preallocation is enabled.

use durability::storage::{Directory, FlushPolicy, FsDirectory};
use durability::walog::{WalMaintenance, WalReader, WalWriter};
use std::sync::Arc;

fn payload(id: u64) -> [u8; 8] {
    (id * 1_000_003).to_le_bytes()
}

fn assert_replays_through(fs: &Arc<dyn Directory>, last: u64, what: &str) {
    for (mode, replayed) in [
        ("strict", WalReader::<()>::new(fs.clone()).replay_bytes()),
        (
            "best-effort",
            WalReader::<()>::new(fs.clone()).replay_bytes_best_effort(),
        ),
    ] {
        let records = replayed.unwrap_or_else(|e| panic!("{what}: {mode} replay failed: {e:?}"));
        assert!(!records.is_empty(), "{what}: {mode} replay is empty");
        let first = records[0].entry_id;
        for (offset, record) in records.iter().enumerate() {
            let id = first + offset as u64;
            assert_eq!(record.entry_id, id, "{what}: {mode} replay has a gap");
            assert_eq!(
                record.payload,
                payload(id),
                "{what}: {mode} payload of {id}"
            );
        }
        assert_eq!(
            records.last().unwrap().entry_id,
            last,
            "{what}: {mode} lost the tail"
        );
    }
}

#[test]
fn recycled_segments_keep_their_frames_with_preallocation() {
    let tmp = tempfile::tempdir().unwrap();
    let fs: Arc<dyn Directory> = FsDirectory::arc(tmp.path()).unwrap();
    let mut writer = WalWriter::<()>::with_options(fs.clone(), FlushPolicy::PerAppend, 0);
    // Three small frames per segment, so every round rotates.
    writer.set_segment_size_limit_bytes(24 + 3 * 24);
    writer.set_recycle_capacity(4);
    writer.set_preallocate_bytes(4096);
    let maintenance = WalMaintenance::new(fs.clone());

    let mut next_id = 1u64;
    for round in 0..4 {
        for _ in 0..4 {
            writer.append_bytes(&payload(next_id)).unwrap();
            next_id += 1;
        }
        writer.flush_and_sync().unwrap();
        let covered = writer.last_entry_id().unwrap();
        let freed = maintenance
            .truncate_to_recycle(covered)
            .unwrap_or_else(|e| panic!("round {round}: truncate_to_recycle failed: {e:?}"));
        for path in freed {
            writer.recycle_segment(path);
        }
    }
    // The pool is now stocked. Write across several more rotations with no
    // truncation, so recycled segments hold entries that must survive.
    for _ in 0..10 {
        writer.append_bytes(&payload(next_id)).unwrap();
        next_id += 1;
    }
    writer.flush_and_sync().unwrap();
    let last = next_id - 1;

    assert_replays_through(&fs, last, "writer open");
    drop(writer);
    assert_replays_through(&fs, last, "writer dropped");
}
