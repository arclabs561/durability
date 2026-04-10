//! Integration test for generic `CheckpointPublisher::publish()` with custom types.

use durability::publish::CheckpointPublisher;
use durability::storage::FsDirectory;
use durability::walog::{WalReader, WalWriter};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum KvOp {
    Set { key: String, val: String },
    Del { key: String },
    Checkpoint { path: String, last_id: u64 },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct KvSnapshot {
    entries: Vec<(String, String)>,
}

#[test]
fn generic_publish_with_custom_types() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::arc(tmp.path()).unwrap();

    let mut wal = WalWriter::<KvOp>::new(dir.clone());
    wal.append(&KvOp::Set {
        key: "a".into(),
        val: "1".into(),
    })
    .unwrap();
    wal.append(&KvOp::Set {
        key: "b".into(),
        val: "2".into(),
    })
    .unwrap();
    wal.flush().unwrap();

    let snapshot = KvSnapshot {
        entries: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
    };

    let publisher = CheckpointPublisher::new(dir.clone());
    let result = publisher
        .publish(
            &mut wal,
            &snapshot,
            2,
            "checkpoints/snap.bin",
            |path, last_id| KvOp::Checkpoint {
                path: path.to_string(),
                last_id,
            },
        )
        .unwrap();

    assert_eq!(result.checkpoint_path, "checkpoints/snap.bin");
    assert_eq!(result.checkpoint_last_entry_id, 2);
    assert_eq!(result.wal_checkpoint_entry_id, 3);

    // Verify the checkpoint WAL entry was written correctly.
    drop(wal);
    let records = WalReader::<KvOp>::new(dir.clone()).replay().unwrap();
    let last = records.last().unwrap();
    match &last.payload {
        KvOp::Checkpoint { path, last_id } => {
            assert_eq!(path, "checkpoints/snap.bin");
            assert_eq!(*last_id, 2);
        }
        other => panic!("expected Checkpoint, got {:?}", other),
    }

    // Verify the checkpoint file can be read back.
    let ckpt = durability::checkpoint::CheckpointFile::new(dir);
    let (last_applied, snap): (u64, KvSnapshot) =
        ckpt.read_postcard("checkpoints/snap.bin").unwrap();
    assert_eq!(last_applied, 2);
    assert_eq!(snap.entries.len(), 2);
}

#[test]
fn generic_publish_truncates_covered_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::arc(tmp.path()).unwrap();

    let mut wal = WalWriter::<KvOp>::new(dir.clone());
    wal.set_segment_size_limit_bytes(80); // force multi-segment

    for i in 0..5u64 {
        wal.append(&KvOp::Set {
            key: format!("k{i}"),
            val: format!("v{i}"),
        })
        .unwrap();
    }
    wal.flush().unwrap();

    let snapshot = KvSnapshot { entries: vec![] };
    let publisher = CheckpointPublisher::new(dir.clone());
    let result = publisher
        .publish(&mut wal, &snapshot, 5, "snap.bin", |path, last_id| {
            KvOp::Checkpoint {
                path: path.to_string(),
                last_id,
            }
        })
        .unwrap();

    assert!(result.deleted_wal_segments >= 1);
}
