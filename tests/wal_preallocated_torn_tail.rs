use durability::{
    storage::{Directory, FsDirectory},
    walog::{WalReader, WalWriter},
};
use std::sync::Arc;

#[test]
fn preallocated_partial_frame_refuses_crc_but_eof_and_padding_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
    let mut w = WalWriter::<()>::new(dir.clone());
    w.set_preallocate_bytes(4096);
    w.append_bytes(b"prefix").unwrap();
    w.flush_and_sync().unwrap();
    let path = dir.file_path("wal/wal_1.log").unwrap();
    let first = std::fs::read(&path).unwrap();
    assert_eq!(first.len(), 4096);
    w.append_bytes(b"unsynced-synthetic-tail").unwrap();
    w.flush().unwrap();
    let complete = std::fs::read(&path).unwrap();
    assert_eq!(complete.len(), 4096);
    drop(w);
    // Locate the actual unique payload rather than relying on private frame offsets.
    let marker = b"unsynced-synthetic-tail";
    let offsets: Vec<_> = complete
        .windows(marker.len())
        .enumerate()
        .filter_map(|(i, w)| (w == marker).then_some(i))
        .collect();
    assert_eq!(offsets.len(), 1);
    let cut = offsets[0] + 3;
    // Model persistence of only the prefix of the final payload. This is a
    // synthetic crash image, not a physical power-loss experiment.
    let mut padded = complete.clone();
    padded[cut..].fill(0);
    std::fs::write(&path, &padded).unwrap();
    let e = WalReader::<()>::new(dir.clone())
        .replay_bytes_best_effort()
        .unwrap_err();
    assert!(matches!(
        e,
        durability::PersistenceError::CrcMismatch { .. }
    ));
    let e = match WalWriter::<()>::resume(dir.clone()) {
        Ok(_) => panic!("unexpected padded resume"),
        Err(e) => e,
    };
    assert!(matches!(
        e,
        durability::PersistenceError::CrcMismatch { .. }
    ));
    assert_eq!(std::fs::read(&path).unwrap(), padded);
    std::fs::write(&path, &complete[..cut]).unwrap();
    let r = WalReader::<()>::new(dir.clone())
        .replay_bytes_best_effort()
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].payload, b"prefix");
    let mut w = WalWriter::<()>::resume(dir.clone()).unwrap();
    assert_eq!(w.append_bytes(b"replacement").unwrap(), 2);
    w.flush_and_sync().unwrap();
    drop(w);
    let r = WalReader::<()>::new(dir.clone()).replay_bytes().unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].payload, b"prefix");
    assert_eq!(r[0].entry_id, 1);
    assert_eq!(r[1].entry_id, 2);
    assert_eq!(r[1].payload, b"replacement");
    std::fs::write(&path, &first).unwrap();
    let w = WalWriter::<()>::resume(dir.clone()).unwrap();
    drop(w);
    let r = WalReader::<()>::new(dir).replay_bytes().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].entry_id, 1);
    assert_eq!(r[0].payload, b"prefix");
}
