//! Recovery must recognize synced empty v3 frames after a damaged length.
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalSegmentHeader, WalWriter};
use std::sync::Arc;

fn damaged_segment(
    version: u32,
    suffix: &[u8],
    corrupt_suffix_crc: bool,
) -> (tempfile::TempDir, Arc<dyn Directory>, String, Vec<u8>) {
    let temp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::arc(temp.path()).unwrap();
    let mut writer = WalWriter::<()>::new(dir.clone());
    assert_eq!(writer.append_bytes(b"x").unwrap(), 1);
    assert_eq!(writer.append_bytes(suffix).unwrap(), 2);
    writer.flush_and_sync().unwrap();
    drop(writer);
    let files = dir.list_dir("wal").unwrap();
    let path = format!(
        "wal/{}",
        files.iter().find(|p| p.ends_with(".log")).unwrap()
    );
    let fs_path = dir.file_path(&path).unwrap();
    let mut bytes = std::fs::read(&fs_path).unwrap();
    let first = WalSegmentHeader::SIZE;
    let first_len = u32::from_le_bytes(bytes[first..first + 4].try_into().unwrap()) as usize;
    let second = first + first_len;
    assert_eq!(first_len, 17);
    assert_eq!(bytes.len() - second, 16 + suffix.len());
    assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 3);
    if version == 2 {
        bytes[4..8].copy_from_slice(&version.to_le_bytes());
        // Legacy v2 covers only payload bytes, unlike the writer's v3 frames.
        for (offset, payload) in [(first, b"x".as_slice()), (second, suffix)] {
            bytes[offset + 12..offset + 16]
                .copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
        }
    }
    if corrupt_suffix_crc {
        bytes[second + 12] ^= 1;
    }
    // Length is the first u32 in the frame; bit 16 makes it run past EOF.
    bytes[first + 2] ^= 1;
    std::fs::write(fs_path, &bytes).unwrap();
    (temp, dir, path, bytes)
}

#[test]
fn synced_empty_v3_suffix_prevents_destructive_recovery() {
    let (_temp, dir, path, before) = damaged_segment(3, b"", false);
    let result = WalWriter::<()>::resume(dir.clone());
    let after = std::fs::read(dir.file_path(&path).unwrap()).unwrap();
    assert_eq!(after, before, "recovery must preserve acknowledged frames");
    assert!(
        result.is_err(),
        "valid empty suffix proves mid-segment corruption"
    );
}

#[test]
fn invalid_empty_v3_checksum_does_not_prove_a_valid_suffix() {
    let (_temp, dir, path, _) = damaged_segment(3, b"", true);
    let writer = WalWriter::<()>::resume(dir.clone()).unwrap();
    assert_eq!(
        std::fs::metadata(dir.file_path(&path).unwrap())
            .unwrap()
            .len(),
        WalSegmentHeader::SIZE as u64
    );
    drop(writer);
}

#[test]
fn legacy_empty_payload_crc_remains_insufficient_suffix_evidence() {
    let (_temp, dir, path, _) = damaged_segment(2, b"", false);
    let writer = WalWriter::<()>::resume(dir.clone()).unwrap();
    assert_eq!(
        std::fs::metadata(dir.file_path(&path).unwrap())
            .unwrap()
            .len(),
        WalSegmentHeader::SIZE as u64
    );
    drop(writer);
}

#[test]
fn legacy_nonempty_suffix_still_prevents_destructive_recovery() {
    let (_temp, dir, path, before) = damaged_segment(2, b"y", false);
    assert!(WalWriter::<()>::resume(dir.clone()).is_err());
    assert_eq!(
        std::fs::read(dir.file_path(&path).unwrap()).unwrap(),
        before
    );
}
