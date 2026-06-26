//! Robustness properties for parsers/decoders.
//!
//! These tests are not about accepting arbitrary bytes; they're about:
//! - never panicking on junk input
//! - rejecting corruption via explicit errors
//! - maintaining allocation caps

use durability::checkpoint::CheckpointHeader;
use durability::recordlog::{RecordLogReadMode, RecordLogReader};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalEntryOnDisk, WalReplayMode, WalSegmentHeader};
use proptest::prelude::*;
use std::io::Write;
use std::sync::Arc;

const MAX_WAL_ENTRY_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        cases: 256,
        .. ProptestConfig::default()
    })]

    #[test]
    fn wal_entry_decode_never_panics_on_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        let mut cur = std::io::Cursor::new(bytes);
        let _ = WalEntryOnDisk::decode::<WalEntry, _>(&mut cur, WalReplayMode::BestEffortTail);
    }

    #[test]
    fn wal_header_read_never_panics_on_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let mut cur = std::io::Cursor::new(bytes);
        let _ = WalSegmentHeader::read(&mut cur);
    }

    #[test]
    fn checkpoint_header_read_never_panics_on_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let mut cur = std::io::Cursor::new(bytes);
        let _ = CheckpointHeader::read(&mut cur);
    }
}

#[test]
fn recordlog_best_effort_never_panics_on_random_bytes_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    // Random bytes at the expected path (not necessarily valid header).
    let mut w = dir.create_file("log.bin").unwrap();
    w.write_all(b"NOPE\x00\x01\x02\x03\x04\x05\x06").unwrap();
    w.flush().unwrap();

    let r = RecordLogReader::new(dir, "log.bin");
    let _ = r.read_all(RecordLogReadMode::BestEffort);
}

#[test]
fn wal_entry_decode_rejects_payload_above_cap_before_reading_payload() {
    let payload_len = MAX_WAL_ENTRY_PAYLOAD_BYTES + 1;
    let length = 16u32 + payload_len as u32;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());

    let mut cur = std::io::Cursor::new(bytes);
    let err = WalEntryOnDisk::decode_raw(&mut cur, WalReplayMode::Strict).unwrap_err();
    assert!(
        err.to_string().contains("payload too large"),
        "expected payload cap error, got: {err}"
    );
}

#[test]
fn wal_entry_decode_does_not_reject_cap_sized_payload_as_too_large() {
    let length = 16u32 + MAX_WAL_ENTRY_PAYLOAD_BYTES as u32;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());

    let mut cur = std::io::Cursor::new(bytes);
    let err = WalEntryOnDisk::decode_raw(&mut cur, WalReplayMode::Strict).unwrap_err();
    assert!(
        !err.to_string().contains("payload too large"),
        "exact cap-sized payload should not trip the oversize guard"
    );
}
