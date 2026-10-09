//! A new `RecordLogWriter` on an existing log must append after the last
//! valid record, not after torn bytes.
//!
//! A crash or a full disk can leave a partial record at the end of the file.
//! Readers tolerate that torn tail, but a writer that appended behind it made
//! the next reader stop at the torn bytes with `CrcMismatch` and the log (and
//! every segstore built on it) unopenable. `WalWriter::resume` already
//! repairs its tail; this pins the same contract for the record log.
#![cfg(feature = "postcard")]

use durability::recordlog::{RecordLogReadMode, RecordLogReader, RecordLogWriter};
use durability::storage::{Directory, MemoryDirectory};
use std::io::Read;
use std::sync::Arc;

const PATH: &str = "log.bin";

fn write(dir: &Arc<dyn Directory>, values: &[u32]) {
    let mut w = RecordLogWriter::new(dir.clone(), PATH);
    for v in values {
        w.append_postcard(v).unwrap();
    }
    w.flush().unwrap();
}

fn bytes(dir: &Arc<dyn Directory>) -> Vec<u8> {
    let mut data = Vec::new();
    dir.open_file(PATH).unwrap().read_to_end(&mut data).unwrap();
    data
}

fn read_strict(dir: &Arc<dyn Directory>) -> Vec<u32> {
    RecordLogReader::new(dir.clone(), PATH)
        .read_all_postcard(RecordLogReadMode::Strict)
        .unwrap()
}

#[test]
fn appends_after_a_torn_tail_stay_readable() {
    let dir = MemoryDirectory::arc();
    write(&dir, &[1, 2, 3, 4, 5]);

    // Torn final record: a header promising 100 payload bytes, then 10 bytes.
    let mut data = bytes(&dir);
    data.extend_from_slice(&100u32.to_le_bytes());
    data.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    data.extend_from_slice(&[0xAB; 10]);
    dir.atomic_write(PATH, &data).unwrap();

    write(&dir, &[6, 7]);

    assert_eq!(read_strict(&dir), vec![1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn appends_after_a_torn_length_prefix_stay_readable() {
    let dir = MemoryDirectory::arc();
    write(&dir, &[1, 2]);
    let mut data = bytes(&dir);
    data.extend_from_slice(&[0x07, 0x00]); // two of the four length bytes
    dir.atomic_write(PATH, &data).unwrap();

    write(&dir, &[3]);

    assert_eq!(read_strict(&dir), vec![1, 2, 3]);
}

#[test]
fn appends_after_a_header_that_never_reached_disk() {
    let dir = MemoryDirectory::arc();
    // The file was created but the crash came before its 8-byte header.
    dir.atomic_write(PATH, &[]).unwrap();

    write(&dir, &[1]);

    assert_eq!(read_strict(&dir), vec![1]);
}

#[test]
fn reopening_refuses_when_valid_records_follow_the_damage() {
    let dir = MemoryDirectory::arc();
    write(&dir, &[1, 2, 3, 4, 5]);
    let mut data = bytes(&dir);
    // Corrupt record 2's payload: its CRC fails, but records 3-5 are intact.
    // That is mid-file damage, not a torn tail, so nothing may be truncated.
    let rec2_payload = 8 + (8 + 1) + 8;
    data[rec2_payload] ^= 0xFF;
    dir.atomic_write(PATH, &data).unwrap();

    let mut w = RecordLogWriter::new(dir.clone(), PATH);
    assert!(w.append_postcard(&6u32).is_err());
    drop(w);
    assert_eq!(bytes(&dir), data, "the damaged log must be left untouched");
}
