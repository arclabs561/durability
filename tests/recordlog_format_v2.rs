//! Record log format version 2: the frame checksum covers the length field.
//!
//! In version 1 the checksum covered only the payload, and crc32 of an empty
//! payload is 0, so eight zero bytes decoded as a valid empty record. A crash
//! image whose file size reached disk but whose data blocks did not (a
//! zero-filled tail) replayed as phantom empty records, even in strict mode,
//! and a reopened writer appended after them.
//!
//! Version 1 logs stay readable, and writers keep appending version 1 frames
//! to them.

use durability::formats::RECORDLOG_MAGIC;
use durability::recordlog::{RecordLogReadMode, RecordLogReader, RecordLogWriter};
use durability::storage::{Directory, FlushPolicy, MemoryDirectory};
use std::io::{Read, Write};
use std::sync::Arc;

const PATH: &str = "log.bin";

fn bytes(dir: &Arc<dyn Directory>) -> Vec<u8> {
    let mut data = Vec::new();
    dir.open_file(PATH).unwrap().read_to_end(&mut data).unwrap();
    data
}

fn append(dir: &Arc<dyn Directory>, payloads: &[&[u8]]) {
    let mut w = RecordLogWriter::with_options(dir.clone(), PATH, FlushPolicy::PerAppend, 0);
    for p in payloads {
        w.append_bytes(p).unwrap();
    }
    w.flush().unwrap();
}

fn read(dir: &Arc<dyn Directory>, mode: RecordLogReadMode) -> Vec<Vec<u8>> {
    RecordLogReader::new(dir.clone(), PATH)
        .read_all(mode)
        .unwrap()
        .into_iter()
        .map(|r| r.payload)
        .collect()
}

fn append_zeros(dir: &Arc<dyn Directory>, n: usize) {
    let mut f = dir.append_file(PATH).unwrap();
    f.write_all(&vec![0u8; n]).unwrap();
}

#[test]
fn new_logs_use_version_2() {
    let dir = MemoryDirectory::arc();
    append(&dir, &[b"a"]);
    let data = bytes(&dir);
    assert_eq!(data[..4], RECORDLOG_MAGIC);
    assert_eq!(u32::from_le_bytes(data[4..8].try_into().unwrap()), 2);
}

#[test]
fn zero_filled_tail_is_a_torn_tail() {
    let dir = MemoryDirectory::arc();
    append(&dir, &[b"a", b"b"]);
    // Crash image: the file size reached disk, its last data blocks did not.
    append_zeros(&dir, 64);

    assert!(
        RecordLogReader::new(dir.clone(), PATH)
            .read_all(RecordLogReadMode::Strict)
            .is_err(),
        "strict replay must not decode zeros as records"
    );
    assert_eq!(
        read(&dir, RecordLogReadMode::BestEffort),
        vec![b"a".to_vec(), b"b".to_vec()]
    );

    // A reopened writer cuts the zeros off before appending.
    append(&dir, &[b"c"]);
    assert_eq!(
        read(&dir, RecordLogReadMode::Strict),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
}

#[test]
fn zeros_followed_by_a_valid_record_are_not_a_torn_tail() {
    // A valid version 2 frame, taken from a log written by the library.
    let donor = MemoryDirectory::arc();
    append(&donor, &[b"late"]);
    let frame = bytes(&donor)[8..].to_vec();

    let dir = MemoryDirectory::arc();
    append(&dir, &[b"a"]);
    append_zeros(&dir, 16);
    dir.append_file(PATH).unwrap().write_all(&frame).unwrap();

    assert!(RecordLogReader::new(dir.clone(), PATH)
        .read_all(RecordLogReadMode::BestEffort)
        .is_err());
    let mut w = RecordLogWriter::with_options(dir.clone(), PATH, FlushPolicy::PerAppend, 0);
    assert!(
        w.append_bytes(b"b").is_err(),
        "writer must not truncate a valid record"
    );
}

#[test]
fn empty_records_survive_reopen() {
    let dir = MemoryDirectory::arc();
    append(&dir, &[b"", b"x", b""]);
    append(&dir, &[b"y"]);
    assert_eq!(
        read(&dir, RecordLogReadMode::Strict),
        vec![Vec::new(), b"x".to_vec(), Vec::new(), b"y".to_vec()]
    );
}

/// A version 1 log: `len | crc32(payload) | payload` frames.
fn v1_log(payloads: &[&[u8]]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&RECORDLOG_MAGIC);
    data.extend_from_slice(&1u32.to_le_bytes());
    for p in payloads {
        data.extend_from_slice(&u32::try_from(p.len()).unwrap().to_le_bytes());
        data.extend_from_slice(&crc32fast::hash(p).to_le_bytes());
        data.extend_from_slice(p);
    }
    data
}

#[test]
fn version_1_logs_still_read_and_append() {
    let dir = MemoryDirectory::arc();
    dir.atomic_write(PATH, &v1_log(&[b"old-1", b"", b"old-2"]))
        .unwrap();
    assert_eq!(
        read(&dir, RecordLogReadMode::Strict),
        vec![b"old-1".to_vec(), Vec::new(), b"old-2".to_vec()]
    );

    append(&dir, &[b"new"]);
    assert_eq!(
        bytes(&dir),
        v1_log(&[b"old-1", b"", b"old-2", b"new"]),
        "appends to a version 1 log use version 1 frames"
    );
}

#[test]
fn unknown_versions_are_rejected() {
    let dir = MemoryDirectory::arc();
    let mut data = Vec::new();
    data.extend_from_slice(&RECORDLOG_MAGIC);
    data.extend_from_slice(&3u32.to_le_bytes());
    dir.atomic_write(PATH, &data).unwrap();
    assert!(RecordLogReader::new(dir.clone(), PATH)
        .read_all(RecordLogReadMode::BestEffort)
        .is_err());
    let mut w = RecordLogWriter::new(dir, PATH);
    assert!(w.append_bytes(b"x").is_err());
}
