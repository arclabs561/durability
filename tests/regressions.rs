//! Regression tests for recovery and WAL writer behavior.
//!
//! Each test states the expected behavior; it failed before the fix it pins.
#![cfg(feature = "postcard")]

use durability::checkpoint::CheckpointFile;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::{Directory, FlushPolicy, FsDirectory, MemoryDirectory};
use durability::walog::{WalReader, WalWriter};
use durability::{PersistenceError, PersistenceResult};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum Op {
    Inc,
}

fn count(state: &mut u64, _id: u64, op: Op) {
    let Op::Inc = op;
    *state += 1;
}

/// Delegating directory whose writers fail the next `flush` once armed.
struct FlakyFlushDir {
    inner: Arc<dyn Directory>,
    fail_flush: Arc<AtomicBool>,
}

struct FlakyWriter {
    inner: Box<dyn Write + Send>,
    fail_flush: Arc<AtomicBool>,
}

impl Write for FlakyWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if self.fail_flush.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected flush failure"));
        }
        self.inner.flush()
    }
}

impl FlakyFlushDir {
    fn wrap(&self, w: Box<dyn Write + Send>) -> Box<dyn Write + Send> {
        Box::new(FlakyWriter {
            inner: w,
            fail_flush: self.fail_flush.clone(),
        })
    }
}

impl Directory for FlakyFlushDir {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        Ok(self.wrap(self.inner.create_file(path)?))
    }
    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read + Send>> {
        self.inner.open_file(path)
    }
    fn exists(&self, path: &str) -> bool {
        self.inner.exists(path)
    }
    fn delete(&self, path: &str) -> PersistenceResult<()> {
        self.inner.delete(path)
    }
    fn atomic_rename(&self, from: &str, to: &str) -> PersistenceResult<()> {
        self.inner.atomic_rename(from, to)
    }
    fn create_dir_all(&self, path: &str) -> PersistenceResult<()> {
        self.inner.create_dir_all(path)
    }
    fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>> {
        self.inner.list_dir(path)
    }
    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        Ok(self.wrap(self.inner.append_file(path)?))
    }
    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        self.inner.atomic_write(path, data)
    }
    fn file_path(&self, path: &str) -> Option<PathBuf> {
        self.inner.file_path(path)
    }
}

/// Point-in-time recovery cannot roll a checkpoint back. With a target below
/// the checkpoint's last entry, recovery must refuse rather than return the
/// later checkpoint state as if it were the requested one.
#[test]
fn point_in_time_below_checkpoint_is_rejected() {
    let dir = MemoryDirectory::arc();
    let mut w = WalWriter::<Op>::new(dir.clone());
    for _ in 0..10 {
        w.append(&Op::Inc).unwrap();
    }
    w.flush().unwrap();
    drop(w);
    CheckpointFile::new(dir.clone())
        .write_postcard("c.bin", 8, &8u64)
        .unwrap();

    let r = recover_with_wal::<u64, Op, _>(
        &dir,
        Some("c.bin"),
        RecoveryOptions::up_to(5),
        |c| c.unwrap_or(0),
        count,
    );
    match r {
        Err(PersistenceError::InvalidState(_)) => {}
        Err(e) => panic!("expected InvalidState, got {e}"),
        Ok(r) => panic!(
            "up_to(5) with a checkpoint at 8 returned state {} at entry {}",
            r.state, r.last_entry_id
        ),
    }

    // Without the checkpoint the same target is reachable from the WAL.
    let r = recover_with_wal::<u64, Op, _>(
        &dir,
        None,
        RecoveryOptions::up_to(5),
        |c| c.unwrap_or(0),
        count,
    )
    .unwrap();
    assert_eq!((r.state, r.last_entry_id), (5, 5));
}

/// A flush failure after an append's frame reached the file leaves the file
/// state unknown. The writer must stop accepting appends instead of handing
/// the same entry id to the next append.
#[test]
fn flush_error_after_write_does_not_reuse_the_entry_id() {
    let fail_flush = Arc::new(AtomicBool::new(false));
    let dir: Arc<dyn Directory> = Arc::new(FlakyFlushDir {
        inner: MemoryDirectory::arc(),
        fail_flush: fail_flush.clone(),
    });
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    assert_eq!(w.append_bytes(b"one").unwrap(), 1);

    fail_flush.store(true, Ordering::SeqCst);
    assert!(w.append_bytes(b"two").is_err());
    match w.append_bytes(b"two-retry") {
        Err(PersistenceError::InvalidState(_)) => {}
        other => panic!("append after a failed flush must be refused, got {other:?}"),
    }
    drop(w);

    let ids: Vec<u64> = WalReader::<()>::new(dir)
        .replay_bytes()
        .expect("strict replay")
        .into_iter()
        .map(|r| r.entry_id)
        .collect();
    // The frame of the failed append reached the file before the flush failed.
    assert_eq!(ids, vec![1, 2]);
}

/// `set_segment_max_age` applies to a segment the writer continues after a
/// reopen, not only to segments it created itself.
#[test]
fn segment_max_age_applies_after_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let fs: Arc<dyn Directory> = FsDirectory::arc(tmp.path()).unwrap();
    {
        let mut w = WalWriter::<()>::with_flush_policy(fs.clone(), FlushPolicy::PerAppend);
        w.append_bytes(b"a").unwrap();
        w.flush().unwrap();
    }

    let mut w = WalWriter::<()>::open(fs.clone()).unwrap();
    w.set_segment_max_age(Duration::from_millis(1));
    let seg0 = w.current_segment_id();
    std::thread::sleep(Duration::from_millis(20));
    w.append_bytes(b"b").unwrap();
    w.flush().unwrap();
    assert_eq!(
        w.current_segment_id(),
        seg0 + 1,
        "a segment older than max_age must rotate on the next append"
    );
    drop(w);

    let ids: Vec<u64> = WalReader::<()>::new(fs)
        .replay_bytes()
        .unwrap()
        .into_iter()
        .map(|r| r.entry_id)
        .collect();
    assert_eq!(ids, vec![1, 2]);
}
