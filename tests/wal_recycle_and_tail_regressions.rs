//! Regression tests for WAL tail recovery, segment recycling, rotation
//! failures and concurrent atomic writes. Each test asserts the correct
//! behavior, so a failure means the bug is present.
#![cfg(feature = "postcard")]

use durability::storage::{Directory, FlushPolicy, FsDirectory, MemoryDirectory};
use durability::walog::{WalMaintenance, WalReader, WalWriter};
use durability::PersistenceResult;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

fn read_all(dir: &Arc<dyn Directory>, path: &str) -> Vec<u8> {
    let mut v = Vec::new();
    dir.open_file(path).unwrap().read_to_end(&mut v).unwrap();
    v
}

// ---------------------------------------------------------------------------
// Fault-injecting Directory
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Faults {
    fail_create: Option<String>,
    bogus_file_path: Option<(String, PathBuf)>,
}

struct HookDir {
    inner: Arc<dyn Directory>,
    faults: Mutex<Faults>,
}

impl HookDir {
    fn new(inner: Arc<dyn Directory>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            faults: Mutex::new(Faults::default()),
        })
    }
}

impl Directory for HookDir {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        if self.faults.lock().unwrap().fail_create.as_deref() == Some(path) {
            return Err(std::io::Error::other("injected create failure").into());
        }
        self.inner.create_file(path)
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
        self.inner.append_file(path)
    }
    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        self.inner.atomic_write(path, data)
    }
    fn file_path(&self, path: &str) -> Option<PathBuf> {
        if let Some((p, bogus)) = &self.faults.lock().unwrap().bogus_file_path {
            if p == path {
                return Some(bogus.clone());
            }
        }
        self.inner.file_path(path)
    }
}

// ---------------------------------------------------------------------------
// Best-effort replay must not drop valid records after a damaged length
// field mid-segment (resume already refuses the same file).
// ---------------------------------------------------------------------------

fn ten_entry_wal() -> (Arc<dyn Directory>, Vec<u8>, usize) {
    let dir = MemoryDirectory::arc();
    let mut w = WalWriter::<()>::new(dir.clone());
    for i in 0..10u8 {
        w.append_bytes(&[b'p', i]).unwrap();
    }
    w.flush().unwrap();
    drop(w);
    let data = read_all(&dir, "wal/wal_1.log");
    let mut off = 24;
    for _ in 0..3 {
        off += u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
    }
    (dir, data, off)
}

#[test]
fn best_effort_replay_refuses_zeroed_length_mid_segment() {
    let (dir, mut data, off) = ten_entry_wal();
    data[off..off + 4].fill(0);
    dir.atomic_write("wal/wal_1.log", &data).unwrap();
    let be = WalReader::<()>::new(dir.clone()).replay_bytes_best_effort();
    assert!(
        be.is_err(),
        "best-effort replay returned a prefix and silently dropped 6 valid records"
    );
}

#[test]
fn best_effort_replay_refuses_corrupt_length_mid_segment() {
    let (dir, mut data, off) = ten_entry_wal();
    data[off + 2] ^= 0x01; // length now runs past EOF
    dir.atomic_write("wal/wal_1.log", &data).unwrap();
    let be = WalReader::<()>::new(dir.clone()).replay_bytes_best_effort();
    assert!(
        be.is_err(),
        "best-effort replay returned a prefix and silently dropped 6 valid records"
    );
}

#[test]
fn best_effort_replay_still_drops_a_torn_final_record() {
    // Control for the two tests above: a record cut off at EOF, with nothing
    // valid after it, is a torn tail and is dropped.
    let (dir, data, off) = ten_entry_wal();
    dir.atomic_write("wal/wal_1.log", &data[..off + 5]).unwrap();
    let be = WalReader::<()>::new(dir.clone())
        .replay_bytes_best_effort()
        .unwrap();
    assert_eq!(be.len(), 3);
}

// ---------------------------------------------------------------------------
// Recycling: a failure while turning a recycled file into the next segment
// must leave a replayable WAL.
// ---------------------------------------------------------------------------

fn recycling_writer(hook: &Arc<HookDir>) -> WalWriter<()> {
    let dir: Arc<dyn Directory> = hook.clone();
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(40);
    w.set_recycle_capacity(4);
    for _ in 0..4 {
        w.append_bytes(b"x").unwrap();
    }
    w.flush_and_sync().unwrap(); // entries 1..=4 acknowledged durable
    for p in WalMaintenance::new(dir).truncate_to_recycle(2).unwrap() {
        w.recycle_segment(p);
    }
    w
}

#[test]
fn recycled_rotation_with_failed_create_keeps_wal_replayable() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::arc(tmp.path()).unwrap();
    let hook = HookDir::new(fs.clone());
    let mut w = recycling_writer(&hook);
    hook.faults.lock().unwrap().fail_create = Some("wal/wal_5.log".into());
    let _ = w.append_bytes(b"x");
    drop(w);
    hook.faults.lock().unwrap().fail_create = None;
    let replay = WalReader::<()>::new(fs.clone()).replay_bytes();
    assert!(
        replay.is_ok(),
        "durable entries 3,4 unrecoverable after a failed recycled rotation: {:?}",
        replay.err()
    );
}

#[test]
fn recycled_rotation_with_failed_header_write_keeps_wal_replayable() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::arc(tmp.path()).unwrap();
    let hook = HookDir::new(fs.clone());
    let mut w = recycling_writer(&hook);
    hook.faults.lock().unwrap().fail_create = Some("wal/recycle.tmp".into());
    let e = w.append_bytes(b"x");
    assert!(e.is_err());
    assert!(w.append_bytes(b"y").is_err(), "writer must be poisoned");
    drop(w);
    hook.faults.lock().unwrap().fail_create = None;
    let ids: Vec<u64> = WalReader::<()>::new(fs.clone())
        .replay_bytes()
        .unwrap()
        .iter()
        .map(|r| r.entry_id)
        .collect();
    assert_eq!(ids.last(), Some(&4));
    let next = WalWriter::<()>::resume(fs).unwrap().next_entry_id();
    assert_eq!(next, 5);
}

fn temp_root(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("durability-recycle-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// `truncate_to_recycle` reports covered segments until a rotation reuses
/// them, so a checkpointer that recycles after every checkpoint feeds the
/// same path twice. A later rotation must not fail on the second copy.
#[test]
fn repeated_truncate_to_recycle_does_not_poison_writer() {
    let root = temp_root("repeat");
    let fs: Arc<dyn Directory> = FsDirectory::arc(&root).unwrap();
    let mut w = WalWriter::<()>::with_flush_policy(fs.clone(), FlushPolicy::PerAppend);
    w.set_segment_size_limit_bytes(24 + 3 * 24); // header + 3 frames of 8-byte payload
    w.set_recycle_capacity(4);
    let maint = WalMaintenance::new(fs.clone());
    let mut log = Vec::new();
    for round in 0..6u64 {
        let mut res = Ok(0);
        for i in 0..4u64 {
            res = w.append_bytes(&(round * 10 + i).to_le_bytes());
            if res.is_err() {
                break;
            }
        }
        if let Err(e) = res {
            log.push(format!("round {round}: append Err({e})"));
            break;
        }
        w.flush_and_sync().unwrap();
        let covered = w.last_entry_id().unwrap();
        for p in maint.truncate_to_recycle(covered).unwrap() {
            w.recycle_segment(p);
        }
    }
    let next = w.append_bytes(b"after");
    drop(w);
    let _ = std::fs::remove_dir_all(&root);
    assert!(next.is_ok() && log.is_empty(), "{log:?} {next:?}");
}

// ---------------------------------------------------------------------------
// Preallocated rotation
// ---------------------------------------------------------------------------

/// The shrink of a preallocated outgoing segment ends in a sync. When it
/// fails, a retried sync could falsely succeed, so the writer must refuse
/// further appends.
#[test]
fn failed_rotation_shrink_poisons_writer() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::arc(tmp.path().join("root")).unwrap();
    let hook = HookDir::new(fs);
    let dir: Arc<dyn Directory> = hook.clone();
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(80);
    w.set_preallocate_bytes(4096);
    for _ in 0..3 {
        w.append_bytes(b"x").unwrap();
    }
    // Point the segment's filesystem path at a directory so the shrink fails.
    hook.faults.lock().unwrap().bogus_file_path =
        Some(("wal/wal_1.log".into(), tmp.path().to_path_buf()));
    let e = w.append_bytes(b"x");
    hook.faults.lock().unwrap().bogus_file_path = None;
    let next = w.append_bytes(b"y");
    assert!(e.is_err());
    assert!(
        next.is_err(),
        "writer kept accepting appends after its rotation sync failed"
    );
}

/// A preallocated new segment whose size persisted but whose header did not
/// (all zeros) must not make the synced entries before it unrecoverable.
#[test]
fn zero_filled_preallocated_final_segment_is_a_torn_header() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::arc(tmp.path()).unwrap();
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(80);
    w.set_preallocate_bytes(4096);
    for _ in 0..3 {
        w.append_bytes(b"x").unwrap();
    }
    w.flush_and_sync().unwrap(); // 1..=3 acknowledged durable
    w.append_bytes(b"x").unwrap(); // rotation: wal_2 created and preallocated
    assert_eq!(w.current_segment_id(), 2);
    drop(w);
    // Crash image: wal_2's size reached disk, its data blocks did not.
    std::fs::write(tmp.path().join("wal/wal_2.log"), vec![0u8; 4096]).unwrap();
    let be = WalReader::<()>::new(dir.clone()).replay_bytes_best_effort();
    assert!(
        be.map(|v| v.len() == 3).unwrap_or(false),
        "synced entries 1..=3 unrecoverable"
    );
    let mut w = WalWriter::<()>::resume(dir.clone()).unwrap();
    assert_eq!(w.append_bytes(b"z").unwrap(), 4);
    w.flush().unwrap();
    drop(w);
    assert_eq!(WalReader::<()>::new(dir).replay_bytes().unwrap().len(), 4);
}

// ---------------------------------------------------------------------------
// FsDirectory::atomic_write under concurrent writers to one path
// ---------------------------------------------------------------------------

#[test]
fn concurrent_atomic_writes_to_one_path_stay_atomic() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::arc(tmp.path()).unwrap();
    let a = vec![b'a'; 64 * 1024];
    let b = vec![b'b'; 16];
    dir.atomic_write("x.bin", &a).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs = Vec::new();
    for payload in [a.clone(), b.clone()] {
        let d = dir.clone();
        hs.push(std::thread::spawn(move || {
            let mut errs = 0;
            for _ in 0..300 {
                if d.atomic_write("x.bin", &payload).is_err() {
                    errs += 1;
                }
            }
            errs
        }));
    }
    let (d, s, a2, b2) = (dir.clone(), stop.clone(), a.clone(), b.clone());
    let reader = std::thread::spawn(move || {
        let mut torn = 0;
        while !s.load(Ordering::SeqCst) {
            if let Ok(mut f) = d.open_file("x.bin") {
                let mut v = Vec::new();
                if f.read_to_end(&mut v).is_ok() && v != a2 && v != b2 {
                    torn += 1;
                }
            }
        }
        torn
    });
    let errs: usize = hs.into_iter().map(|h| h.join().unwrap()).sum();
    stop.store(true, Ordering::SeqCst);
    let torn = reader.join().unwrap();
    let fin = std::fs::read(tmp.path().join("x.bin")).unwrap();
    let final_ok = fin == a || fin == b;
    assert!(
        errs == 0 && torn == 0 && final_ok,
        "atomic_write is not atomic under concurrent same-path writers: \
         {errs} errors, {torn} torn reads"
    );
}
