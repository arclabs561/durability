//! Regression tests for the high-severity WAL bugs from the October 2026 bug
//! hunt (copied from the bug-hunt repro patch; assertions unchanged). Each
//! test asserts the correct behavior, so a failure means the bug is present.
#![cfg(feature = "postcard")]
#![allow(clippy::let_and_return)]

use durability::publish::CheckpointPublisher;
use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::{Directory, FlushPolicy, FsDirectory, MemoryDirectory};
use durability::walog::{WalMaintenance, WalReader, WalWriter};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum Op {
    Inc,
    Checkpoint { path: String, last_id: u64 },
}

fn count(state: &mut u64, _id: u64, op: Op) {
    if let Op::Inc = op {
        *state += 1;
    }
}

fn read_all(dir: &Arc<dyn Directory>, path: &str) -> Vec<u8> {
    let mut v = Vec::new();
    dir.open_file(path).unwrap().read_to_end(&mut v).unwrap();
    v
}

// ---------------------------------------------------------------------------
// A. Resume after truncate_prefix with a header-only newest segment.
// ---------------------------------------------------------------------------

#[test]
fn a_resume_after_truncate_restarts_entry_ids_when_newest_segment_has_no_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::arc(tmp.path()).unwrap();
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::Manual, 64 * 1024);
    w.set_segment_size_limit_bytes(40);
    for _ in 0..10 {
        w.append_bytes(b"x").unwrap();
        w.flush().unwrap();
    }
    w.flush_and_sync().unwrap();
    // Entry 11 rotates into a new segment: the header goes straight to the
    // file, the frame sits in the userspace write buffer.
    assert_eq!(w.append_bytes(b"x").unwrap(), 11);
    // A checkpoint covers 1..=10; truncate the covered prefix.
    let deleted = WalMaintenance::new(dir.clone())
        .truncate_prefix(10)
        .unwrap();
    eprintln!(
        "A: deleted {deleted} segments; remaining {:?}",
        dir.list_dir("wal").unwrap()
    );
    // Process crash: buffered frame 11 is lost (Drop does not flush).
    drop(w);

    let mut r = WalWriter::<()>::resume(dir.clone()).unwrap();
    eprintln!("A: resumed next_entry_id = {}", r.next_entry_id());
    let id = r.append_bytes(b"after-crash").unwrap();
    r.flush_and_sync().unwrap(); // acknowledged durable
    drop(r);
    let replay = WalReader::<()>::new(dir.clone()).replay_bytes();
    eprintln!(
        "A: new entry id = {id}; strict replay after ack = {:?}",
        replay.as_ref().map(|v| v.len())
    );
    assert!(
        id > 10,
        "entry id reused: got {id}, checkpoint covers 1..=10"
    );
    assert!(
        replay.is_ok(),
        "acknowledged-durable WAL is unreadable: {replay:?}"
    );
}

// ---------------------------------------------------------------------------
// B. recover_with_wal has no checkpoint/WAL continuity check.
// ---------------------------------------------------------------------------

fn publish_scenario(tmp: &std::path::Path) -> Arc<dyn Directory> {
    let dir = FsDirectory::arc(tmp).unwrap();
    let mut wal = WalWriter::<Op>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    wal.set_segment_size_limit_bytes(40);
    for _ in 0..5 {
        wal.append(&Op::Inc).unwrap();
    }
    wal.flush_and_sync().unwrap();
    let p = CheckpointPublisher::new(dir.clone());
    p.publish(&mut wal, &5u64, 5, "c1.bin", |path, last_id| {
        Op::Checkpoint {
            path: path.into(),
            last_id,
        }
    })
    .unwrap();
    for _ in 0..5 {
        wal.append(&Op::Inc).unwrap();
    }
    let last = wal.last_entry_id().unwrap();
    p.publish(&mut wal, &10u64, last, "c2.bin", |path, last_id| {
        Op::Checkpoint {
            path: path.into(),
            last_id,
        }
    })
    .unwrap();
    for _ in 0..3 {
        wal.append(&Op::Inc).unwrap();
    }
    wal.flush_and_sync().unwrap();
    dir
}

#[test]
fn b1_strict_recovery_with_missing_checkpoint_silently_skips_truncated_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = publish_scenario(tmp.path());
    std::fs::remove_file(tmp.path().join("c2.bin")).unwrap();
    let r = recover_with_wal::<u64, Op, _>(
        &dir,
        Some("c2.bin"),
        RecoveryOptions::strict(),
        |c| c.unwrap_or(0),
        count,
    );
    eprintln!(
        "B1: true counter = 13; strict recovery -> {:?}",
        r.as_ref().map(|r| (r.state, r.last_entry_id))
    );
    assert!(
        r.is_err(),
        "strict recovery returned a gapped state instead of an error"
    );
}

#[test]
fn b2_strict_recovery_from_older_checkpoint_silently_skips_truncated_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = publish_scenario(tmp.path());
    let r = recover_with_wal::<u64, Op, _>(
        &dir,
        Some("c1.bin"),
        RecoveryOptions::strict(),
        |c| c.unwrap_or(0),
        count,
    );
    eprintln!(
        "B2: true counter = 13; strict recovery from c1 -> {:?}",
        r.as_ref().map(|r| (r.state, r.last_entry_id))
    );
    assert!(
        r.is_err(),
        "strict recovery from an older checkpoint skipped truncated WAL entries"
    );
}

#[test]
fn b3_best_effort_recovery_with_corrupt_checkpoint_silently_skips_truncated_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = publish_scenario(tmp.path());
    let p = tmp.path().join("c2.bin");
    let mut bytes = std::fs::read(&p).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 0xff;
    std::fs::write(&p, &bytes).unwrap();
    let r = recover_with_wal::<u64, Op, _>(
        &dir,
        Some("c2.bin"),
        RecoveryOptions::best_effort(),
        |c| c.unwrap_or(0),
        count,
    );
    eprintln!(
        "B3: true counter = 13; best-effort recovery -> {:?}",
        r.as_ref().map(|r| (r.state, r.last_entry_id))
    );
    assert!(
        r.is_err(),
        "best-effort recovery returned a non-prefix (gapped) state"
    );
}

// ---------------------------------------------------------------------------
// D. Rotation without preallocation does not sync the outgoing segment;
//    a legal crash image makes the whole WAL unrecoverable.
//
// The bug hunt's `d1_` test is not included: it requires best-effort replay
// to tolerate a truncated non-final segment, which the README contract
// ("corruption in non-final segments is also an error") rules out.
// ---------------------------------------------------------------------------

fn rotation_scenario() -> (tempfile::TempDir, Arc<dyn Directory>) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = FsDirectory::arc(tmp.path()).unwrap();
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(80); // header 24 + 3 frames of 17 bytes
    w.append_bytes(b"1").unwrap();
    w.append_bytes(b"2").unwrap();
    w.flush_and_sync().unwrap(); // entries 1, 2 acknowledged durable
    w.append_bytes(b"3").unwrap(); // unsynced tail of wal_1
    w.append_bytes(b"4").unwrap(); // rotates: wal_1 is NOT synced here
    assert_eq!(w.current_segment_id(), 2);
    drop(w);
    (tmp, dir)
}

#[test]
fn d2_header_only_new_segment_is_accepted_then_wal_becomes_unreadable() {
    let (tmp, dir) = rotation_scenario();
    let set = |name: &str, len: u64| {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path().join(name))
            .unwrap();
        f.set_len(len).unwrap();
    };
    set("wal/wal_1.log", 24 + 2 * 17); // wal_1 tail lost
    set("wal/wal_2.log", 24); // wal_2 header (start_entry_id=4) survived, frame did not
    let mut w = WalWriter::<()>::resume(dir.clone()).unwrap();
    let id = w.append_bytes(b"new").unwrap();
    w.flush_and_sync().unwrap();
    drop(w);
    let r = WalReader::<()>::new(dir).replay_bytes();
    eprintln!(
        "D2: resumed append got id {id}; strict replay after ack -> {:?}",
        r.as_ref().map(|v| v.len()).map_err(|e| e.to_string())
    );
    assert!(
        r.is_ok(),
        "resume accepted the WAL, acked an append, and left it unreadable"
    );
}

#[test]
fn q2_strict_replay_accepts_header_only_first_segment_and_drops_its_entries() {
    let dir = MemoryDirectory::arc();
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(40);
    for _ in 0..3 {
        w.append_bytes(b"x").unwrap();
    }
    drop(w);
    // Crash image: wal_1's frame never reached disk; its header did.
    let seg = read_all(&dir, "wal/wal_1.log");
    dir.atomic_write("wal/wal_1.log", &seg[..24]).unwrap();
    let strict = WalReader::<()>::new(dir.clone()).replay_bytes();
    let resume = WalWriter::<()>::resume(dir.clone()).map(|w| w.next_entry_id());
    eprintln!(
        "Q2: strict replay -> {:?}; resume -> {:?}",
        strict
            .as_ref()
            .map(|v| v.iter().map(|r| r.entry_id).collect::<Vec<_>>())
            .map_err(|e| e.to_string()),
        resume.as_ref().map_err(|e| e.to_string())
    );
    assert!(
        strict.is_err(),
        "strict replay silently skipped entry 1 (wal_1 header says start=1, wal_2 starts at 2)"
    );
}

// ---------------------------------------------------------------------------
// Round 2: resume races and format upgrades (from tests/bughunt2.rs).
// ---------------------------------------------------------------------------

type Hook = Box<dyn FnOnce() + Send>;

/// Delegating `Directory` that runs a one-shot hook inside the first
/// `list_dir("wal")` call, *after* the inner listing was taken. This pins the
/// interleaving "B lists the WAL, then A acts, then B continues" without
/// relying on scheduler luck.
struct HookDir {
    inner: Arc<dyn Directory>,
    hook: Mutex<Option<Hook>>,
}

impl Directory for HookDir {
    fn create_file(&self, p: &str) -> durability::PersistenceResult<Box<dyn Write + Send>> {
        self.inner.create_file(p)
    }
    fn open_file(&self, p: &str) -> durability::PersistenceResult<Box<dyn Read + Send>> {
        self.inner.open_file(p)
    }
    fn exists(&self, p: &str) -> bool {
        self.inner.exists(p)
    }
    fn delete(&self, p: &str) -> durability::PersistenceResult<()> {
        self.inner.delete(p)
    }
    fn atomic_rename(&self, f: &str, t: &str) -> durability::PersistenceResult<()> {
        self.inner.atomic_rename(f, t)
    }
    fn create_dir_all(&self, p: &str) -> durability::PersistenceResult<()> {
        self.inner.create_dir_all(p)
    }
    fn list_dir(&self, p: &str) -> durability::PersistenceResult<Vec<String>> {
        let listing = self.inner.list_dir(p)?;
        if p == "wal" {
            if let Some(h) = self.hook.lock().unwrap().take() {
                h();
            }
        }
        Ok(listing)
    }
    fn append_file(&self, p: &str) -> durability::PersistenceResult<Box<dyn Write + Send>> {
        self.inner.append_file(p)
    }
    fn atomic_write(&self, p: &str, d: &[u8]) -> durability::PersistenceResult<()> {
        self.inner.atomic_write(p, d)
    }
    fn file_path(&self, p: &str) -> Option<PathBuf> {
        self.inner.file_path(p)
    }
}

fn temp_root(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("durability-bh2-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn writer(dir: Arc<dyn Directory>, resume: bool) -> WalWriter<()> {
    let mut w = if resume {
        let r = WalWriter::<()>::resume(dir).unwrap();
        r
    } else {
        WalWriter::<()>::with_flush_policy(dir, FlushPolicy::PerAppend)
    };
    w.set_segment_size_limit_bytes(24 + 3 * 24); // header + 3 frames of 8-byte payload
    w
}

/// R1: `WalWriter::resume` enumerates `wal/` *before* taking the writer lock
/// (walog.rs:946 vs 953) and then trusts that stale listing. If the previous
/// writer rotates and releases the lock in between, the new writer resumes in
/// the old segment with stale ids and later recreates (O_TRUNC) the segment
/// the previous writer synced.
#[test]
fn r1_resume_lists_segments_before_taking_the_lock() {
    let root = temp_root("r1");
    let fs: Arc<dyn Directory> = FsDirectory::arc(&root).unwrap();

    // Initial WAL: ids 1..=6 across wal_1, wal_2 (3 frames each).
    {
        let mut w = writer(fs.clone(), false);
        for i in 1..=6u64 {
            w.append_bytes(&i.to_le_bytes()).unwrap();
        }
        w.flush_and_sync().unwrap();
    }
    println!("initial segments: {:?}", fs.list_dir("wal").unwrap());

    // Writer A runs entirely inside B's enumerate window: it resumes, rotates
    // into wal_3, acks ids 7..=9 as durable, and exits (lock released).
    let acked: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let (fs_a, acked_a) = (fs.clone(), acked.clone());
    let hook: Hook = Box::new(move || {
        let mut a = writer(fs_a.clone(), true);
        let mut got = Vec::new();
        for i in 7..=9u64 {
            got.push(a.append_bytes(&i.to_le_bytes()).unwrap());
        }
        a.flush_and_sync().unwrap();
        println!(
            "A: appended {got:?}, flush_and_sync Ok, segment now {}",
            a.current_segment_id()
        );
        *acked_a.lock().unwrap() = got;
    });
    let hooked: Arc<dyn Directory> = Arc::new(HookDir {
        inner: fs.clone(),
        hook: Mutex::new(Some(hook)),
    });

    // Writer B: the resume that started before A rotated.
    let b = WalWriter::<()>::resume(hooked);
    let mut b = match b {
        Ok(b) => b,
        Err(e) => {
            println!("B: resume refused: {e} (correct)");
            return;
        }
    };
    b.set_segment_size_limit_bytes(24 + 3 * 24);
    println!(
        "B: resumed at segment {} next_entry_id {}",
        b.current_segment_id(),
        b.next_entry_id()
    );
    let acked = acked.lock().unwrap().clone();
    let mut b_ids = Vec::new();
    for i in 0..4u64 {
        b_ids.push(b.append_bytes(&(100 + i).to_le_bytes()).unwrap());
    }
    let sync = b.flush_and_sync();
    println!("B: appended {b_ids:?}, flush_and_sync {sync:?}");
    drop(b);

    println!("segments after B: {:?}", fs.list_dir("wal").unwrap());
    println!(
        "strict replay: {:?}",
        WalReader::<()>::new(fs.clone())
            .replay_bytes()
            .map(|v| v.into_iter().map(|r| r.entry_id).collect::<Vec<_>>())
    );
    let resume_again = WalWriter::<()>::resume(fs.clone()).map(|w| w.next_entry_id());
    println!("resume after B: {resume_again:?}");
    let survived = WalReader::<()>::new(fs.clone())
        .replay_bytes_best_effort()
        .map(|v| {
            v.into_iter()
                .filter(|r| r.payload.len() == 8)
                .map(|r| u64::from_le_bytes(r.payload[..].try_into().unwrap()))
                .collect::<Vec<_>>()
        });
    println!("best-effort payloads: {survived:?}");

    assert!(
        b_ids[0] > *acked.last().unwrap(),
        "B reused entry ids {b_ids:?} that A already acked as durable ({acked:?})"
    );
}

/// R10 (found by the new fuzz_wal_resume oracle, AFL): resume appends v3
/// frames (frame checksum) into a legacy v2 last segment, whose reader uses
/// the payload-only checksum. After an upgrade, the first append makes the
/// WAL unreadable: strict and best-effort replay both fail with a CRC error.
#[test]
fn r10_resume_appends_v3_frames_into_v2_segment() {
    use durability::storage::MemoryDirectory;
    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
    // A WAL written by a pre-v3 release: header version 2, payload-only CRC.
    let mut seg = Vec::new();
    seg.extend_from_slice(b"VWAL");
    seg.extend_from_slice(&2u32.to_le_bytes());
    seg.extend_from_slice(&1u64.to_le_bytes()); // start_entry_id
    seg.extend_from_slice(&1u64.to_le_bytes()); // segment_id
    for (id, p) in [(1u64, &b"old-1"[..]), (2, &b"old-2"[..])] {
        seg.extend_from_slice(&(16 + p.len() as u32).to_le_bytes());
        seg.extend_from_slice(&id.to_le_bytes());
        seg.extend_from_slice(&crc32fast::hash(p).to_le_bytes());
        seg.extend_from_slice(p);
    }
    dir.atomic_write("wal/wal_1.log", &seg).unwrap();
    let before = WalReader::<()>::new(dir.clone())
        .replay_bytes()
        .map(|v| v.len());
    println!("legacy v2 WAL readable before upgrade: {before:?}");

    let mut w = WalWriter::<()>::resume(dir.clone()).unwrap();
    let id = w.append_bytes(b"new-3").unwrap();
    w.flush().unwrap();
    drop(w);
    let strict = WalReader::<()>::new(dir.clone())
        .replay_bytes()
        .map(|v| v.len());
    let be = WalReader::<()>::new(dir.clone())
        .replay_bytes_best_effort()
        .map(|v| v.len());
    let again = WalWriter::<()>::resume(dir.clone()).map(|w| w.next_entry_id());
    println!(
        "after resume+append(id {id}): strict {strict:?}; best-effort {be:?}; resume {again:?}"
    );
    assert_eq!(strict.ok(), Some(3));
}
