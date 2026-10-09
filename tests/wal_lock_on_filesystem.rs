//! On a real filesystem the WAL writer lock must exclude a live second writer
//! but must not outlive a crashed one.
//!
//! An `O_EXCL` lockfile does neither well: any crash (abort, kill -9, power
//! loss) leaves it behind so `resume` refuses until someone deletes it by
//! hand, and `resume_after_crash` deletes it unconditionally, which also
//! disarms the guard against a writer that is still alive. A kernel lock
//! held for the writer's lifetime is released by the OS on exit.
#![cfg(feature = "postcard")]

use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalReader, WalWriter};
use std::sync::Arc;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("durability-lock-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn add(w: &mut WalWriter<WalEntry>, segment_id: u64) {
    w.append(&WalEntry::AddSegment {
        segment_id,
        doc_count: 1,
    })
    .unwrap();
    w.flush().unwrap();
}

#[test]
fn a_stale_lockfile_with_no_holder_is_ignored() {
    let root = temp_root("no-holder");
    let dir: Arc<dyn Directory> = FsDirectory::arc(&root).unwrap();
    {
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        add(&mut w, 1);
    }
    // What a crashed process leaves: the file, with no process holding a lock.
    std::fs::write(root.join("wal/.lock"), b"left by a crash").unwrap();

    let mut w = WalWriter::<WalEntry>::resume(FsDirectory::arc(&root).unwrap())
        .expect("a lockfile nobody holds must not block resume");
    add(&mut w, 2);
    drop(w);
    let entries = WalReader::<WalEntry>::new(dir).replay().unwrap();
    assert_eq!(entries.len(), 2);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn resume_after_crash_cannot_displace_a_live_writer() {
    let root = temp_root("live");
    let dir: Arc<dyn Directory> = FsDirectory::arc(&root).unwrap();
    let mut live = WalWriter::<WalEntry>::new(dir.clone());
    add(&mut live, 1);

    let second = WalWriter::<WalEntry>::resume_after_crash(FsDirectory::arc(&root).unwrap());
    assert!(
        second.is_err(),
        "resume_after_crash must not take over a WAL whose writer is alive"
    );
    drop(live);
    let _ = std::fs::remove_dir_all(&root);
}
