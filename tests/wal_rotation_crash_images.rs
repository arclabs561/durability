//! Crash images around WAL segment rotation.
//!
//! `CrashDirectory` keeps two views of every file: what the process sees now,
//! and what has been made durable by `durable_sync_file` (contents) and
//! `durable_sync_parent_dir` (names). A power loss may keep or drop each file's
//! unsynced bytes, and each unsynced name, independently of the others. The
//! tests enumerate every such image and check that entries acknowledged by
//! `flush_and_sync` are still recoverable.

use durability::storage::{Directory, FlushPolicy, MemoryDirectory};
use durability::walog::{WalReader, WalWriter};
use durability::PersistenceResult;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Durable {
    contents: BTreeMap<String, Vec<u8>>,
    names: BTreeSet<String>,
}

struct CrashDirectory {
    live: MemoryDirectory,
    durable: Mutex<Durable>,
}

impl CrashDirectory {
    fn new() -> Self {
        Self {
            live: MemoryDirectory::new(),
            durable: Mutex::new(Durable::default()),
        }
    }

    fn live_files(&self) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        for name in self.live.list_dir("wal").unwrap() {
            let path = format!("wal/{name}");
            let mut bytes = Vec::new();
            self.live
                .open_file(&path)
                .unwrap()
                .read_to_end(&mut bytes)
                .unwrap();
            out.insert(path, bytes);
        }
        out
    }

    /// Every directory a power loss could leave behind.
    fn crash_images(&self) -> Vec<Arc<dyn Directory>> {
        let live = self.live_files();
        let durable = self.durable.lock().unwrap();
        // Per WAL segment: the versions it may have after a crash (`None` = absent).
        let mut choices: Vec<(String, Vec<Option<Vec<u8>>>)> = Vec::new();
        for (path, now) in &live {
            if !path.ends_with(".log") {
                continue;
            }
            let synced = durable.contents.get(path).cloned().unwrap_or_default();
            let mut options = vec![Some(synced.clone())];
            if *now != synced {
                options.push(Some(now.clone()));
            }
            if !durable.names.contains(path) {
                options.push(None);
            }
            choices.push((path.clone(), options));
        }
        let mut images = vec![Vec::<(String, Vec<u8>)>::new()];
        for (path, options) in choices {
            let mut next = Vec::new();
            for image in &images {
                for option in &options {
                    let mut image = image.clone();
                    if let Some(bytes) = option {
                        image.push((path.clone(), bytes.clone()));
                    }
                    next.push(image);
                }
            }
            images = next;
        }
        images
            .into_iter()
            .map(|files| {
                let dir = MemoryDirectory::arc();
                for (path, bytes) in files {
                    dir.atomic_write(&path, &bytes).unwrap();
                }
                dir
            })
            .collect()
    }
}

impl Directory for CrashDirectory {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        self.live.create_file(path)
    }
    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read + Send>> {
        self.live.open_file(path)
    }
    fn exists(&self, path: &str) -> bool {
        self.live.exists(path)
    }
    fn delete(&self, path: &str) -> PersistenceResult<()> {
        self.live.delete(path)
    }
    fn atomic_rename(&self, from: &str, to: &str) -> PersistenceResult<()> {
        self.live.atomic_rename(from, to)
    }
    fn create_dir_all(&self, path: &str) -> PersistenceResult<()> {
        self.live.create_dir_all(path)
    }
    fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>> {
        self.live.list_dir(path)
    }
    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        self.live.append_file(path)
    }
    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        self.live.atomic_write(path, data)
    }
    fn file_path(&self, _path: &str) -> Option<PathBuf> {
        None
    }
    fn durable_sync_file(&self, path: &str) -> PersistenceResult<()> {
        let mut bytes = Vec::new();
        self.live.open_file(path)?.read_to_end(&mut bytes)?;
        self.durable
            .lock()
            .unwrap()
            .contents
            .insert(path.to_string(), bytes);
        Ok(())
    }
    fn durable_sync_parent_dir(&self, path: &str) -> PersistenceResult<()> {
        let parent = path.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
        let prefix = format!("{parent}/");
        let names = self.live.list_dir(parent)?;
        let mut durable = self.durable.lock().unwrap();
        durable.names.retain(|n| !n.starts_with(&prefix));
        durable
            .names
            .extend(names.into_iter().map(|n| format!("{prefix}{n}")));
        Ok(())
    }
}

/// Header (24 bytes) plus three one-byte frames (17 bytes each) per segment.
const SEGMENT_LIMIT: u64 = 24 + 3 * 17;

/// Append `total` entries, acknowledging with `flush_and_sync` after each id in
/// `acks`. Returns the directory and the highest acknowledged entry id.
fn run(total: u8, acks: &[u8]) -> (CrashDirectory, u64) {
    let crash = Arc::new(CrashDirectory::new());
    let dir: Arc<dyn Directory> = crash.clone();
    let mut w = WalWriter::<()>::with_options(dir, FlushPolicy::PerAppend, 0);
    w.set_segment_size_limit_bytes(SEGMENT_LIMIT);
    let mut acked = 0;
    for i in 1..=total {
        let id = w.append_bytes(&[i]).unwrap();
        if acks.contains(&i) {
            w.flush_and_sync().unwrap();
            acked = id;
        }
    }
    w.flush().unwrap();
    drop(w);
    let crash = Arc::try_unwrap(crash).ok().expect("writer dropped");
    (crash, acked)
}

fn assert_acked_entries_survive(crash: &CrashDirectory, acked: u64) {
    let images = crash.crash_images();
    assert!(images.len() > 1, "scenario produced no unsynced state");
    for (n, image) in images.into_iter().enumerate() {
        let mut files = image.list_dir("wal").unwrap();
        files.sort();
        let replayed = WalReader::<()>::new(image.clone()).replay_bytes_best_effort();
        let ids: Vec<u64> = match &replayed {
            Ok(records) => records.iter().map(|r| r.entry_id).collect(),
            Err(e) => panic!("crash image {n} {files:?}: best-effort replay failed: {e}"),
        };
        assert!(
            ids.len() as u64 >= acked && ids.iter().copied().eq(1..=ids.len() as u64),
            "crash image {n} {files:?}: replay returned {ids:?}, entries 1..={acked} were acknowledged"
        );
        let next = WalWriter::<()>::resume(image)
            .unwrap_or_else(|e| panic!("crash image {n} {files:?}: resume failed: {e}"))
            .next_entry_id();
        assert!(
            next > acked,
            "crash image {n} {files:?}: resume would reuse acknowledged id {next}"
        );
    }
}

/// Entries 1 and 2 are acknowledged; entry 3 is an unsynced tail of segment 1
/// when entry 4 rotates into segment 2. Segment 2 reaching disk while segment
/// 1's tail is lost leaves a gap that blocks replay of entries 1 and 2.
#[test]
fn rotation_makes_the_outgoing_segment_durable_before_the_next_exists() {
    let (crash, acked) = run(4, &[2]);
    assert_eq!(acked, 2);
    assert_acked_entries_survive(&crash, acked);
}

/// Segment 2's name is unsynced when entry 7 rotates into segment 3. Segment 3
/// surviving without segment 2 leaves a gap after the acknowledged entries.
#[test]
fn rotation_makes_the_outgoing_segment_name_durable_before_the_next_exists() {
    let (crash, acked) = run(7, &[3]);
    assert_eq!(acked, 3);
    assert_acked_entries_survive(&crash, acked);
}
