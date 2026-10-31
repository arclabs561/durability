//! Dropping a `WalWriter` flushes its write buffer, like `std::io::BufWriter`.
//!
//! Appends acknowledged with `Ok(id)` under a buffering flush policy used to
//! vanish when the writer went out of scope without an explicit `flush()`.
//! A poisoned writer must not touch the file on drop: its state after the
//! failure that poisoned it is indeterminate.

use durability::error::PersistenceResult;
use durability::storage::{Directory, FlushPolicy, MemoryDirectory};
use durability::walog::{WalReader, WalWriter};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn replayed(dir: &Arc<dyn Directory>) -> Vec<Vec<u8>> {
    WalReader::<()>::new(dir.clone())
        .replay_bytes()
        .unwrap()
        .into_iter()
        .map(|r| r.payload)
        .collect()
}

#[test]
fn dropped_writer_flushes_buffered_entries() {
    let dir = MemoryDirectory::arc();
    {
        let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::Manual, 64 * 1024);
        for payload in [b"a", b"b", b"c"] {
            w.append_bytes(payload).unwrap();
        }
        assert!(
            replayed(&dir).is_empty(),
            "entries are still in the write buffer before drop"
        );
    }
    assert_eq!(
        replayed(&dir),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
}

/// Counts `write` and `flush` calls that reach segment files.
struct CountingDir {
    inner: Arc<dyn Directory>,
    file_ops: Arc<AtomicUsize>,
}

struct CountingWriter {
    inner: Box<dyn Write + Send>,
    file_ops: Arc<AtomicUsize>,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file_ops.fetch_add(1, Ordering::SeqCst);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file_ops.fetch_add(1, Ordering::SeqCst);
        self.inner.flush()
    }
}

impl CountingDir {
    fn wrap(&self, inner: Box<dyn Write + Send>) -> Box<dyn Write + Send> {
        Box::new(CountingWriter {
            inner,
            file_ops: self.file_ops.clone(),
        })
    }
}

impl Directory for CountingDir {
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

#[test]
fn poisoned_writer_does_not_flush_on_drop() {
    let file_ops = Arc::new(AtomicUsize::new(0));
    let dir: Arc<dyn Directory> = Arc::new(CountingDir {
        inner: MemoryDirectory::arc(),
        file_ops: file_ops.clone(),
    });
    let mut w = WalWriter::<()>::with_options(dir.clone(), FlushPolicy::Manual, 64 * 1024);
    w.append_bytes(b"a").unwrap();
    // MemoryDirectory has no stable-storage sync: the sync fails after the
    // flush and poisons the writer.
    assert!(w.flush_and_sync().is_err());
    assert!(w.append_bytes(b"b").is_err(), "writer is poisoned");

    let before_drop = file_ops.load(Ordering::SeqCst);
    drop(w);
    assert_eq!(
        file_ops.load(Ordering::SeqCst),
        before_drop,
        "a poisoned writer wrote to or flushed its segment on drop"
    );
    assert_eq!(replayed(&dir), vec![b"a".to_vec()]);
}
