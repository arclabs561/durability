//! Filesystem-backed `Directory` wrapper with targeted fault injection.
//!
//! Important: this file lives under `tests/support/` so it is **not** compiled as a standalone
//! integration test target.

use durability::storage::{Directory, FsDirectory};
use std::io;
use std::sync::{Arc, Mutex};

/// Fault-injection configuration for WAL-related operations.
#[derive(Default)]
pub struct FaultConfig {
    /// Fail when opening an append handle for WAL files.
    pub fail_wal_append_file: bool,
    /// Fail by hiding `file_path()` for WAL paths (simulates “cannot prove durability”).
    pub fail_wal_file_path: bool,
    /// Fail when deleting WAL paths (during truncation).
    pub fail_wal_delete: bool,
    /// Count of delete calls attempted against WAL paths.
    pub delete_calls: usize,
}

/// A filesystem-backed `Directory` wrapper with targeted fault injection.
pub struct FaultyDirectory {
    inner: FsDirectory,
    cfg: Arc<Mutex<FaultConfig>>,
}

impl FaultyDirectory {
    /// Wrap an existing `FsDirectory`.
    pub fn new(inner: FsDirectory) -> Self {
        Self {
            inner,
            cfg: Arc::new(Mutex::new(FaultConfig::default())),
        }
    }

    /// Access the shared fault config (for toggling failpoints and reading counters).
    pub fn cfg(&self) -> Arc<Mutex<FaultConfig>> {
        self.cfg.clone()
    }

    fn is_wal_path(path: &str) -> bool {
        path.starts_with("wal/") || path == "wal"
    }
}

impl Directory for FaultyDirectory {
    fn create_file(&self, path: &str) -> durability::PersistenceResult<Box<dyn io::Write>> {
        self.inner.create_file(path)
    }

    fn open_file(&self, path: &str) -> durability::PersistenceResult<Box<dyn io::Read>> {
        self.inner.open_file(path)
    }

    fn exists(&self, path: &str) -> bool {
        self.inner.exists(path)
    }

    fn delete(&self, path: &str) -> durability::PersistenceResult<()> {
        let mut cfg = self.cfg.lock().unwrap();
        if Self::is_wal_path(path) {
            cfg.delete_calls += 1;
            if cfg.fail_wal_delete {
                return Err(io::Error::new(io::ErrorKind::Other, "injected delete failure").into());
            }
        }
        drop(cfg);
        self.inner.delete(path)
    }

    fn atomic_rename(&self, from: &str, to: &str) -> durability::PersistenceResult<()> {
        self.inner.atomic_rename(from, to)
    }

    fn create_dir_all(&self, path: &str) -> durability::PersistenceResult<()> {
        self.inner.create_dir_all(path)
    }

    fn list_dir(&self, path: &str) -> durability::PersistenceResult<Vec<String>> {
        self.inner.list_dir(path)
    }

    fn append_file(&self, path: &str) -> durability::PersistenceResult<Box<dyn io::Write>> {
        let cfg = self.cfg.lock().unwrap();
        if cfg.fail_wal_append_file && Self::is_wal_path(path) {
            return Err(io::Error::new(io::ErrorKind::Other, "injected append failure").into());
        }
        drop(cfg);
        self.inner.append_file(path)
    }

    fn atomic_write(&self, path: &str, data: &[u8]) -> durability::PersistenceResult<()> {
        self.inner.atomic_write(path, data)
    }

    fn file_path(&self, path: &str) -> Option<std::path::PathBuf> {
        let cfg = self.cfg.lock().unwrap();
        if cfg.fail_wal_file_path && Self::is_wal_path(path) {
            return None;
        }
        drop(cfg);
        self.inner.file_path(path)
    }
}
