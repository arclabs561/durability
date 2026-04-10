//! Async directory trait and blocking adapter.
//!
//! Requires the `async` feature flag.
//!
//! Two components:
//!
//! - [`AsyncDirectory`]: async counterpart to [`Directory`].
//!   Implement this for truly async backends (e.g. object stores, network filesystems).
//!
//! - [`BlockingBridge`]: wraps any sync `Directory` for use from async code by running
//!   each operation inside [`tokio::task::spawn_blocking`]. This is the fastest path
//!   to using durability from a tokio application without blocking the runtime.
//!
//! # Example
//!
//! ```no_run
//! use durability::async_dir::{AsyncDirectory, BlockingBridge};
//! use durability::storage::FsDirectory;
//!
//! # async fn example() {
//! let fs = FsDirectory::new("/tmp/wal-demo").unwrap();
//! let bridge = BlockingBridge::new(fs);
//!
//! bridge.create_dir_all("wal").await.unwrap();
//! bridge.atomic_write("wal/hello.txt", b"world".to_vec()).await.unwrap();
//!
//! let data = bridge.read_file("wal/hello.txt").await.unwrap();
//! assert_eq!(data, b"world");
//! # }
//! ```

use crate::error::{PersistenceError, PersistenceResult};
use crate::storage::Directory;
use std::path::PathBuf;
use std::sync::Arc;

/// Async counterpart to [`Directory`].
///
/// All methods are async and return `PersistenceResult`. Implement this trait
/// for backends with native async I/O (object stores, network filesystems).
///
/// For wrapping a sync `Directory`, use [`BlockingBridge`] instead.
#[allow(async_fn_in_trait)]
pub trait AsyncDirectory: Send + Sync {
    /// Create a new file and write `data` to it (overwriting if it exists).
    async fn write_file(&self, path: &str, data: Vec<u8>) -> PersistenceResult<()>;

    /// Read an entire file into memory.
    async fn read_file(&self, path: &str) -> PersistenceResult<Vec<u8>>;

    /// Append `data` to an existing file (creating it if missing).
    async fn append(&self, path: &str, data: Vec<u8>) -> PersistenceResult<()>;

    /// Return whether a path exists.
    async fn exists(&self, path: &str) -> PersistenceResult<bool>;

    /// Delete a file or directory.
    async fn delete(&self, path: &str) -> PersistenceResult<()>;

    /// Atomically write bytes to a path (write-tmp + rename).
    async fn atomic_write(&self, path: &str, data: Vec<u8>) -> PersistenceResult<()>;

    /// Create a directory (and parents if needed).
    async fn create_dir_all(&self, path: &str) -> PersistenceResult<()>;

    /// List entries in a directory.
    async fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>>;

    /// Optional filesystem path for backends that support it.
    fn file_path(&self, path: &str) -> Option<PathBuf>;
}

/// Wraps a sync [`Directory`] for use from async code.
///
/// Each operation runs inside [`tokio::task::spawn_blocking`], so the async
/// runtime's worker threads are never blocked by filesystem I/O.
pub struct BlockingBridge<D> {
    inner: Arc<D>,
}

impl<D: Directory + 'static> BlockingBridge<D> {
    /// Wrap a sync directory for async use.
    pub fn new(dir: D) -> Self {
        Self {
            inner: Arc::new(dir),
        }
    }

    /// Create from an existing `Arc`.
    pub fn from_arc(dir: Arc<D>) -> Self {
        Self { inner: dir }
    }

    /// Return a reference to the underlying sync directory.
    pub fn inner(&self) -> &D {
        &self.inner
    }

    /// Return a clone of the inner `Arc<D>`.
    pub fn inner_arc(&self) -> Arc<D> {
        self.inner.clone()
    }
}

impl<D: Directory + 'static> AsyncDirectory for BlockingBridge<D> {
    async fn write_file(&self, path: &str, data: Vec<u8>) -> PersistenceResult<()> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            let mut f = dir.create_file(&path)?;
            std::io::Write::write_all(&mut f, &data)?;
            std::io::Write::flush(&mut f)?;
            Ok(())
        })
        .await
        .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    async fn read_file(&self, path: &str) -> PersistenceResult<Vec<u8>> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            let mut f = dir.open_file(&path)?;
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut f, &mut buf)?;
            Ok(buf)
        })
        .await
        .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    async fn append(&self, path: &str, data: Vec<u8>) -> PersistenceResult<()> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            let mut f = dir.append_file(&path)?;
            std::io::Write::write_all(&mut f, &data)?;
            std::io::Write::flush(&mut f)?;
            Ok(())
        })
        .await
        .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    async fn exists(&self, path: &str) -> PersistenceResult<bool> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || Ok(dir.exists(&path)))
            .await
            .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    async fn delete(&self, path: &str) -> PersistenceResult<()> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || dir.delete(&path))
            .await
            .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    async fn atomic_write(&self, path: &str, data: Vec<u8>) -> PersistenceResult<()> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || dir.atomic_write(&path, &data))
            .await
            .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    async fn create_dir_all(&self, path: &str) -> PersistenceResult<()> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || dir.create_dir_all(&path))
            .await
            .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    async fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>> {
        let dir = self.inner.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || dir.list_dir(&path))
            .await
            .map_err(|e| PersistenceError::InvalidState(format!("spawn_blocking panicked: {e}")))?
    }

    fn file_path(&self, path: &str) -> Option<PathBuf> {
        self.inner.file_path(path)
    }
}
