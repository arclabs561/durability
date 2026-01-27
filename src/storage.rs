//! Storage abstraction for durability.
//!
//! Vocabulary note:
//! - Some durability primitives require **atomicity** (e.g. atomic rename/write) and
//!   **integrity** (checksums, framing).
//! - Stable-storage **durability** (survives power loss after reporting success)
//!   additionally requires explicit `fsync`/`sync_all` barriers and sometimes
//!   parent-directory sync after renames.

use crate::error::{PersistenceError, PersistenceResult};
use std::io::{Read, Write};
use std::path::PathBuf;

/// Attempt to `fsync`/`sync_all` the file at `path`.
///
/// This is the minimal stable-storage durability primitive: once you have written bytes and
/// flushed userspace buffers, `sync_all` is the mechanism that asks the OS to persist them to
/// stable storage (subject to filesystem semantics).
///
/// Notes:
/// - This requires a `Directory` backend that exposes `file_path()`. For backends that do not
///   map to the OS filesystem, this returns `NotSupported`.
/// - This does **not** sync the parent directory; if you are relying on file creation or rename
///   being durable across power loss, also call [`sync_parent_dir`].
pub fn sync_file<D: Directory + ?Sized>(dir: &D, path: &str) -> PersistenceResult<()> {
    let Some(p) = dir.file_path(path) else {
        return Err(PersistenceError::NotSupported(
            "sync_file requires Directory::file_path()".into(),
        ));
    };
    let f = std::fs::OpenOptions::new().read(true).open(&p)?;
    f.sync_all()?;
    Ok(())
}

/// Attempt to `fsync`/`sync_all` the parent directory of `path`.
///
/// This is the commonly-missed step needed to make *names* durable:
/// - durable file creation
/// - durable atomic rename
///
/// Notes:
/// - On some platforms/filesystems, syncing the directory is required for the rename/create to
///   survive power loss even after syncing the file itself.
/// - This requires `Directory::file_path()`. If unavailable, returns `NotSupported`.
pub fn sync_parent_dir<D: Directory + ?Sized>(dir: &D, path: &str) -> PersistenceResult<()> {
    let Some(p) = dir.file_path(path) else {
        return Err(PersistenceError::NotSupported(
            "sync_parent_dir requires Directory::file_path()".into(),
        ));
    };
    let Some(parent) = p.parent() else {
        return Err(PersistenceError::InvalidConfig(format!(
            "path has no parent directory: {p:?}"
        )));
    };
    let f = std::fs::File::open(parent)?;
    // Best-effort: if this fails due to platform-specific directory open semantics,
    // surface the error to the caller; stable storage requires an explicit decision.
    f.sync_all()?;
    Ok(())
}

/// Policy for when writers call `Write::flush()`.
///
/// Vocabulary note:
/// - `flush()` is not a stable-storage durability guarantee on most filesystems; it is best
///   treated as an IO boundary (push to OS / underlying writer).
/// - Stable-storage durability requires explicit `sync_all`/`fsync` barriers, which are not
///   expressible via the `Directory` trait today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Call `flush()` after each logical append operation.
    PerAppend,
    /// Call `flush()` every N logical append operations.
    ///
    /// `n=1` is equivalent to `PerAppend`. `n=0` is treated as `Manual`.
    EveryN(usize),
    /// Do not call `flush()` implicitly; callers may flush explicitly (if supported by the backend).
    Manual,
}

/// Trait for directory-like storage backends.
pub trait Directory: Send + Sync {
    /// Create a new file for writing (overwriting if it exists).
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write>>;
    /// Open an existing file for reading.
    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read>>;
    /// Return whether a path exists.
    fn exists(&self, path: &str) -> bool;
    /// Delete a file or directory (directories recursively).
    fn delete(&self, path: &str) -> PersistenceResult<()>;
    /// Atomically rename/move a file.
    fn atomic_rename(&self, from: &str, to: &str) -> PersistenceResult<()>;
    /// Create a directory (and parents if needed).
    fn create_dir_all(&self, path: &str) -> PersistenceResult<()>;
    /// List entries in a directory.
    fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>>;
    /// Open a file for appending (creating it if missing).
    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write>>;
    /// Atomically write bytes to a path.
    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()>;
    /// Optional filesystem path for backends that support it.
    fn file_path(&self, path: &str) -> Option<PathBuf>;
}

/// Opt-in stable-storage durability operations for a `Directory`.
///
/// Why this exists:
/// - `Directory` is intentionally minimal and backend-agnostic.
/// - Stable-storage durability requires explicit barriers (`sync_all`/`fsync`) and (often)
///   syncing parent directories for durable creates/renames.
///
/// Design:
/// - This trait is intentionally *small* and *composable*.
/// - Default implementations use [`sync_file`] / [`sync_parent_dir`], which require
///   `Directory::file_path()`. For non-filesystem backends, these return `NotSupported`.
pub trait DurableDirectory: Directory {
    /// Attempt to make the file at `path` durable on stable storage.
    fn sync_file(&self, path: &str) -> PersistenceResult<()> {
        sync_file(self, path)
    }

    /// Attempt to make the *name* of `path` durable (sync the parent directory).
    fn sync_parent_dir(&self, path: &str) -> PersistenceResult<()> {
        sync_parent_dir(self, path)
    }

    /// Atomically rename and then sync the destination parent directory.
    fn atomic_rename_durable(&self, from: &str, to: &str) -> PersistenceResult<()> {
        // Fail fast on backends that can't provide stable-storage semantics.
        let from_path = match self.file_path(from) {
            Some(p) => p,
            None => {
                return Err(PersistenceError::NotSupported(
                    "atomic_rename_durable requires Directory::file_path()".into(),
                ));
            }
        };
        let to_path = match self.file_path(to) {
            Some(p) => p,
            None => {
                return Err(PersistenceError::NotSupported(
                    "atomic_rename_durable requires Directory::file_path()".into(),
                ));
            }
        };

        self.atomic_rename(from, to)?;
        // Durable rename requires syncing the destination directory. If the rename crosses
        // directory boundaries, also sync the source directory to persist removal of the old name.
        let from_parent = from_path.parent();
        let to_parent = to_path.parent();
        if from_parent != to_parent {
            self.sync_parent_dir(from)?;
        }
        self.sync_parent_dir(to)?;
        Ok(())
    }

    /// Atomically write bytes to `path` with explicit durability barriers.
    ///
    /// Implementation strategy:
    /// - write temp file
    /// - `sync_file(temp)`
    /// - atomic rename temp → final
    /// - `sync_parent_dir(final)`
    ///
    /// This is a stronger, more explicit version of `Directory::atomic_write`.
    fn atomic_write_durable(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        // Fail fast on backends that can't provide stable-storage semantics.
        if self.file_path(path).is_none() {
            return Err(PersistenceError::NotSupported(
                "atomic_write_durable requires Directory::file_path()".into(),
            ));
        }

        let tmp = format!("{path}.tmp");
        // Step 1: write temp
        if let Err(e) = (|| -> PersistenceResult<()> {
            let mut w = self.create_file(&tmp)?;
            w.write_all(data)?;
            w.flush()?;
            Ok(())
        })() {
            let _ = self.delete(&tmp);
            return Err(e);
        }

        // Step 2: fsync temp
        if let Err(e) = self.sync_file(&tmp) {
            let _ = self.delete(&tmp);
            return Err(e);
        }

        // Step 3: rename + dir barriers
        if let Err(e) = self.atomic_rename_durable(&tmp, path) {
            let _ = self.delete(&tmp);
            return Err(e);
        }

        Ok(())
    }
}

impl<T: Directory + ?Sized> DurableDirectory for T {}

/// Filesystem-backed `Directory` rooted at a local path.
pub struct FsDirectory {
    root: PathBuf,
}

impl FsDirectory {
    /// Create (or open) a filesystem directory backend rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> PersistenceResult<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn resolve_path(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }
}

impl Directory for FsDirectory {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write>> {
        let full_path = self.resolve_path(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Box::new(std::fs::File::create(full_path)?))
    }

    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read>> {
        let full_path = self.resolve_path(path);
        if !full_path.exists() {
            return Err(PersistenceError::MissingPath(full_path));
        }
        Ok(Box::new(std::fs::File::open(full_path)?))
    }

    fn exists(&self, path: &str) -> bool {
        self.resolve_path(path).exists()
    }

    fn delete(&self, path: &str) -> PersistenceResult<()> {
        let full_path = self.resolve_path(path);
        if full_path.is_dir() {
            std::fs::remove_dir_all(full_path)?;
        } else if full_path.exists() {
            std::fs::remove_file(full_path)?;
        }
        Ok(())
    }

    fn atomic_rename(&self, from: &str, to: &str) -> PersistenceResult<()> {
        let from_path = self.resolve_path(from);
        let to_path = self.resolve_path(to);
        if let Some(parent) = to_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(from_path, to_path)?;
        Ok(())
    }

    fn create_dir_all(&self, path: &str) -> PersistenceResult<()> {
        std::fs::create_dir_all(self.resolve_path(path))?;
        Ok(())
    }

    fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>> {
        let full_path = self.resolve_path(path);
        if !full_path.exists() {
            return Ok(Vec::new());
        }
        let entries = std::fs::read_dir(full_path)?;
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry?;
            out.push(entry.file_name().to_string_lossy().to_string());
        }
        out.sort();
        Ok(out)
    }

    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write>> {
        let full_path = self.resolve_path(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(full_path)?;
        Ok(Box::new(file))
    }

    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        let temp_path = format!("{path}.tmp");
        let full_temp_path = self.resolve_path(&temp_path);
        if let Some(parent) = full_temp_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut temp_file = std::fs::File::create(&full_temp_path)?;
        temp_file.write_all(data)?;
        temp_file.sync_all()?;

        let full_path = self.resolve_path(path);
        std::fs::rename(&full_temp_path, &full_path)?;

        if let Some(parent) = full_path.parent() {
            if let Ok(parent_file) = std::fs::File::open(parent) {
                let _ = parent_file.sync_all();
            }
        }
        Ok(())
    }

    fn file_path(&self, path: &str) -> Option<PathBuf> {
        Some(self.resolve_path(path))
    }
}

/// In-memory `Directory` used for tests.
#[derive(Clone, Default)]
pub struct MemoryDirectory {
    files: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>>,
}

impl MemoryDirectory {
    /// Create an empty in-memory directory.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Directory for MemoryDirectory {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write>> {
        // Overwrite semantics: clear the file eagerly, then append in-place.
        self.files
            .write()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "memory directory".to_string(),
                reason: "lock poisoned".to_string(),
            })?
            .insert(path.to_string(), Vec::new());

        Ok(Box::new(MemoryInPlaceWriter {
            files: self.files.clone(),
            path: path.to_string(),
        }))
    }

    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read>> {
        let files = self
            .files
            .read()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "memory directory".to_string(),
                reason: "lock poisoned".to_string(),
            })?;
        let data = files
            .get(path)
            .ok_or_else(|| PersistenceError::NotFound(path.to_string()))?
            .clone();
        Ok(Box::new(std::io::Cursor::new(data)))
    }

    fn exists(&self, path: &str) -> bool {
        self.files
            .read()
            .map(|f| f.contains_key(path))
            .unwrap_or(false)
    }

    fn delete(&self, path: &str) -> PersistenceResult<()> {
        self.files
            .write()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "memory directory".to_string(),
                reason: "lock poisoned".to_string(),
            })?
            .remove(path);
        Ok(())
    }

    fn atomic_rename(&self, from: &str, to: &str) -> PersistenceResult<()> {
        let mut files = self
            .files
            .write()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "memory directory".to_string(),
                reason: "lock poisoned".to_string(),
            })?;
        if let Some(data) = files.remove(from) {
            files.insert(to.to_string(), data);
        }
        Ok(())
    }

    fn create_dir_all(&self, _path: &str) -> PersistenceResult<()> {
        Ok(())
    }

    fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>> {
        let files = self
            .files
            .read()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "memory directory".to_string(),
                reason: "lock poisoned".to_string(),
            })?;
        let prefix = if path.is_empty() {
            "".to_string()
        } else {
            format!("{path}/")
        };
        let mut result: Vec<String> = files
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .map(|k| k.strip_prefix(&prefix).unwrap_or(k).to_string())
            .collect();
        result.sort();
        Ok(result)
    }

    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write>> {
        // Ensure the file exists, then append in-place.
        {
            let mut files = self
                .files
                .write()
                .map_err(|_| PersistenceError::LockFailed {
                    resource: "memory directory".to_string(),
                    reason: "lock poisoned".to_string(),
                })?;
            files.entry(path.to_string()).or_insert_with(Vec::new);
        }
        Ok(Box::new(MemoryInPlaceWriter {
            files: self.files.clone(),
            path: path.to_string(),
        }))
    }

    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        let mut files = self
            .files
            .write()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "memory directory".to_string(),
                reason: "lock poisoned".to_string(),
            })?;
        files.insert(path.to_string(), data.to_vec());
        Ok(())
    }

    fn file_path(&self, _path: &str) -> Option<PathBuf> {
        None
    }
}

struct MemoryInPlaceWriter {
    files: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>>,
    path: String,
}

impl Write for MemoryInPlaceWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut files = self
            .files
            .write()
            .map_err(|_| std::io::Error::other("lock poisoned"))?;
        let entry = files.entry(self.path.clone()).or_insert_with(Vec::new);
        entry.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
