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

/// Read exactly `len` payload bytes without trusting `len` for the upfront
/// allocation.
///
/// The length came from a length prefix that has passed its format cap but is
/// otherwise unverified: a corrupt value between the true remaining bytes and
/// the cap would make an eager `vec![0u8; len]` allocate up to the cap (e.g.
/// a 256 MiB zeroed buffer from a 1 KB corrupt checkpoint) before the read
/// fails. Growing the buffer with the bytes actually read bounds the
/// allocation by real data. A short read surfaces as `UnexpectedEof`, the
/// same error class `read_exact` produces, so caller error handling and
/// best-effort-tail semantics are unchanged.
pub(crate) fn read_exact_bounded<R: std::io::Read + ?Sized>(
    r: &mut R,
    len: usize,
) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    // Cap the speculative reserve; beyond this, growth is amortized by
    // read_to_end and tracks bytes actually present.
    const INITIAL_RESERVE_CAP: usize = 1 << 20;
    let mut payload = Vec::with_capacity(len.min(INITIAL_RESERVE_CAP));
    let n = r.take(len as u64).read_to_end(&mut payload)?;
    if n < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("payload truncated: got {n} of {len} bytes"),
        ));
    }
    Ok(payload)
}

/// Make the **data** of `path` durable (`fdatasync`).
///
/// Uses `sync_data()` (fdatasync) rather than `sync_all()` (fsync). For
/// append-only logs, this is correct and faster: fdatasync skips unnecessary
/// metadata updates (mtime, atime) that don't affect crash recovery.
///
/// Requires `Directory::file_path()`. Returns `NotSupported` if unavailable.
pub fn sync_file<D: Directory + ?Sized>(dir: &D, path: &str) -> PersistenceResult<()> {
    let Some(p) = dir.file_path(path) else {
        return Err(PersistenceError::NotSupported(
            "sync_file requires Directory::file_path()".into(),
        ));
    };
    let f = std::fs::OpenOptions::new().read(true).open(&p)?;
    f.sync_data()?;
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
    sync_parent_of_path(&p)
}

/// Attempt to `fsync`/`sync_all` the parent directory of a raw filesystem path.
///
/// Same semantics as [`sync_parent_dir`], but does not require a [`Directory`]
/// instance. Useful for callers that operate on raw `&Path` values and don't
/// otherwise need the `Directory` abstraction (e.g. a JSON or postcard
/// serializer that takes a path argument and inlines its own temp+rename).
///
/// Notes:
/// - On some platforms/filesystems, syncing the directory is required for the
///   rename/create to survive power loss even after syncing the file itself
///   (XFS, some ext4 configurations, overlayfs).
/// - Some platforms (notably Windows) don't allow opening a directory as a
///   `File` and will return an `io::Error`; the caller decides whether to
///   surface that or treat it as best-effort. ext4 with `auto_da_alloc` syncs
///   implicitly.
pub fn sync_parent_of_path(path: &std::path::Path) -> PersistenceResult<()> {
    let Some(parent) = path.parent() else {
        return Err(PersistenceError::InvalidConfig(format!(
            "path has no parent directory: {path:?}"
        )));
    };
    let f = std::fs::File::open(parent)?;
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
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FlushPolicy {
    /// Call `flush()` after each logical append operation.
    PerAppend,
    /// Call `flush()` every N logical append operations.
    ///
    /// `n=1` is equivalent to `PerAppend`. `n=0` is treated as `PerAppend`.
    EveryN(usize),
    /// Call `flush()` when the specified duration has elapsed since the last flush.
    ///
    /// Checked lazily on each `append()` call. If no appends arrive, no flush occurs.
    /// For background flushing independent of write activity, use an external timer
    /// calling [`WalWriter::flush`](crate::walog::WalWriter::flush) directly.
    Interval(std::time::Duration),
    /// Do not call `flush()` implicitly; callers may flush explicitly (if supported by the backend).
    Manual,
}

/// Trait for directory-like storage backends.
pub trait Directory: Send + Sync {
    /// Create a new file for writing (overwriting if it exists).
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>>;
    /// Open an existing file for reading.
    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read + Send>>;
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
    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>>;
    /// Atomically write bytes to a path.
    fn atomic_write(&self, path: &str, data: &[u8]) -> PersistenceResult<()>;
    /// Optional filesystem path for backends that support it.
    fn file_path(&self, path: &str) -> Option<PathBuf>;

    // -- Stable-storage durability helpers (default: delegate to free functions) --
    //
    // These require `file_path()` to return `Some`. Non-filesystem backends
    // get `NotSupported` from the defaults, which is correct -- stable-storage
    // durability is meaningless without a real filesystem.

    /// Attempt to make the file at `path` durable on stable storage.
    ///
    /// Default: delegates to [`sync_file`].
    /// Returns `NotSupported` if `file_path()` returns `None`.
    fn durable_sync_file(&self, path: &str) -> PersistenceResult<()> {
        sync_file(self, path)
    }

    /// Attempt to make the *name* of `path` durable (sync the parent directory).
    ///
    /// Default: delegates to [`sync_parent_dir`].
    /// Returns `NotSupported` if `file_path()` returns `None`.
    fn durable_sync_parent_dir(&self, path: &str) -> PersistenceResult<()> {
        sync_parent_dir(self, path)
    }

    /// Delete `path` and then sync its parent directory.
    ///
    /// This is the durable-delete counterpart to [`Directory::delete`]: the
    /// deletion updates the directory entry, so stable-storage durability
    /// requires a parent-directory sync after the delete. Missing paths keep
    /// `delete`'s no-op semantics.
    ///
    /// Returns `NotSupported` if `file_path()` returns `None`.
    fn delete_durable(&self, path: &str) -> PersistenceResult<()> {
        if self.file_path(path).is_none() {
            return Err(PersistenceError::NotSupported(
                "delete_durable requires Directory::file_path()".into(),
            ));
        }
        if !self.exists(path) {
            return Ok(());
        }
        self.delete(path)?;
        self.durable_sync_parent_dir(path)?;
        Ok(())
    }

    /// Atomically rename and then sync the destination parent directory.
    ///
    /// Returns `NotSupported` if `file_path()` returns `None`.
    fn atomic_rename_durable(&self, from: &str, to: &str) -> PersistenceResult<()> {
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
        let from_parent = from_path.parent();
        let to_parent = to_path.parent();
        if from_parent != to_parent {
            self.durable_sync_parent_dir(from)?;
        }
        self.durable_sync_parent_dir(to)?;
        Ok(())
    }

    /// Atomically write bytes to `path` with explicit durability barriers.
    ///
    /// Stronger than [`Directory::atomic_write`]: writes temp, syncs temp,
    /// renames, syncs parent directory.
    ///
    /// Returns `NotSupported` if `file_path()` returns `None`.
    fn atomic_write_durable(&self, path: &str, data: &[u8]) -> PersistenceResult<()> {
        if self.file_path(path).is_none() {
            return Err(PersistenceError::NotSupported(
                "atomic_write_durable requires Directory::file_path()".into(),
            ));
        }

        let tmp = format!("{path}.tmp");
        if let Err(e) = (|| -> PersistenceResult<()> {
            let mut w = self.create_file(&tmp)?;
            w.write_all(data)?;
            w.flush()?;
            Ok(())
        })() {
            let _ = self.delete(&tmp);
            return Err(e);
        }

        if let Err(e) = self.durable_sync_file(&tmp) {
            let _ = self.delete(&tmp);
            return Err(e);
        }

        if let Err(e) = self.atomic_rename_durable(&tmp, path) {
            let _ = self.delete(&tmp);
            return Err(e);
        }

        Ok(())
    }
}

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

    /// Create a filesystem directory backend wrapped in `Arc<dyn Directory>`.
    pub fn arc(
        root: impl Into<std::path::PathBuf>,
    ) -> PersistenceResult<std::sync::Arc<dyn Directory>> {
        Ok(std::sync::Arc::new(Self::new(root)?))
    }

    fn resolve_path(&self, path: &str) -> PersistenceResult<PathBuf> {
        // Reject path traversal: `..`, absolute paths, and prefix components.
        for component in std::path::Path::new(path).components() {
            match component {
                std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_) => {
                    return Err(PersistenceError::InvalidConfig(format!(
                        "path must not contain '..', absolute, or prefix components: {path}"
                    )));
                }
                _ => {}
            }
        }
        Ok(self.root.join(path))
    }
}

impl Directory for FsDirectory {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        let full_path = self.resolve_path(path)?;
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Box::new(std::fs::File::create(full_path)?))
    }

    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read + Send>> {
        let full_path = self.resolve_path(path)?;
        if !full_path.exists() {
            return Err(PersistenceError::NotFound(full_path.display().to_string()));
        }
        Ok(Box::new(std::fs::File::open(full_path)?))
    }

    fn exists(&self, path: &str) -> bool {
        self.resolve_path(path).map(|p| p.exists()).unwrap_or(false)
    }

    fn delete(&self, path: &str) -> PersistenceResult<()> {
        let full_path = self.resolve_path(path)?;
        if full_path.is_dir() {
            std::fs::remove_dir_all(full_path)?;
        } else if full_path.exists() {
            std::fs::remove_file(full_path)?;
        }
        Ok(())
    }

    fn atomic_rename(&self, from: &str, to: &str) -> PersistenceResult<()> {
        let from_path = self.resolve_path(from)?;
        let to_path = self.resolve_path(to)?;
        if let Some(parent) = to_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(from_path, to_path)?;
        Ok(())
    }

    fn create_dir_all(&self, path: &str) -> PersistenceResult<()> {
        std::fs::create_dir_all(self.resolve_path(path)?)?;
        Ok(())
    }

    fn list_dir(&self, path: &str) -> PersistenceResult<Vec<String>> {
        let full_path = self.resolve_path(path)?;
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

    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
        let full_path = self.resolve_path(path)?;
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
        let full_temp_path = self.resolve_path(&temp_path)?;
        if let Some(parent) = full_temp_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if let Err(e) = (|| -> PersistenceResult<()> {
            let mut temp_file = std::fs::File::create(&full_temp_path)?;
            temp_file.write_all(data)?;
            temp_file.sync_all()?;
            Ok(())
        })() {
            let _ = std::fs::remove_file(&full_temp_path);
            return Err(e);
        }

        let full_path = self.resolve_path(path)?;
        if let Err(e) = std::fs::rename(&full_temp_path, &full_path) {
            let _ = std::fs::remove_file(&full_temp_path);
            return Err(e.into());
        }

        if let Some(parent) = full_path.parent() {
            let parent_file = std::fs::File::open(parent)?;
            parent_file.sync_all()?;
        }
        Ok(())
    }

    fn file_path(&self, path: &str) -> Option<PathBuf> {
        self.resolve_path(path).ok()
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

    /// Create an empty in-memory directory wrapped in `Arc<dyn Directory>`.
    pub fn arc() -> std::sync::Arc<dyn Directory> {
        std::sync::Arc::new(Self::new())
    }
}

impl Directory for MemoryDirectory {
    fn create_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
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

    fn open_file(&self, path: &str) -> PersistenceResult<Box<dyn Read + Send>> {
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
        let mut files = self
            .files
            .write()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "memory directory".to_string(),
                reason: "lock poisoned".to_string(),
            })?;
        files.remove(path);
        // Also remove children (simulate remove_dir_all).
        let prefix = format!("{path}/");
        files.retain(|k, _| !k.starts_with(&prefix));
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
        let data = files
            .remove(from)
            .ok_or_else(|| PersistenceError::NotFound(from.to_string()))?;
        files.insert(to.to_string(), data);
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
        let result: std::collections::BTreeSet<String> = files
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .filter_map(|k| {
                let rest = k.strip_prefix(&prefix).unwrap_or(k);
                let first_component = rest.split('/').next().unwrap_or(rest);
                if first_component.is_empty() {
                    None
                } else {
                    Some(first_component.to_string())
                }
            })
            .collect();
        Ok(result.into_iter().collect())
    }

    fn append_file(&self, path: &str) -> PersistenceResult<Box<dyn Write + Send>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `sync_parent_of_path` succeeds for an existing parent dir and surfaces
    /// the io::Error when the parent does not exist. The successful sync is
    /// the load-bearing case for atomic-rename callers; the failure case
    /// pins the error-surfacing contract so a caller-side `?` does the
    /// right thing.
    #[cfg(unix)]
    #[test]
    fn sync_parent_of_path_existing_parent_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("some_file.txt");
        std::fs::write(&path, b"hi").expect("write");

        // Parent (the tempdir) exists; sync should succeed.
        sync_parent_of_path(&path).expect("sync_parent_of_path on existing parent");
    }

    #[cfg(unix)]
    #[test]
    fn sync_parent_of_path_missing_parent_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let nonexistent = dir.path().join("definitely-not-a-real-subdir/file.txt");
        // Parent does not exist; File::open(parent) fails with NotFound,
        // propagated as PersistenceError::Io via `?`. Assert the specific
        // variant so a future change that started returning a different
        // error class (e.g., InvalidConfig for the non-existent parent)
        // would surface here instead of silently passing.
        match sync_parent_of_path(&nonexistent) {
            Err(PersistenceError::Io(_)) => {}
            other => panic!("expected PersistenceError::Io for missing parent, got {other:?}"),
        }
    }

    #[test]
    fn sync_parent_of_path_root_errors_with_invalid_config() {
        // A bare path with no parent (e.g. just "/") returns InvalidConfig,
        // not a wrapped io::Error. Pins the parent.is_none() branch.
        let root = std::path::Path::new("/");
        match sync_parent_of_path(root) {
            Err(PersistenceError::InvalidConfig(_)) => {}
            other => panic!("expected InvalidConfig for root path, got {other:?}"),
        }
    }
}
