//! Generic checkpoint file (single snapshot blob).
//!
//! This is a generic building block: store one snapshot payload with a small
//! header and CRC32. Higher layers decide *when* to checkpoint and what the
//! snapshot schema is. The core API stores raw bytes; the default `postcard`
//! feature adds serde/postcard convenience methods.
//!
//! ## Public invariants (must not change without a format bump)
//!
//! - **Header**: `[CHECKPOINT_MAGIC][FORMAT_VERSION][last_applied_id:u64][payload_len:u64][crc32:u32]`
//!   (little-endian for integers).
//! - **Checksum**: `crc32fast` over the payload bytes.
//! - **`last_applied_id` semantics**: replay log entries with id \(>\) `last_applied_id`.
//! - **Atomicity**: `CheckpointFile` writes via `Directory::atomic_write`.

use crate::error::{PersistenceError, PersistenceResult};
use crate::formats::{CHECKPOINT_MAGIC, FORMAT_VERSION};
use crate::storage::{self, Directory};
use std::io::{Read, Write};
use std::sync::Arc;

/// Upper bound on checkpoint payload size, to prevent allocating absurd buffers
/// from corrupt/malicious headers.
///
/// This is a *safety* cap, not a correctness requirement; higher layers can
/// choose their own smaller caps by rejecting large snapshots before writing.
pub const MAX_CHECKPOINT_PAYLOAD_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

/// Fixed-size header stored at the start of a checkpoint file.
///
/// Internal wire format; exposed for testing and fuzzing.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct CheckpointHeader {
    magic: [u8; 4],
    version: u32,
    /// The last applied log entry id included in this checkpoint.
    pub last_applied_id: u64,
    /// Payload length in bytes.
    pub payload_len: u64,
    /// CRC32 computed over payload bytes.
    pub checksum: u32,
}

impl CheckpointHeader {
    /// Number of bytes in the serialized header.
    pub const SIZE: usize = 4 + 4 + 8 + 8 + 4;

    /// Create a header with correct magic and version.
    pub(crate) fn new(last_applied_id: u64, payload_len: u64, checksum: u32) -> Self {
        Self {
            magic: CHECKPOINT_MAGIC,
            version: FORMAT_VERSION,
            last_applied_id,
            payload_len,
            checksum,
        }
    }

    /// Write the header to a stream.
    pub fn write<W: Write>(&self, w: &mut W) -> PersistenceResult<()> {
        w.write_all(&self.magic)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.last_applied_id.to_le_bytes())?;
        w.write_all(&self.payload_len.to_le_bytes())?;
        w.write_all(&self.checksum.to_le_bytes())?;
        Ok(())
    }

    /// Read the header from a stream.
    pub fn read<R: Read + ?Sized>(r: &mut R) -> PersistenceResult<Self> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if magic != CHECKPOINT_MAGIC {
            return Err(PersistenceError::Format("invalid checkpoint magic".into()));
        }
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        if version != FORMAT_VERSION {
            return Err(PersistenceError::Format(
                "checkpoint version mismatch".into(),
            ));
        }
        r.read_exact(&mut buf8)?;
        let last_applied_id = u64::from_le_bytes(buf8);
        r.read_exact(&mut buf8)?;
        let payload_len = u64::from_le_bytes(buf8);
        r.read_exact(&mut buf4)?;
        let checksum = u32::from_le_bytes(buf4);
        Ok(Self {
            magic,
            version,
            last_applied_id,
            payload_len,
            checksum,
        })
    }
}

/// Read/write checkpoint files in a `Directory`.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "postcard")]
/// # {
/// use durability::checkpoint::CheckpointFile;
/// use durability::storage::MemoryDirectory;
///
/// #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
/// struct Snapshot { count: u64 }
///
/// let dir = MemoryDirectory::arc();
/// let ckpt = CheckpointFile::new(dir.clone());
/// ckpt.write_postcard("snap.bin", 42, &Snapshot { count: 7 }).unwrap();
///
/// let (last_id, snap): (u64, Snapshot) = ckpt.read_postcard("snap.bin").unwrap();
/// assert_eq!(last_id, 42);
/// assert_eq!(snap, Snapshot { count: 7 });
/// # }
/// ```
pub struct CheckpointFile {
    dir: Arc<dyn Directory>,
}

impl CheckpointFile {
    /// Create a checkpoint helper for `dir`.
    pub fn new(dir: impl Into<Arc<dyn Directory>>) -> Self {
        Self { dir: dir.into() }
    }

    /// Write `payload` to `path` with header + CRC.
    ///
    /// `last_applied_id` should be the last applied log entry id included in
    /// the payload (use `0` if not applicable).
    pub fn write_bytes(
        &self,
        path: &str,
        last_applied_id: u64,
        payload: &[u8],
    ) -> PersistenceResult<()> {
        if payload.len() > MAX_CHECKPOINT_PAYLOAD_BYTES {
            return Err(PersistenceError::Format(format!(
                "checkpoint payload too large: {} bytes (max {})",
                payload.len(),
                MAX_CHECKPOINT_PAYLOAD_BYTES
            )));
        }
        let checksum = crc32fast::hash(payload);
        let h = CheckpointHeader::new(last_applied_id, payload.len() as u64, checksum);
        let mut buf = Vec::with_capacity(CheckpointHeader::SIZE + payload.len());
        h.write(&mut buf)?;
        buf.extend_from_slice(payload);
        self.dir.atomic_write(path, &buf)?;
        Ok(())
    }

    /// Write a raw checkpoint payload and attempt to make it durable on stable storage.
    ///
    /// This is stronger than [`CheckpointFile::write_bytes`]:
    /// - `write_bytes` relies on `Directory::atomic_write` for atomic publish.
    /// - `write_bytes_durable` additionally performs explicit stable-storage barriers on the
    ///   final path (file + parent dir), so "success" better matches "survives power loss".
    ///
    /// Returns `NotSupported` if the underlying directory does not provide `file_path()`.
    ///
    /// Note: if a barrier fails after the atomic publish, this returns an error even though the
    /// checkpoint file may now exist. The error means "not proven durable".
    pub fn write_bytes_durable(
        &self,
        path: &str,
        last_applied_id: u64,
        payload: &[u8],
    ) -> PersistenceResult<()> {
        if self.dir.file_path(path).is_none() {
            return Err(PersistenceError::NotSupported(
                "write_bytes_durable requires Directory::file_path()".into(),
            ));
        }
        self.write_bytes(path, last_applied_id, payload)?;
        storage::sync_file(&*self.dir, path)?;
        storage::sync_parent_dir(&*self.dir, path)?;
        Ok(())
    }

    /// Read `path` and return CRC-validated raw payload bytes.
    ///
    /// Returns `(last_applied_id, payload)`.
    pub fn read_bytes(&self, path: &str) -> PersistenceResult<(u64, Vec<u8>)> {
        let mut f = self.dir.open_file(path)?;
        let h = CheckpointHeader::read(&mut *f)?;
        let len = usize::try_from(h.payload_len)
            .map_err(|_| PersistenceError::Format("payload_len overflow".into()))?;
        if len > MAX_CHECKPOINT_PAYLOAD_BYTES {
            return Err(PersistenceError::Format(format!(
                "checkpoint payload too large: {} bytes (max {})",
                len, MAX_CHECKPOINT_PAYLOAD_BYTES
            )));
        }
        let payload = storage::read_exact_bounded(&mut *f, len)?;
        let got = crc32fast::hash(&payload);
        if got != h.checksum {
            return Err(PersistenceError::CrcMismatch {
                expected: h.checksum,
                actual: got,
            });
        }
        Ok((h.last_applied_id, payload))
    }

    /// Write `value` to `path` as postcard bytes with header + CRC.
    ///
    /// `last_applied_id` should be the last applied log entry id included in
    /// `value` (use `0` if not applicable).
    #[cfg(feature = "postcard")]
    pub fn write_postcard<T: serde::Serialize>(
        &self,
        path: &str,
        last_applied_id: u64,
        value: &T,
    ) -> PersistenceResult<()> {
        let payload =
            postcard::to_allocvec(value).map_err(|e| PersistenceError::Encode(e.to_string()))?;
        self.write_bytes(path, last_applied_id, &payload)
    }

    /// Write a checkpoint and attempt to make it durable on stable storage.
    ///
    /// This is stronger than [`CheckpointFile::write_postcard`]:
    /// - `write_postcard` relies on `Directory::atomic_write` for atomic publish.
    /// - `write_postcard_durable` additionally performs explicit stable-storage barriers on the
    ///   final path (file + parent dir), so "success" better matches "survives power loss".
    ///
    /// Returns `NotSupported` if the underlying directory does not provide `file_path()`.
    ///
    /// Note: if a barrier fails after the atomic publish, this returns an error even though the
    /// checkpoint file may now exist. The error means "not proven durable".
    #[cfg(feature = "postcard")]
    pub fn write_postcard_durable<T: serde::Serialize>(
        &self,
        path: &str,
        last_applied_id: u64,
        value: &T,
    ) -> PersistenceResult<()> {
        if self.dir.file_path(path).is_none() {
            return Err(PersistenceError::NotSupported(
                "write_postcard_durable requires Directory::file_path()".into(),
            ));
        }
        let payload =
            postcard::to_allocvec(value).map_err(|e| PersistenceError::Encode(e.to_string()))?;
        self.write_bytes_durable(path, last_applied_id, &payload)
    }

    /// Read `path` and decode postcard bytes after CRC validation.
    ///
    /// Returns `(last_applied_id, value)`.
    #[cfg(feature = "postcard")]
    pub fn read_postcard<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> PersistenceResult<(u64, T)> {
        let (last_applied_id, payload) = self.read_bytes(path)?;
        let val: T =
            postcard::from_bytes(&payload).map_err(|e| PersistenceError::Decode(e.to_string()))?;
        Ok((last_applied_id, val))
    }
}

#[cfg(test)]
mod raw_tests {
    use super::*;
    use crate::storage::MemoryDirectory;

    #[test]
    fn checkpoint_roundtrip_bytes() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let ckpt = CheckpointFile::new(dir);
        ckpt.write_bytes("c.bin", 42, b"raw snapshot").unwrap();

        let (last_id, out) = ckpt.read_bytes("c.bin").unwrap();
        assert_eq!(last_id, 42);
        assert_eq!(out, b"raw snapshot");
    }
}

#[cfg(all(test, feature = "postcard"))]
mod tests {
    use super::*;
    use crate::storage::{FsDirectory, MemoryDirectory};

    #[test]
    fn checkpoint_roundtrip_postcard() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct S {
            n: u64,
            city: String,
        }
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let ckpt = CheckpointFile::new(dir.clone());
        ckpt.write_postcard(
            "c.bin",
            42,
            &S {
                n: 7,
                city: "東京".into(),
            },
        )
        .unwrap();
        let (last_id, out): (u64, S) = ckpt.read_postcard("c.bin").unwrap();
        assert_eq!(last_id, 42);
        assert_eq!(out.n, 7);
        assert_eq!(out.city, "東京");
    }

    #[test]
    fn durable_checkpoint_requires_fs_backend() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct S {
            n: u64,
        }

        // MemoryDirectory: fail fast without writing.
        let mem: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let ckpt = CheckpointFile::new(mem.clone());
        let err = ckpt
            .write_postcard_durable("c.bin", 1, &S { n: 7 })
            .unwrap_err();
        assert!(matches!(err, PersistenceError::NotSupported(_)));
        assert!(!mem.exists("c.bin"));
    }

    #[test]
    fn durable_checkpoint_roundtrip_fs() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct S {
            city: String,
        }

        let tmp = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
        let ckpt = CheckpointFile::new(fs.clone());
        ckpt.write_postcard_durable(
            "checkpoints/c1.chk",
            7,
            &S {
                city: "東京".into(),
            },
        )
        .unwrap();

        let (last, out): (u64, S) = ckpt.read_postcard("checkpoints/c1.chk").unwrap();
        assert_eq!(last, 7);
        assert_eq!(out.city, "東京");
    }
}
