//! Generic checkpoint file (single snapshot blob).
//!
//! This is a generic building block: store one postcard-encoded snapshot with
//! a small header and CRC32. Higher layers decide *when* to checkpoint and what
//! the snapshot schema is.
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
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::sync::Arc;

/// Upper bound on checkpoint payload size, to prevent allocating absurd buffers
/// from corrupt/malicious headers.
///
/// This is a *safety* cap, not a correctness requirement; higher layers can
/// choose their own smaller caps by rejecting large snapshots before writing.
pub const MAX_CHECKPOINT_PAYLOAD_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

#[repr(C)]
#[derive(Debug, Clone, Copy)]
/// Fixed-size header stored at the start of a checkpoint file.
pub struct CheckpointHeader {
    /// Magic bytes (should equal `CHECKPOINT_MAGIC`).
    pub magic: [u8; 4],
    /// Format version (should equal `FORMAT_VERSION`).
    pub version: u32,
    /// The last applied log entry id included in this checkpoint.
    ///
    /// Invariants:
    /// - **Monotone** for successive checkpoints of the same logical stream.
    /// - Used to decide which log entries must be replayed after loading a
    ///   checkpoint (replay entries with id > last_applied_id).
    ///
    /// For callers that do not have a log, use `0`.
    pub last_applied_id: u64,
    /// Payload length in bytes.
    pub payload_len: u64,
    /// CRC32 computed over payload bytes.
    pub checksum: u32,
}

impl CheckpointHeader {
    /// Number of bytes in the serialized header.
    pub const SIZE: usize = 4 + 4 + 8 + 8 + 4;

    /// Write the header to a stream.
    pub fn write<W: Write>(&self, w: &mut W) -> PersistenceResult<()> {
        w.write_all(&self.magic)?;
        w.write_u32::<LittleEndian>(self.version)?;
        w.write_u64::<LittleEndian>(self.last_applied_id)?;
        w.write_u64::<LittleEndian>(self.payload_len)?;
        w.write_u32::<LittleEndian>(self.checksum)?;
        Ok(())
    }

    /// Read the header from a stream.
    pub fn read<R: Read + ?Sized>(r: &mut R) -> PersistenceResult<Self> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if magic != CHECKPOINT_MAGIC {
            return Err(PersistenceError::Format("invalid checkpoint magic".into()));
        }
        let version = r.read_u32::<LittleEndian>()?;
        if version != FORMAT_VERSION {
            return Err(PersistenceError::Format(
                "checkpoint version mismatch".into(),
            ));
        }
        Ok(Self {
            magic,
            version,
            last_applied_id: r.read_u64::<LittleEndian>()?,
            payload_len: r.read_u64::<LittleEndian>()?,
            checksum: r.read_u32::<LittleEndian>()?,
        })
    }
}

/// Read/write checkpoint files in a `Directory`.
pub struct CheckpointFile {
    dir: Arc<dyn Directory>,
}

impl CheckpointFile {
    /// Create a checkpoint helper for `dir`.
    pub fn new(dir: impl Into<Arc<dyn Directory>>) -> Self {
        Self { dir: dir.into() }
    }

    /// Write `value` to `path` as postcard bytes with header + CRC.
    ///
    /// `last_applied_id` should be the last applied log entry id included in
    /// `value` (use `0` if not applicable).
    pub fn write_postcard<T: serde::Serialize>(
        &self,
        path: &str,
        last_applied_id: u64,
        value: &T,
    ) -> PersistenceResult<()> {
        let payload =
            postcard::to_allocvec(value).map_err(|e| PersistenceError::Encode(e.to_string()))?;
        if payload.len() > MAX_CHECKPOINT_PAYLOAD_BYTES {
            return Err(PersistenceError::Format(format!(
                "checkpoint payload too large: {} bytes (max {})",
                payload.len(),
                MAX_CHECKPOINT_PAYLOAD_BYTES
            )));
        }
        let checksum = crc32fast::hash(&payload);
        let h = CheckpointHeader {
            magic: CHECKPOINT_MAGIC,
            version: FORMAT_VERSION,
            last_applied_id,
            payload_len: payload.len() as u64,
            checksum,
        };
        let mut buf = Vec::with_capacity(CheckpointHeader::SIZE + payload.len());
        h.write(&mut buf)?;
        buf.extend_from_slice(&payload);
        self.dir.atomic_write(path, &buf)?;
        Ok(())
    }

    /// Write a checkpoint and attempt to make it durable on stable storage.
    ///
    /// This is stronger than [`write_postcard`]:
    /// - `write_postcard` relies on `Directory::atomic_write` for atomic publish.
    /// - `write_postcard_durable` additionally performs explicit stable-storage barriers on the
    ///   final path (file + parent dir), so “success” better matches “survives power loss”.
    ///
    /// Returns `NotSupported` if the underlying directory does not provide `file_path()`.
    ///
    /// Note: if a barrier fails after the atomic publish, this returns an error even though the
    /// checkpoint file may now exist. The error means “not proven durable”.
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
        self.write_postcard(path, last_applied_id, value)?;
        storage::sync_file(&*self.dir, path)?;
        storage::sync_parent_dir(&*self.dir, path)?;
        Ok(())
    }

    /// Read `path` and decode postcard bytes after CRC validation.
    ///
    /// Returns `(last_applied_id, value)`.
    pub fn read_postcard<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> PersistenceResult<(u64, T)> {
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
        let mut payload = vec![0u8; len];
        f.read_exact(&mut payload)?;
        let got = crc32fast::hash(&payload);
        if got != h.checksum {
            return Err(PersistenceError::CrcMismatch {
                expected: h.checksum,
                actual: got,
            });
        }
        let val: T =
            postcard::from_bytes(&payload).map_err(|e| PersistenceError::Decode(e.to_string()))?;
        Ok((h.last_applied_id, val))
    }
}

#[cfg(test)]
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
