//! Append-only record log (generic WAL primitive).
//!
//! This is the *generic* record log primitive intended for reuse by multiple
//! segment/index implementations.
//!
//! Vocabulary note:
//! - This module provides **integrity** (CRC + framing) and **crash-recovery posture**
//!   (strict vs best-effort tail).
//! - It does not, by itself, guarantee stable-storage **durability**; that depends on
//!   the underlying `Directory` implementation.
//!
//! ## Public invariants (must not change without a format bump)
//!
//! - **File header**: `[RECORDLOG_MAGIC][FORMAT_VERSION]` at byte 0.
//! - **Record framing** (little-endian):
//!   `len:u32 | crc32:u32 | payload bytes...`
//! - **Checksum**: `crc32fast` over the payload bytes.
//! - **Limits**: payload length is capped at `MAX_RECORD_BYTES`.
//!
//! ## Recovery posture
//!
//! This module supports both strict replay and best-effort replay.
//! Best-effort replay is the common WAL posture used by systems like SQLite:
//! scan forward validating checksums and stop at the first truncated tail record.

use crate::error::{PersistenceError, PersistenceResult};
use crate::formats::{FORMAT_VERSION, RECORDLOG_MAGIC};
use crate::storage::{self, Directory, FlushPolicy};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::sync::Arc;

/// Hard cap to avoid unbounded allocations on corrupt logs.
const MAX_RECORD_BYTES: u32 = 64 * 1024 * 1024; // 64 MiB

/// Read mode for record logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordLogReadMode {
    /// Any corruption/truncation is an error.
    Strict,
    /// Allow a torn tail record (e.g. after a crash) by treating certain decode
    /// failures as clean EOF.
    BestEffort,
}

/// A decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The raw payload bytes for this record (CRC-validated).
    pub payload: Vec<u8>,
}

/// Append-only record log writer.
pub struct RecordLogWriter {
    dir: Arc<dyn Directory>,
    path: String,
    header_checked: bool,
    w: Option<Box<dyn Write>>,
    flush_policy: FlushPolicy,
    since_flush: usize,
    write_buffer: Vec<u8>,
    write_buffer_limit: usize,
}

impl RecordLogWriter {
    /// Create a record log writer that appends to `path`.
    pub fn new(dir: impl Into<Arc<dyn Directory>>, path: impl Into<String>) -> Self {
        // Fast-by-default: buffer writes to reduce syscalls, and flush periodically.
        //
        // Contract note:
        // - `append_*` does not guarantee the bytes are visible to a reader until a `flush()`
        //   boundary (explicit or via policy). This is acceptable for WAL-style usage, where
        //   logs are typically replayed after process restart.
        Self::with_options(dir, path, FlushPolicy::EveryN(64), 64 * 1024)
    }

    /// Conservative default: no buffering + flush after each append.
    pub fn new_conservative(dir: impl Into<Arc<dyn Directory>>, path: impl Into<String>) -> Self {
        Self::with_options(dir, path, FlushPolicy::PerAppend, 0)
    }

    /// Create a record log writer with an explicit flush policy.
    pub fn with_flush_policy(
        dir: impl Into<Arc<dyn Directory>>,
        path: impl Into<String>,
        flush_policy: FlushPolicy,
    ) -> Self {
        Self::with_options(dir, path, flush_policy, 0)
    }

    /// Create a record log writer with explicit flush policy and write buffer size.
    ///
    /// `write_buffer_limit_bytes == 0` disables buffering (writes are issued on each append).
    pub fn with_options(
        dir: impl Into<Arc<dyn Directory>>,
        path: impl Into<String>,
        flush_policy: FlushPolicy,
        write_buffer_limit_bytes: usize,
    ) -> Self {
        Self {
            dir: dir.into(),
            path: path.into(),
            header_checked: false,
            w: None,
            flush_policy,
            since_flush: 0,
            write_buffer: Vec::new(),
            write_buffer_limit: write_buffer_limit_bytes,
        }
    }

    fn ensure_header(&mut self) -> PersistenceResult<()> {
        if self.header_checked {
            return Ok(());
        }
        if self.dir.exists(&self.path) {
            let mut r = self.dir.open_file(&self.path)?;
            let mut magic = [0u8; 4];
            r.read_exact(&mut magic)?;
            if magic != RECORDLOG_MAGIC {
                return Err(PersistenceError::Format("invalid recordlog magic".into()));
            }
            let version = r.read_u32::<LittleEndian>()?;
            if version != FORMAT_VERSION {
                return Err(PersistenceError::Format(
                    "recordlog version mismatch".into(),
                ));
            }
            self.header_checked = true;
            return Ok(());
        }

        let mut w = self.dir.create_file(&self.path)?;
        w.write_all(&RECORDLOG_MAGIC)?;
        w.write_u32::<LittleEndian>(FORMAT_VERSION)?;
        if self.flush_policy == FlushPolicy::PerAppend {
            w.flush()?;
        }
        self.header_checked = true;
        Ok(())
    }

    fn ensure_writer(&mut self) -> PersistenceResult<()> {
        if self.w.is_none() {
            self.w = Some(self.dir.append_file(&self.path)?);
        }
        Ok(())
    }

    fn drain_buffer_to_writer(&mut self) -> PersistenceResult<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        self.ensure_writer()?;
        let w = self.w.as_mut().expect("writer exists");
        w.write_all(&self.write_buffer)?;
        self.write_buffer.clear();
        Ok(())
    }

    /// Flush the underlying writer (if one is open).
    pub fn flush(&mut self) -> PersistenceResult<()> {
        self.drain_buffer_to_writer()?;
        if let Some(w) = self.w.as_mut() {
            w.flush()?;
        }
        self.since_flush = 0;
        Ok(())
    }

    /// Flush buffered bytes and attempt to make the record log durable on stable storage.
    ///
    /// This is an *opt-in* stronger guarantee than `flush()`:
    /// - `flush()` is a visibility boundary (userspace → OS / underlying writer).
    /// - `flush_and_sync()` additionally calls `sync_all` on the underlying file.
    ///
    /// Returns `NotSupported` if the underlying `Directory` does not provide `file_path()`.
    pub fn flush_and_sync(&mut self) -> PersistenceResult<()> {
        self.flush()?;
        storage::sync_file(&*self.dir, &self.path)?;
        storage::sync_parent_dir(&*self.dir, &self.path)?;
        Ok(())
    }

    /// Append one record containing `payload`.
    pub fn append_bytes(&mut self, payload: &[u8]) -> PersistenceResult<()> {
        self.ensure_header()?;
        if payload.len() > usize::try_from(MAX_RECORD_BYTES).unwrap_or(usize::MAX) {
            return Err(PersistenceError::Format(format!(
                "record payload too large: {} (max {})",
                payload.len(),
                MAX_RECORD_BYTES
            )));
        }
        let len_u32 = u32::try_from(payload.len())
            .map_err(|_| PersistenceError::Format("record length overflows u32".into()))?;

        // Small optimization: write the fixed 8-byte header in one call.
        let crc = crc32fast::hash(payload);
        let mut header = [0u8; 8];
        header[..4].copy_from_slice(&len_u32.to_le_bytes());
        header[4..].copy_from_slice(&crc.to_le_bytes());
        self.write_buffer.extend_from_slice(&header);
        self.write_buffer.extend_from_slice(payload);

        if self.write_buffer_limit > 0 && self.write_buffer.len() >= self.write_buffer_limit {
            self.drain_buffer_to_writer()?;
        } else if self.write_buffer_limit == 0 {
            // Unbuffered mode: write immediately.
            self.drain_buffer_to_writer()?;
        }

        self.since_flush = self.since_flush.saturating_add(1);
        match self.flush_policy {
            FlushPolicy::PerAppend => {
                self.flush()?;
                self.since_flush = 0;
            }
            FlushPolicy::EveryN(n) => {
                // Note: guards don't participate in exhaustiveness checking; handle `n` here.
                let n = n.max(1);
                if self.since_flush >= n {
                    self.flush()?;
                }
            }
            FlushPolicy::Manual => {}
        }
        Ok(())
    }

    /// Append one postcard-encoded value as a record payload.
    pub fn append_postcard<T: serde::Serialize>(&mut self, value: &T) -> PersistenceResult<()> {
        let payload =
            postcard::to_allocvec(value).map_err(|e| PersistenceError::Encode(e.to_string()))?;
        self.append_bytes(&payload)
    }
}

/// Sequential record log reader.
pub struct RecordLogReader {
    dir: Arc<dyn Directory>,
    path: String,
}

impl RecordLogReader {
    /// Create a record log reader for `path`.
    pub fn new(dir: impl Into<Arc<dyn Directory>>, path: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            path: path.into(),
        }
    }

    pub(crate) fn open_stream(&self) -> PersistenceResult<Box<dyn Read>> {
        let mut f = self.dir.open_file(&self.path)?;

        let mut magic = [0u8; 4];
        f.read_exact(&mut magic)?;
        if magic != RECORDLOG_MAGIC {
            return Err(PersistenceError::Format("invalid recordlog magic".into()));
        }
        let version = f.read_u32::<LittleEndian>()?;
        if version != FORMAT_VERSION {
            return Err(PersistenceError::Format(
                "recordlog version mismatch".into(),
            ));
        }
        Ok(f)
    }

    pub(crate) fn next_record_inner(
        &self,
        r: &mut dyn Read,
        mode: RecordLogReadMode,
    ) -> PersistenceResult<Option<Record>> {
        // Tail-aware length decode:
        // - clean EOF at record boundary => Ok(None)
        // - truncated length prefix => error in Strict, EOF in BestEffort
        let mut first = [0u8; 1];
        match r.read_exact(&mut first) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let mut rest = [0u8; 3];
        if let Err(e) = r.read_exact(&mut rest) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return match mode {
                    RecordLogReadMode::Strict => Err(e.into()),
                    RecordLogReadMode::BestEffort => Ok(None),
                };
            }
            return Err(e.into());
        }
        let len = u32::from_le_bytes([first[0], rest[0], rest[1], rest[2]]);
        if len > MAX_RECORD_BYTES {
            return Err(PersistenceError::Format(format!(
                "record length too large: {len} (max {MAX_RECORD_BYTES})"
            )));
        }

        let expected_crc = match r.read_u32::<LittleEndian>() {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return match mode {
                    RecordLogReadMode::Strict => Err(e.into()),
                    RecordLogReadMode::BestEffort => Ok(None),
                };
            }
            Err(e) => return Err(e.into()),
        };
        let mut payload = vec![0u8; len as usize];
        if let Err(e) = r.read_exact(&mut payload) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return match mode {
                    RecordLogReadMode::Strict => Err(e.into()),
                    RecordLogReadMode::BestEffort => Ok(None),
                };
            }
            return Err(e.into());
        }
        let got = crc32fast::hash(&payload);
        if got != expected_crc {
            return Err(PersistenceError::CrcMismatch {
                expected: expected_crc,
                actual: got,
            });
        }
        Ok(Some(Record { payload }))
    }

    /// Read all records from the log.
    pub fn read_all(&self, mode: RecordLogReadMode) -> PersistenceResult<Vec<Record>> {
        if !self.dir.exists(&self.path) {
            return Ok(vec![]);
        }
        let mut f = match self.open_stream() {
            Ok(f) => f,
            Err(PersistenceError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Best-effort recovery: if the file exists but is too small to even contain a
                // header, treat it as an empty log (crash during initial creation).
                match mode {
                    RecordLogReadMode::Strict => return Err(PersistenceError::Io(e)),
                    RecordLogReadMode::BestEffort => return Ok(vec![]),
                }
            }
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        while let Some(rec) = self.next_record_inner(&mut *f, mode)? {
            out.push(rec);
        }
        Ok(out)
    }

    /// Read all records as postcard-decoded values.
    pub fn read_all_postcard<T: serde::de::DeserializeOwned>(
        &self,
        mode: RecordLogReadMode,
    ) -> PersistenceResult<Vec<T>> {
        let records = self.read_all(mode)?;
        let mut out = Vec::with_capacity(records.len());
        for r in records {
            let v: T = postcard::from_bytes(&r.payload)
                .map_err(|e| PersistenceError::Decode(e.to_string()))?;
            out.push(v);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{FsDirectory, MemoryDirectory};
    use std::io::Read;

    fn read_all_bytes(dir: &Arc<dyn Directory>, path: &str) -> Vec<u8> {
        let mut f = dir.open_file(path).unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn recordlog_roundtrip_postcard() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct E {
            msg: String,
        }
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        let mut w = RecordLogWriter::new(dir.clone(), "log.bin");
        w.append_postcard(&E {
            msg: "東京".into()
        })
        .unwrap();
        w.append_postcard(&E {
            msg: "Москва".into(),
        })
        .unwrap();
        w.flush().unwrap();

        let r = RecordLogReader::new(dir, "log.bin");
        let xs: Vec<E> = r.read_all_postcard(RecordLogReadMode::Strict).unwrap();
        assert_eq!(xs.len(), 2);
        assert_eq!(xs[0].msg, "東京");
        assert_eq!(xs[1].msg, "Москва");
    }

    #[test]
    fn recordlog_tolerates_torn_tail_record() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct E {
            msg: String,
        }

        let tmp = tempfile::tempdir().unwrap();
        let dir = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);

        let mut w = RecordLogWriter::new(dir.clone(), "log.bin");
        w.append_postcard(&E {
            msg: "東京".into()
        })
        .unwrap();
        w.append_postcard(&E {
            msg: "Москва".into(),
        })
        .unwrap();
        w.flush().unwrap();

        let Some(path) = dir.file_path("log.bin") else {
            panic!("FsDirectory must return file_path()");
        };
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len().saturating_sub(3)); // tear last record
        std::fs::write(&path, bytes).unwrap();

        let r = RecordLogReader::new(dir, "log.bin");
        let xs: Vec<E> = r.read_all_postcard(RecordLogReadMode::BestEffort).unwrap();
        assert_eq!(xs.len(), 1);
        assert_eq!(xs[0].msg, "東京");
    }

    #[test]
    fn recordlog_truncated_length_prefix_is_error_in_strict_but_eof_in_best_effort() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct E {
            msg: String,
        }

        let tmp = tempfile::tempdir().unwrap();
        let dir = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);

        let mut w = RecordLogWriter::new(dir.clone(), "log.bin");
        w.append_postcard(&E {
            msg: "東京".into()
        })
        .unwrap();
        w.flush().unwrap();

        let Some(path) = dir.file_path("log.bin") else {
            panic!("FsDirectory must return file_path()");
        };
        let bytes = std::fs::read(&path).unwrap();

        // Keep header, then keep only 1 byte of the next record's u32 length prefix.
        let header_len = 4 + 4; // magic + version
        let truncated = &bytes[..header_len + 1];
        std::fs::write(&path, truncated).unwrap();

        let r = RecordLogReader::new(dir.clone(), "log.bin");
        assert!(r.read_all_postcard::<E>(RecordLogReadMode::Strict).is_err());

        let xs: Vec<E> = r.read_all_postcard(RecordLogReadMode::BestEffort).unwrap();
        assert_eq!(xs.len(), 0);
    }

    #[test]
    fn recordlog_writer_refuses_to_append_to_wrong_file() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        // Create a file with the wrong magic at the expected path.
        dir.atomic_write("log.bin", b"NOPE0000").unwrap();

        let mut w = RecordLogWriter::new(dir, "log.bin");
        let err = w.append_bytes(b"hello").unwrap_err();
        assert!(err.to_string().contains("invalid recordlog magic"));
    }

    #[test]
    fn recordlog_best_effort_treats_truncated_header_as_empty() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        // Write fewer than 8 bytes (magic+version), simulating a crash during initial create.
        dir.atomic_write("log.bin", b"\xAA\xBB\xCC").unwrap();

        let r = RecordLogReader::new(dir.clone(), "log.bin");
        assert!(r.read_all(RecordLogReadMode::Strict).is_err());
        let xs = r.read_all(RecordLogReadMode::BestEffort).unwrap();
        assert_eq!(xs.len(), 0);
    }

    #[test]
    fn recordlog_flush_policy_does_not_change_bytes() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct E {
            msg: String,
        }

        let make = |policy: FlushPolicy| {
            let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
            let mut w = RecordLogWriter::with_flush_policy(dir.clone(), "log.bin", policy);
            w.append_postcard(&E {
                msg: "東京".into()
            })
            .unwrap();
            w.append_postcard(&E {
                msg: "Москва".into(),
            })
            .unwrap();
            w.append_postcard(&E {
                msg: "التقى محمد بن سلمان".into(),
            })
            .unwrap();
            w.flush().unwrap();
            (dir.clone(), read_all_bytes(&dir, "log.bin"))
        };

        let (_d1, b1) = make(FlushPolicy::PerAppend);
        let (_d2, b2) = make(FlushPolicy::EveryN(64));
        let (_d3, b3) = make(FlushPolicy::Manual);
        assert_eq!(b1, b2);
        assert_eq!(b1, b3);
    }

    #[test]
    fn recordlog_buffered_and_unbuffered_produce_same_bytes() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct E {
            msg: String,
        }

        let make = |buf_limit: usize| {
            let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
            let mut w = RecordLogWriter::with_options(
                dir.clone(),
                "log.bin",
                FlushPolicy::Manual,
                buf_limit,
            );
            for i in 0..200usize {
                w.append_postcard(&E {
                    msg: format!("msg-{i}-東京-Москва-الرياض"),
                })
                .unwrap();
            }
            w.flush().unwrap();
            read_all_bytes(&dir, "log.bin")
        };

        let unbuffered = make(0);
        let buffered = make(64 * 1024);
        assert_eq!(unbuffered, buffered);
    }

    #[test]
    fn recordlog_flush_and_sync_requires_fs_backend() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct E {
            msg: String,
        }

        // MemoryDirectory cannot provide stable-storage barriers.
        let mem: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = RecordLogWriter::new(mem.clone(), "log.bin");
        w.append_postcard(&E { msg: "x".into() }).unwrap();
        let err = w.flush_and_sync().unwrap_err();
        assert!(matches!(err, PersistenceError::NotSupported(_)));

        // FsDirectory supports `file_path`, so sync succeeds.
        let tmp = tempfile::tempdir().unwrap();
        let fs = FsDirectory::new(tmp.path()).unwrap();
        let fs: Arc<dyn Directory> = Arc::new(fs);
        let mut w2 = RecordLogWriter::new(fs.clone(), "log.bin");
        w2.append_postcard(&E {
            msg: "東京".into()
        })
        .unwrap();
        w2.flush_and_sync().unwrap();

        let r = RecordLogReader::new(fs, "log.bin");
        let xs: Vec<E> = r.read_all_postcard(RecordLogReadMode::Strict).unwrap();
        assert_eq!(xs.len(), 1);
        assert_eq!(xs[0].msg, "東京");
    }
}
