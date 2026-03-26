//! Write-ahead log (WAL) for incremental updates.
//!
//! ## Generic design
//!
//! `WalWriter<E>` and `WalReader<E>` are generic over any entry type `E` that implements
//! `Serialize + DeserializeOwned`. Entry IDs are assigned by the writer and stored in the
//! frame header, not in the payload. On replay, entries are returned as `WalRecord<E>`
//! pairing the assigned entry ID with the deserialized payload.
//!
//! ## Public invariants (must not change without a format bump)
//!
//! - **Segment files live under `wal/`** and are named `wal_<id>.log`.
//! - **Segment ordering**: segments are replayed by numeric `<id>` (not lexicographic).
//! - **Segment header**: `[WAL_MAGIC][WAL_FORMAT_VERSION][start_entry_id:u64][segment_id:u64]`
//!   (little-endian for integers).
//! - **Entry ids are strictly increasing** across the concatenated replay stream.
//! - **Entry framing**: `[length:u32][entry_id:u64][crc32:u32][postcard payload...]`.
//! - **Checksum**: `crc32fast` over the postcard payload bytes.
//!
//! ## On-disk layout
//!
//! ```text
//! Segment file (wal/wal_<id>.log):
//!   [WAL_MAGIC:4][WAL_FORMAT_VERSION:u32][start_entry_id:u64][segment_id:u64]
//!   [entry 1][entry 2][...][entry N]
//!
//! Entry frame:
//!   [length:u32][entry_id:u64][crc32:u32][postcard payload...]
//!   length covers the entire frame (4 + 8 + 4 + payload_len).
//!   CRC covers only the postcard payload bytes.
//! ```
//!
//! All integers are little-endian.
//!
//! ## Recovery posture
//!
//! `WalReader::replay_best_effort()` matches the common WAL recovery stance used by
//! Kafka/Bitcask/SQLite-style systems: scan forward validating checksums and stop at
//! the first *truncated* tail record (torn write) in the **final** segment.
//!
//! Corruption in non-final segments is always an error.

use crate::error::{PersistenceError, PersistenceResult};
use crate::formats::{WAL_FORMAT_VERSION, WAL_MAGIC};
use crate::storage::{self, Directory, FlushPolicy};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::sync::Arc;

const MAX_WAL_ENTRY_PAYLOAD_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

// ---------------------------------------------------------------------------
// Domain-specific entry type (kept for backward compatibility with vicinity)
// ---------------------------------------------------------------------------

/// Segment-index WAL operations.
///
/// This is the concrete entry type for segment-based search indices. Use it as the
/// type parameter for `WalWriter<WalEntry>` / `WalReader<WalEntry>` when building
/// segment-lifecycle WALs.
///
/// For custom domains, define your own `#[derive(Serialize, Deserialize)]` enum
/// and use `WalWriter<YourEntry>` directly.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WalEntry {
    /// A new segment became visible.
    AddSegment {
        /// The new segment id.
        segment_id: u64,
        /// Number of documents in the new segment.
        doc_count: u32,
    },
    /// A merge transaction started (not yet committed).
    StartMerge {
        /// Merge transaction identifier.
        transaction_id: u64,
        /// Segments participating in the merge.
        segment_ids: Vec<u64>,
    },
    /// A merge transaction was cancelled (no visible changes).
    CancelMerge {
        /// Merge transaction identifier.
        transaction_id: u64,
        /// Segments that were participating in the merge.
        segment_ids: Vec<u64>,
    },
    /// A merge transaction completed and produced a new segment.
    EndMerge {
        /// Merge transaction identifier.
        transaction_id: u64,
        /// The new merged segment id.
        new_segment_id: u64,
        /// Old segments to remove.
        old_segment_ids: Vec<u64>,
        /// Deletes that occurred during merge and were remapped into the new segment.
        remapped_deletes: Vec<(u64, u32)>,
    },
    /// Logical deletes against existing segments.
    DeleteDocuments {
        /// Delete list as (segment_id, doc_id) pairs.
        deletes: Vec<(u64, u32)>,
    },
    /// A checkpoint was created (usually allowing WAL truncation).
    Checkpoint {
        /// Path to the checkpoint file.
        checkpoint_path: String,
        /// The last WAL entry included in that checkpoint.
        last_entry_id: u64,
    },
}

// ---------------------------------------------------------------------------
// WalRecord: entry + frame-assigned ID
// ---------------------------------------------------------------------------

/// A WAL entry paired with its frame-assigned entry ID.
#[derive(Debug, Clone)]
pub struct WalRecord<E> {
    /// Monotonic entry ID assigned by the WAL writer.
    pub entry_id: u64,
    /// The deserialized payload.
    pub payload: E,
}

// ---------------------------------------------------------------------------
// Segment header (unchanged)
// ---------------------------------------------------------------------------

/// Per-file header for a WAL segment.
#[derive(Debug, Clone, Copy)]
pub struct WalSegmentHeader {
    /// Magic bytes (should equal `WAL_MAGIC`).
    pub magic: [u8; 4],
    /// WAL format version.
    pub version: u32,
    /// First entry id present in this segment.
    pub start_entry_id: u64,
    /// WAL segment id.
    pub segment_id: u64,
}

impl WalSegmentHeader {
    /// Number of bytes in the serialized header.
    pub const SIZE: usize = 4 + 4 + 8 + 8;

    /// Write the header to a stream.
    pub fn write<W: Write>(&self, writer: &mut W) -> PersistenceResult<()> {
        use byteorder::{LittleEndian, WriteBytesExt};
        writer.write_all(&self.magic)?;
        writer.write_u32::<LittleEndian>(self.version)?;
        writer.write_u64::<LittleEndian>(self.start_entry_id)?;
        writer.write_u64::<LittleEndian>(self.segment_id)?;
        Ok(())
    }

    /// Read the header from a stream.
    pub fn read<R: Read>(reader: &mut R) -> PersistenceResult<Self> {
        use byteorder::{LittleEndian, ReadBytesExt};

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != WAL_MAGIC {
            return Err(PersistenceError::Format("invalid WAL magic".into()));
        }

        let version = reader.read_u32::<LittleEndian>()?;
        if version != WAL_FORMAT_VERSION {
            return Err(PersistenceError::Format(format!(
                "WAL version mismatch (got {version}, expected {WAL_FORMAT_VERSION})"
            )));
        }

        Ok(Self {
            magic,
            version,
            start_entry_id: reader.read_u64::<LittleEndian>()?,
            segment_id: reader.read_u64::<LittleEndian>()?,
        })
    }
}

// ---------------------------------------------------------------------------
// On-disk entry framing (generic)
// ---------------------------------------------------------------------------

/// Encode/decode WAL entries on disk.
///
/// Frame layout: `[length:u32][entry_id:u64][crc32:u32][postcard payload...]`
/// where `length` = 4 (len) + 8 (entry_id) + 4 (crc) + payload_len.
pub struct WalEntryOnDisk;

impl WalEntryOnDisk {
    fn read_u32_len<R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
    ) -> PersistenceResult<Option<u32>> {
        let mut first = [0u8; 1];
        match reader.read_exact(&mut first) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let mut rest = [0u8; 3];
        if let Err(e) = reader.read_exact(&mut rest) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return match mode {
                    WalReplayMode::Strict => Err(e.into()),
                    WalReplayMode::BestEffortTail => Ok(None),
                };
            }
            return Err(e.into());
        }

        let bytes = [first[0], rest[0], rest[1], rest[2]];
        Ok(Some(u32::from_le_bytes(bytes)))
    }

    /// Encode a WAL entry into bytes suitable for appending.
    pub fn encode<E: serde::Serialize>(entry_id: u64, entry: &E) -> PersistenceResult<Vec<u8>> {
        let payload =
            postcard::to_allocvec(entry).map_err(|e| PersistenceError::Encode(e.to_string()))?;
        let checksum = crc32fast::hash(&payload);

        // Frame: [length:u32][entry_id:u64][crc32:u32][payload...]
        let length_u64 = 4u64 + 8u64 + 4u64 + (payload.len() as u64);
        let length = u32::try_from(length_u64)
            .map_err(|_| PersistenceError::Format("WAL entry too large".into()))?;

        let mut encoded = Vec::with_capacity(4 + 8 + 4 + payload.len());
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&entry_id.to_le_bytes());
        encoded.extend_from_slice(&checksum.to_le_bytes());
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    /// Decode the next WAL entry, returning `Ok(None)` at EOF.
    ///
    /// Returns `(entry_id, payload_bytes)` for the caller to deserialize.
    pub fn decode_raw<R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
    ) -> PersistenceResult<Option<(u64, Vec<u8>)>> {
        use byteorder::{LittleEndian, ReadBytesExt};

        let Some(length) = Self::read_u32_len(reader, mode)? else {
            return Ok(None);
        };

        let entry_id = match reader.read_u64::<LittleEndian>() {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return match mode {
                    WalReplayMode::Strict => Err(e.into()),
                    WalReplayMode::BestEffortTail => Ok(None),
                };
            }
            Err(e) => return Err(e.into()),
        };

        let checksum = match reader.read_u32::<LittleEndian>() {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return match mode {
                    WalReplayMode::Strict => Err(e.into()),
                    WalReplayMode::BestEffortTail => Ok(None),
                };
            }
            Err(e) => return Err(e.into()),
        };

        // Frame overhead: 4 (len) + 8 (entry_id) + 4 (crc) = 16 bytes
        if length < 16 {
            return Err(PersistenceError::Format("WAL entry length < header".into()));
        }

        let payload_len = length as usize - 16;
        if payload_len > MAX_WAL_ENTRY_PAYLOAD_BYTES {
            return Err(PersistenceError::Format(format!(
                "WAL entry payload too large: {payload_len} bytes"
            )));
        }
        let mut payload = vec![0u8; payload_len];
        if let Err(e) = reader.read_exact(&mut payload) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return match mode {
                    WalReplayMode::Strict => Err(e.into()),
                    WalReplayMode::BestEffortTail => Ok(None),
                };
            }
            return Err(e.into());
        }

        let computed = crc32fast::hash(&payload);
        if computed != checksum {
            return Err(PersistenceError::CrcMismatch {
                expected: checksum,
                actual: computed,
            });
        }

        Ok(Some((entry_id, payload)))
    }

    /// Decode the next WAL entry and deserialize the payload.
    pub fn decode<E: serde::de::DeserializeOwned, R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
    ) -> PersistenceResult<Option<WalRecord<E>>> {
        let Some((entry_id, payload)) = Self::decode_raw(reader, mode)? else {
            return Ok(None);
        };
        let entry: E =
            postcard::from_bytes(&payload).map_err(|e| PersistenceError::Decode(e.to_string()))?;
        Ok(Some(WalRecord {
            entry_id,
            payload: entry,
        }))
    }
}

/// Error-handling posture for WAL replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReplayMode {
    /// Treat any corruption/truncation as an error.
    Strict,
    /// Treat a truncated tail (torn record) as EOF and return entries up to that point.
    ///
    /// Note: This does *not* mean "ignore corruption". CRC/decode failures remain errors.
    BestEffortTail,
}

// ---------------------------------------------------------------------------
// WAL segment enumeration
// ---------------------------------------------------------------------------

/// List WAL segment files, returning (segment_id, filename) pairs sorted by segment_id.
fn enumerate_wal_segments(dir: &dyn Directory) -> PersistenceResult<Vec<(u64, String)>> {
    let wal_files = dir.list_dir("wal")?;
    let mut segments: Vec<(u64, String)> = wal_files
        .into_iter()
        .filter(|n| n.ends_with(".log"))
        .filter_map(|n| {
            let raw = n.strip_prefix("wal_")?.strip_suffix(".log")?;
            let id = raw.parse::<u64>().ok()?;
            Some((id, n))
        })
        .collect();
    segments.sort_by_key(|(id, _)| *id);
    Ok(segments)
}

// ---------------------------------------------------------------------------
// WalWriter (generic)
// ---------------------------------------------------------------------------

/// WAL writer that appends entries to numbered segment files under `wal/`.
pub struct WalWriter<E> {
    directory: Arc<dyn Directory>,
    current_segment_id: u64,
    current_entry_id: u64,
    current_offset: u64,
    segment_size_limit: u64,
    wal_dir_ready: bool,
    current_path: Option<String>,
    current_file: Option<Box<dyn Write>>,
    flush_policy: FlushPolicy,
    since_flush: usize,
    write_buffer: Vec<u8>,
    write_buffer_limit: usize,
    _marker: PhantomData<E>,
}

impl<E: serde::Serialize + serde::de::DeserializeOwned> WalWriter<E> {
    /// Create a new WAL writer.
    ///
    /// Fast-by-default: buffered writes (64 KiB) + flush every 64 appends.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self::with_options(directory, FlushPolicy::EveryN(64), 64 * 1024)
    }

    /// Create a new WAL writer with an explicit flush policy.
    pub fn with_flush_policy(
        directory: impl Into<Arc<dyn Directory>>,
        flush_policy: FlushPolicy,
    ) -> Self {
        Self::with_options(directory, flush_policy, 0)
    }

    /// Create a new WAL writer with flush policy and write buffer size.
    ///
    /// `write_buffer_limit_bytes == 0` disables buffering (writes are issued on each append).
    pub fn with_options(
        directory: impl Into<Arc<dyn Directory>>,
        flush_policy: FlushPolicy,
        write_buffer_limit_bytes: usize,
    ) -> Self {
        Self {
            directory: directory.into(),
            current_segment_id: 1,
            current_entry_id: 1,
            current_offset: 0,
            segment_size_limit: 10 * 1024 * 1024,
            wal_dir_ready: false,
            current_path: None,
            current_file: None,
            flush_policy,
            since_flush: 0,
            write_buffer: Vec::new(),
            write_buffer_limit: write_buffer_limit_bytes,
            _marker: PhantomData,
        }
    }

    /// Set the target maximum size of a WAL segment file (in bytes).
    pub fn set_segment_size_limit_bytes(&mut self, bytes: u64) {
        let min = WalSegmentHeader::SIZE as u64 + 16;
        self.segment_size_limit = bytes.max(min);
    }

    /// Return the last assigned entry ID, or None if no entries have been appended.
    pub fn last_entry_id(&self) -> Option<u64> {
        if self.current_entry_id <= 1 {
            None
        } else {
            Some(self.current_entry_id - 1)
        }
    }

    /// Return the entry ID that will be assigned to the next appended entry.
    pub fn next_entry_id(&self) -> u64 {
        self.current_entry_id
    }

    /// Resume appending to an existing WAL (if present).
    ///
    /// If no `wal/` files exist, this is equivalent to `WalWriter::new`.
    /// If the last segment has a **torn tail record**, this function repairs it by
    /// truncating the file back to the last valid record boundary, then continues.
    pub fn resume(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        let directory: Arc<dyn Directory> = directory.into();

        let wal_segments = enumerate_wal_segments(&*directory)?;

        if wal_segments.is_empty() {
            return Ok(Self::new(directory));
        }

        let mut last_entry_id: u64 = 0;
        let mut last_seen_entry_id: Option<u64> = None;

        for (i, (segment_id, wal_file)) in wal_segments.iter().enumerate() {
            let wal_path = format!("wal/{wal_file}");
            let is_last = i + 1 == wal_segments.len();

            let mut f = directory.open_file(&wal_path)?;
            if is_last {
                let mut bytes = Vec::new();
                f.read_to_end(&mut bytes)?;

                let (valid_len, last_in_file) =
                    scan_last_segment_prefix(&bytes, last_seen_entry_id)?;

                if valid_len < bytes.len() {
                    directory.atomic_write(&wal_path, &bytes[..valid_len])?;
                    bytes.truncate(valid_len);
                }

                if let Some(id) = last_in_file {
                    last_entry_id = id;
                }

                let mut w =
                    Self::with_options(directory.clone(), FlushPolicy::EveryN(64), 64 * 1024);
                w.wal_dir_ready = true;
                w.current_segment_id = *segment_id;
                w.current_entry_id = last_entry_id.saturating_add(1).max(1);
                w.current_offset = u64::try_from(bytes.len()).map_err(|_| {
                    PersistenceError::Format("WAL file length overflows u64".into())
                })?;
                w.current_path = Some(wal_path);
                w.current_file = None;
                return Ok(w);
            }

            // Non-last segment: decode strictly and track monotone entry_id.
            let _h = WalSegmentHeader::read(&mut f)?;
            while let Some((entry_id, _payload)) =
                WalEntryOnDisk::decode_raw(&mut f, WalReplayMode::Strict)?
            {
                if let Some(prev) = last_seen_entry_id {
                    if entry_id <= prev {
                        return Err(PersistenceError::Format(format!(
                            "WAL entry_id is not strictly increasing (prev={prev}, got={entry_id})"
                        )));
                    }
                }
                last_seen_entry_id = Some(entry_id);
                last_entry_id = entry_id;
            }
        }

        Err(PersistenceError::InvalidState(
            "WAL resume internal error: missing last segment".into(),
        ))
    }

    fn ensure_wal_dir(&mut self) -> PersistenceResult<()> {
        if !self.wal_dir_ready {
            self.directory.create_dir_all("wal")?;
            // Safety: prevent silent entry-id collision when called via new() on existing WAL
            if self.current_entry_id == 1 && self.current_segment_id == 1 {
                let existing = enumerate_wal_segments(&*self.directory)?;
                if !existing.is_empty() {
                    return Err(PersistenceError::InvalidState(
                        "WAL directory already contains segments; use WalWriter::resume() to continue an existing WAL".into(),
                    ));
                }
            }
            self.wal_dir_ready = true;
        }
        Ok(())
    }

    fn ensure_segment_open(&mut self, start_entry_id: u64) -> PersistenceResult<String> {
        self.ensure_wal_dir()?;
        let wal_path = match &self.current_path {
            Some(p) => p.clone(),
            None => format!("wal/wal_{}.log", self.current_segment_id),
        };

        if self.current_offset == 0 {
            let mut file = self.directory.create_file(&wal_path)?;
            WalSegmentHeader {
                magic: WAL_MAGIC,
                version: WAL_FORMAT_VERSION,
                start_entry_id,
                segment_id: self.current_segment_id,
            }
            .write(&mut file)?;
            if self.flush_policy == FlushPolicy::PerAppend {
                file.flush()?;
            }
            self.current_offset = WalSegmentHeader::SIZE as u64;
            self.current_path = Some(wal_path.clone());
            self.current_file = Some(file);
        } else if self.current_file.is_none() {
            self.current_file = Some(self.directory.append_file(&wal_path)?);
        }
        Ok(wal_path)
    }

    fn drain_buffer_to_file(&mut self) -> PersistenceResult<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        let f = self
            .current_file
            .as_mut()
            .expect("segment file must be open");
        f.write_all(&self.write_buffer)?;
        self.current_offset += self.write_buffer.len() as u64;
        self.write_buffer.clear();
        Ok(())
    }

    /// Flush the current segment file if open.
    pub fn flush(&mut self) -> PersistenceResult<()> {
        self.drain_buffer_to_file()?;
        if let Some(f) = self.current_file.as_mut() {
            f.flush()?;
        }
        self.since_flush = 0;
        Ok(())
    }

    /// Flush buffered bytes and attempt to make the current WAL segment durable.
    ///
    /// Returns `NotSupported` if the underlying `Directory` does not provide `file_path()`.
    pub fn flush_and_sync(&mut self) -> PersistenceResult<()> {
        self.flush()?;
        let Some(path) = self.current_path.as_deref() else {
            return Ok(());
        };
        storage::sync_file(&*self.directory, path)?;
        storage::sync_parent_dir(&*self.directory, path)?;
        Ok(())
    }

    /// Append an entry, returning its assigned entry id.
    pub fn append(&mut self, entry: &E) -> PersistenceResult<u64> {
        let entry_id = self.current_entry_id;

        let _wal_path = self.ensure_segment_open(entry_id)?;

        let encoded = WalEntryOnDisk::encode(entry_id, entry)?;

        let projected =
            self.current_offset + (self.write_buffer.len() as u64) + (encoded.len() as u64);
        if projected > self.segment_size_limit
            && self.current_offset > WalSegmentHeader::SIZE as u64
        {
            self.flush()?;
            self.current_segment_id += 1;
            self.current_offset = 0;
            self.current_path = None;
            self.current_file = None;
            self.since_flush = 0;

            let _ = self.ensure_segment_open(entry_id)?;
        }

        self.write_buffer.extend_from_slice(&encoded);
        if self.write_buffer_limit == 0 || self.write_buffer.len() >= self.write_buffer_limit {
            self.drain_buffer_to_file()?;
        }

        self.since_flush = self.since_flush.saturating_add(1);
        match self.flush_policy {
            FlushPolicy::PerAppend => {
                self.flush()?;
            }
            FlushPolicy::EveryN(n) => {
                let n = n.max(1);
                if self.since_flush >= n {
                    self.flush()?;
                }
            }
            FlushPolicy::Manual => {}
        }

        self.current_entry_id += 1;
        Ok(entry_id)
    }
}

// ---------------------------------------------------------------------------
// scan_last_segment_prefix (format-aware, type-agnostic)
// ---------------------------------------------------------------------------

/// Scan the last WAL segment bytes and return:
/// - the valid prefix length (byte offset) that ends on a record boundary, and
/// - the last entry id present in that valid prefix (if any).
fn scan_last_segment_prefix(
    bytes: &[u8],
    last_seen_entry_id: Option<u64>,
) -> PersistenceResult<(usize, Option<u64>)> {
    if bytes.len() < WalSegmentHeader::SIZE {
        return Ok((0, None));
    }

    let mut cur = std::io::Cursor::new(bytes);
    let header = match WalSegmentHeader::read(&mut cur) {
        Ok(h) => h,
        Err(PersistenceError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok((0, None));
        }
        Err(e) => return Err(e),
    };

    let mut first_entry_id_in_segment: Option<u64> = None;
    let mut last_id = last_seen_entry_id;

    loop {
        let start_pos = cur.position() as usize;
        match WalEntryOnDisk::decode_raw(&mut cur, WalReplayMode::BestEffortTail)? {
            Some((entry_id, _payload)) => {
                if first_entry_id_in_segment.is_none() {
                    first_entry_id_in_segment = Some(entry_id);
                }
                if let Some(prev) = last_id {
                    if entry_id <= prev {
                        return Err(PersistenceError::Format(format!(
                            "WAL entry_id is not strictly increasing (prev={prev}, got={entry_id})"
                        )));
                    }
                }
                last_id = Some(entry_id);
            }
            None => {
                let prefix = start_pos.max(WalSegmentHeader::SIZE).min(bytes.len());
                if let Some(first) = first_entry_id_in_segment {
                    if first != header.start_entry_id {
                        return Err(PersistenceError::Format(format!(
                            "WAL segment start_entry_id mismatch (header={}, first_entry={})",
                            header.start_entry_id, first
                        )));
                    }
                }
                return Ok((prefix, last_id));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WalReader (generic)
// ---------------------------------------------------------------------------

/// WAL reader that replays entries from all segment files under `wal/`.
pub struct WalReader<E> {
    directory: Arc<dyn Directory>,
    _marker: PhantomData<E>,
}

impl<E: serde::de::DeserializeOwned> WalReader<E> {
    /// Create a new WAL reader.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
            _marker: PhantomData,
        }
    }

    /// Replay all WAL entries in sorted segment-id order.
    pub fn replay(&self) -> PersistenceResult<Vec<WalRecord<E>>> {
        self.replay_with_mode(WalReplayMode::Strict)
    }

    /// Best-effort replay: stop at first truncated tail record in the final segment.
    pub fn replay_best_effort(&self) -> PersistenceResult<Vec<WalRecord<E>>> {
        self.replay_with_mode(WalReplayMode::BestEffortTail)
    }

    fn replay_with_mode(&self, mode: WalReplayMode) -> PersistenceResult<Vec<WalRecord<E>>> {
        let mut records = Vec::new();
        let wal_segments = enumerate_wal_segments(&*self.directory)?;
        let last_segment_id = wal_segments.last().map(|(id, _)| *id);

        for (segment_id, wal_file) in wal_segments {
            let wal_path = format!("wal/{wal_file}");
            let mut file = self.directory.open_file(&wal_path)?;
            let header = match WalSegmentHeader::read(&mut file) {
                Ok(h) => h,
                Err(PersistenceError::Io(e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof
                        && mode == WalReplayMode::BestEffortTail
                        && Some(segment_id) == last_segment_id =>
                {
                    break;
                }
                Err(e) => return Err(e),
            };

            let mut first_entry_id_in_segment: Option<u64> = None;
            let mut last_seen_entry_id: Option<u64> =
                records.last().map(|r: &WalRecord<E>| r.entry_id);

            let segment_mode = match mode {
                WalReplayMode::Strict => WalReplayMode::Strict,
                WalReplayMode::BestEffortTail => {
                    if Some(segment_id) == last_segment_id {
                        WalReplayMode::BestEffortTail
                    } else {
                        WalReplayMode::Strict
                    }
                }
            };

            while let Some(record) = WalEntryOnDisk::decode::<E, _>(&mut file, segment_mode)? {
                if first_entry_id_in_segment.is_none() {
                    first_entry_id_in_segment = Some(record.entry_id);
                }
                if let Some(prev) = last_seen_entry_id {
                    if record.entry_id <= prev {
                        return Err(PersistenceError::Format(format!(
                            "WAL entry_id is not strictly increasing (prev={prev}, got={})",
                            record.entry_id
                        )));
                    }
                }
                last_seen_entry_id = Some(record.entry_id);
                records.push(record);
            }

            if let Some(first_id) = first_entry_id_in_segment {
                if first_id != header.start_entry_id {
                    return Err(PersistenceError::Format(format!(
                        "WAL segment start_entry_id mismatch (header={}, first_entry={})",
                        header.start_entry_id, first_id
                    )));
                }
            }
        }

        Ok(records)
    }
}

// ---------------------------------------------------------------------------
// WalMaintenance (type-agnostic)
// ---------------------------------------------------------------------------

/// Maintenance helpers for WAL directories (metadata + truncation).
pub struct WalMaintenance {
    directory: Arc<dyn Directory>,
}

/// Per-segment metadata derived from decoding a segment.
#[derive(Debug, Clone)]
pub struct WalSegmentRange {
    /// Numeric segment id (from filename).
    pub segment_id: u64,
    /// Full WAL path (e.g. `wal/wal_3.log`).
    pub path: String,
    /// Header-declared start entry id.
    pub start_entry_id: u64,
    /// Max entry id seen in this segment (None if the segment contains no valid entries).
    pub end_entry_id: Option<u64>,
}

impl WalMaintenance {
    /// Create a WAL maintenance helper for a directory backend.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// Return per-segment entry-id ranges by decoding segments strictly.
    pub fn segment_ranges_strict(&self) -> PersistenceResult<Vec<WalSegmentRange>> {
        let wal_segments = enumerate_wal_segments(&*self.directory)?;

        let mut out = Vec::new();
        for (segment_id, wal_file) in wal_segments {
            let path = format!("wal/{wal_file}");
            let mut f = self.directory.open_file(&path)?;
            let header = WalSegmentHeader::read(&mut f)?;
            let mut end: Option<u64> = None;
            let mut first: Option<u64> = None;
            while let Some((entry_id, _payload)) =
                WalEntryOnDisk::decode_raw(&mut f, WalReplayMode::Strict)?
            {
                if first.is_none() {
                    first = Some(entry_id);
                }
                end = Some(entry_id);
            }
            if let Some(first_id) = first {
                if first_id != header.start_entry_id {
                    return Err(PersistenceError::Format(format!(
                        "WAL segment start_entry_id mismatch (header={}, first_entry={})",
                        header.start_entry_id, first_id
                    )));
                }
            }

            out.push(WalSegmentRange {
                segment_id,
                path,
                start_entry_id: header.start_entry_id,
                end_entry_id: end,
            });
        }
        Ok(out)
    }

    /// Delete WAL segments that are fully covered by a checkpoint at `last_entry_id`.
    ///
    /// Returns the number of deleted segment files.
    pub fn truncate_prefix(&self, last_entry_id: u64) -> PersistenceResult<usize> {
        let ranges = self.segment_ranges_strict()?;
        let mut deleted = 0usize;
        for seg in ranges {
            let Some(end) = seg.end_entry_id else {
                continue;
            };
            if end <= last_entry_id {
                self.directory.delete(&seg.path)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryDirectory;
    use std::io::Read;

    fn write_wal_segment(
        dir: &Arc<dyn Directory>,
        seg_id: u64,
        start_entry_id: u64,
        entries: &[(u64, WalEntry)], // (entry_id, payload)
    ) {
        dir.create_dir_all("wal").unwrap();
        let path = format!("wal/wal_{seg_id}.log");
        let mut f = dir.create_file(&path).unwrap();
        WalSegmentHeader {
            magic: WAL_MAGIC,
            version: WAL_FORMAT_VERSION,
            start_entry_id,
            segment_id: seg_id,
        }
        .write(&mut f)
        .unwrap();
        for (eid, e) in entries {
            let bytes = WalEntryOnDisk::encode(*eid, e).unwrap();
            f.write_all(&bytes).unwrap();
        }
        f.flush().unwrap();
    }

    fn read_all(dir: &Arc<dyn Directory>, path: &str) -> Vec<u8> {
        let mut f = dir.open_file(path).unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn wal_best_effort_tolerates_truncated_length_prefix_in_last_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        write_wal_segment(
            &dir,
            1,
            1,
            &[(
                1,
                WalEntry::AddSegment {
                    segment_id: 1,
                    doc_count: 1,
                },
            )],
        );
        write_wal_segment(
            &dir,
            2,
            2,
            &[(
                2,
                WalEntry::AddSegment {
                    segment_id: 2,
                    doc_count: 1,
                },
            )],
        );

        let bytes = read_all(&dir, "wal/wal_2.log");
        let truncated = &bytes[..WalSegmentHeader::SIZE + 1];
        dir.atomic_write("wal/wal_2.log", truncated).unwrap();

        let r = WalReader::<WalEntry>::new(dir.clone());
        assert!(r.replay().is_err());
        let records = r.replay_best_effort().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].entry_id, 1);
    }

    #[test]
    fn wal_best_effort_tolerates_torn_header_in_last_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        write_wal_segment(
            &dir,
            1,
            1,
            &[(
                1,
                WalEntry::AddSegment {
                    segment_id: 1,
                    doc_count: 1,
                },
            )],
        );

        let torn_header = vec![0u8; 3];
        dir.atomic_write("wal/wal_2.log", &torn_header).unwrap();

        let r = WalReader::<WalEntry>::new(dir.clone());
        assert!(r.replay().is_err());

        let out = r.replay_best_effort().unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].entry_id, 1);
    }

    #[test]
    fn wal_roundtrip_replay_in_memory() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::new(dir.clone());

        w.append(&WalEntry::AddSegment {
            segment_id: 7,
            doc_count: 3,
        })
        .unwrap();

        w.append(&WalEntry::DeleteDocuments {
            deletes: vec![(7, 1), (7, 2)],
        })
        .unwrap();

        w.flush().unwrap();

        let r = WalReader::<WalEntry>::new(dir);
        let records = r.replay().unwrap();
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].entry_id, 1);
        match &records[0].payload {
            WalEntry::AddSegment {
                segment_id,
                doc_count,
            } => {
                assert_eq!(*segment_id, 7);
                assert_eq!(*doc_count, 3);
            }
            other => panic!("unexpected entry[0]: {other:?}"),
        }

        assert_eq!(records[1].entry_id, 2);
        match &records[1].payload {
            WalEntry::DeleteDocuments { deletes } => {
                assert_eq!(deletes, &vec![(7, 1), (7, 2)]);
            }
            other => panic!("unexpected entry[1]: {other:?}"),
        }
    }

    #[test]
    fn wal_rejects_bad_checksum() {
        let entry = WalEntry::DeleteDocuments {
            deletes: vec![(7, 1)],
        };
        let mut bytes = WalEntryOnDisk::encode(1, &entry).unwrap();
        *bytes.last_mut().unwrap() ^= 0xFF;

        let mut cur = std::io::Cursor::new(bytes);
        let err =
            WalEntryOnDisk::decode::<WalEntry, _>(&mut cur, WalReplayMode::Strict).unwrap_err();
        assert!(err.to_string().contains("crc mismatch"));
    }

    #[test]
    fn wal_reader_rejects_bad_magic() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        dir.create_dir_all("wal").unwrap();

        let mut f = dir.create_file("wal/wal_1.log").unwrap();
        f.write_all(b"NOPE").unwrap();
        f.flush().unwrap();

        let r = WalReader::<WalEntry>::new(dir);
        let err = r.replay().unwrap_err();
        assert!(err.to_string().contains("invalid WAL magic"));
    }

    #[test]
    fn wal_reader_sorts_by_numeric_segment_id_not_lexicographic() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        write_wal_segment(
            &dir,
            10,
            10,
            &[(
                10,
                WalEntry::AddSegment {
                    segment_id: 10,
                    doc_count: 1,
                },
            )],
        );
        write_wal_segment(
            &dir,
            2,
            2,
            &[(
                2,
                WalEntry::AddSegment {
                    segment_id: 2,
                    doc_count: 1,
                },
            )],
        );

        let r = WalReader::<WalEntry>::new(dir);
        let records = r.replay().unwrap();
        assert_eq!(records.len(), 2);

        let ids: Vec<u64> = records.iter().map(|r| r.entry_id).collect();
        assert_eq!(ids, vec![2, 10]);
    }

    #[test]
    fn wal_best_effort_only_tolerates_torn_tail_in_last_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        write_wal_segment(
            &dir,
            1,
            1,
            &[(
                1,
                WalEntry::AddSegment {
                    segment_id: 1,
                    doc_count: 1,
                },
            )],
        );
        write_wal_segment(
            &dir,
            2,
            2,
            &[(
                2,
                WalEntry::AddSegment {
                    segment_id: 2,
                    doc_count: 1,
                },
            )],
        );

        let mut bytes = read_all(&dir, "wal/wal_2.log");
        bytes.truncate(bytes.len().saturating_sub(3));
        dir.atomic_write("wal/wal_2.log", &bytes).unwrap();

        let r = WalReader::<WalEntry>::new(dir.clone());
        assert!(r.replay().is_err());

        let records = r.replay_best_effort().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].entry_id, 1);
    }

    #[test]
    fn wal_best_effort_does_not_ignore_corruption_in_non_last_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        write_wal_segment(
            &dir,
            1,
            1,
            &[(
                1,
                WalEntry::AddSegment {
                    segment_id: 1,
                    doc_count: 1,
                },
            )],
        );
        let mut bytes = read_all(&dir, "wal/wal_1.log");
        *bytes.last_mut().unwrap() ^= 0xFF;
        dir.atomic_write("wal/wal_1.log", &bytes).unwrap();

        write_wal_segment(
            &dir,
            2,
            2,
            &[(
                2,
                WalEntry::AddSegment {
                    segment_id: 2,
                    doc_count: 1,
                },
            )],
        );

        let r = WalReader::<WalEntry>::new(dir);
        assert!(r.replay_best_effort().is_err());
    }

    #[test]
    fn wal_flush_policy_does_not_change_bytes() {
        let make = |policy: FlushPolicy| {
            let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
            let mut w = WalWriter::<WalEntry>::with_options(dir.clone(), policy, 64 * 1024);
            w.append(&WalEntry::AddSegment {
                segment_id: 7,
                doc_count: 3,
            })
            .unwrap();
            w.append(&WalEntry::DeleteDocuments {
                deletes: vec![(7, 1), (7, 2)],
            })
            .unwrap();
            w.flush().unwrap();
            read_all(&dir, "wal/wal_1.log")
        };

        let b1 = make(FlushPolicy::PerAppend);
        let b2 = make(FlushPolicy::EveryN(64));
        let b3 = make(FlushPolicy::Manual);
        assert_eq!(b1, b2);
        assert_eq!(b1, b3);
    }

    #[test]
    fn wal_buffered_and_unbuffered_produce_same_bytes() {
        let make = |buf_limit: usize| {
            let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
            let mut w =
                WalWriter::<WalEntry>::with_options(dir.clone(), FlushPolicy::Manual, buf_limit);
            for i in 0..100u64 {
                w.append(&WalEntry::AddSegment {
                    segment_id: i + 1,
                    doc_count: (i as u32) % 1000,
                })
                .unwrap();
            }
            w.flush().unwrap();
            read_all(&dir, "wal/wal_1.log")
        };

        let unbuffered = make(0);
        let buffered = make(64 * 1024);
        assert_eq!(unbuffered, buffered);
    }

    #[test]
    fn wal_resume_continues_entry_ids_and_appends() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        {
            let mut w = WalWriter::<WalEntry>::new(dir.clone());
            w.append(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 3,
            })
            .unwrap();
            w.append(&WalEntry::DeleteDocuments {
                deletes: vec![(1, 2)],
            })
            .unwrap();
            w.flush().unwrap();
        }

        let mut w = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
        let id3 = w
            .append(&WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 7,
            })
            .unwrap();
        assert_eq!(id3, 3);
        w.flush().unwrap();

        let r = WalReader::<WalEntry>::new(dir);
        let records = r.replay().unwrap();
        assert_eq!(records.len(), 3);
        let ids: Vec<u64> = records.iter().map(|r| r.entry_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn wal_resume_repairs_torn_tail_then_allows_strict_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = crate::storage::FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);

        {
            let mut w = WalWriter::<WalEntry>::new(dir.clone());
            w.append(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 3,
            })
            .unwrap();
            w.append(&WalEntry::DeleteDocuments {
                deletes: vec![(1, 2)],
            })
            .unwrap();
            w.flush().unwrap();
        }

        let wal_path = "wal/wal_1.log";
        let Some(fs_path) = dir.file_path(wal_path) else {
            panic!("FsDirectory must return file_path()");
        };
        let mut bytes = std::fs::read(&fs_path).unwrap();
        bytes.truncate(bytes.len().saturating_sub(3));
        std::fs::write(&fs_path, &bytes).unwrap();

        let r = WalReader::<WalEntry>::new(dir.clone());
        assert!(r.replay().is_err());

        let mut w = WalWriter::<WalEntry>::resume(dir.clone()).unwrap();
        let id2 = w
            .append(&WalEntry::DeleteDocuments {
                deletes: vec![(1, 0)],
            })
            .unwrap();
        assert_eq!(id2, 2);
        w.flush().unwrap();

        let out = r.replay().unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn wal_flush_and_sync_requires_fs_backend() {
        let mem: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::new(mem.clone());
        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        let err = w.flush_and_sync().unwrap_err();
        assert!(matches!(err, PersistenceError::NotSupported(_)));

        let tmp = tempfile::tempdir().unwrap();
        let fs = crate::storage::FsDirectory::new(tmp.path()).unwrap();
        let fs: Arc<dyn Directory> = Arc::new(fs);
        let mut w2 = WalWriter::<WalEntry>::new(fs.clone());
        w2.append(&WalEntry::AddSegment {
            segment_id: 7,
            doc_count: 3,
        })
        .unwrap();
        w2.flush_and_sync().unwrap();

        let r = WalReader::<WalEntry>::new(fs);
        let out = r.replay().unwrap();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn wal_generic_with_custom_entry_type() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        enum CustomOp {
            Insert { key: String, value: String },
            Delete { key: String },
        }

        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<CustomOp>::new(dir.clone());

        w.append(&CustomOp::Insert {
            key: "hello".into(),
            value: "world".into(),
        })
        .unwrap();
        w.append(&CustomOp::Delete {
            key: "hello".into(),
        })
        .unwrap();
        w.flush().unwrap();

        let r = WalReader::<CustomOp>::new(dir);
        let records = r.replay().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].entry_id, 1);
        assert_eq!(
            records[0].payload,
            CustomOp::Insert {
                key: "hello".into(),
                value: "world".into()
            }
        );
        assert_eq!(records[1].entry_id, 2);
        assert_eq!(
            records[1].payload,
            CustomOp::Delete {
                key: "hello".into()
            }
        );
    }
}
