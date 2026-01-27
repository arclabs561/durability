//! Write-ahead log (WAL) for incremental updates.
//!
//! ## Public invariants (must not change without a format bump)
//!
//! - **Segment files live under `wal/`** and are named `wal_<id>.log`.
//! - **Segment ordering**: segments are replayed by numeric `<id>` (not lexicographic).
//! - **Segment header**: `[WAL_MAGIC][FORMAT_VERSION][start_entry_id:u64][segment_id:u64]`
//!   (little-endian for integers).
//! - **Entry ids are strictly increasing** across the concatenated replay stream.
//! - **Entry framing**: `[length:u32][type:u8][crc32:u32][postcard payload...]`.
//! - **Checksum**: `crc32fast` over the postcard payload bytes.
//!
//! ## Recovery posture
//!
//! `WalReader::replay_best_effort()` matches the common WAL recovery stance used by
//! Kafka/Bitcask/SQLite-style systems: scan forward validating checksums and stop at
//! the first *truncated* tail record (torn write) in the **final** segment.
//!
//! Corruption in non-final segments is always an error.

use crate::error::{PersistenceError, PersistenceResult};
use crate::formats::{FORMAT_VERSION, WAL_MAGIC};
use crate::storage::{self, Directory, FlushPolicy};
use std::io::{Read, Write};
use std::sync::Arc;

const MAX_WAL_ENTRY_PAYLOAD_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// An entry in the write-ahead log.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WalEntry {
    /// A new segment became visible.
    AddSegment {
        /// Monotonic entry id assigned by the WAL writer.
        entry_id: u64,
        /// The new segment id.
        segment_id: u64,
        /// Number of documents in the new segment.
        doc_count: u32,
    },
    /// A merge transaction started (not yet committed).
    StartMerge {
        /// Monotonic entry id assigned by the WAL writer.
        entry_id: u64,
        /// Merge transaction identifier.
        transaction_id: u64,
        /// Segments participating in the merge.
        segment_ids: Vec<u64>,
    },
    /// A merge transaction was cancelled (no visible changes).
    CancelMerge {
        /// Monotonic entry id assigned by the WAL writer.
        entry_id: u64,
        /// Merge transaction identifier.
        transaction_id: u64,
        /// Segments that were participating in the merge.
        segment_ids: Vec<u64>,
    },
    /// A merge transaction completed and produced a new segment.
    EndMerge {
        /// Monotonic entry id assigned by the WAL writer.
        entry_id: u64,
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
        /// Monotonic entry id assigned by the WAL writer.
        entry_id: u64,
        /// Delete list as (segment_id, doc_id) pairs.
        deletes: Vec<(u64, u32)>,
    },
    /// A checkpoint was created (usually allowing WAL truncation).
    Checkpoint {
        /// Monotonic entry id assigned by the WAL writer.
        entry_id: u64,
        /// Path to the checkpoint file.
        checkpoint_path: String,
        /// The last WAL entry included in that checkpoint.
        last_entry_id: u64,
    },
}

/// Per-file header for a WAL segment.
#[repr(C)]
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
        if version != FORMAT_VERSION {
            return Err(PersistenceError::Format(format!(
                "WAL version mismatch (got {version}, expected {FORMAT_VERSION})"
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

/// Helper for encoding/decoding WAL entries on disk.
pub struct WalEntryOnDisk;

impl WalEntryOnDisk {
    fn read_u32_len<R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
    ) -> PersistenceResult<Option<u32>> {
        // Distinguish:
        // - clean EOF at record boundary (0 bytes available) => Ok(None)
        // - truncated length prefix (1-3 bytes available) => error in Strict, EOF in BestEffortTail
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

    fn entry_type(entry: &WalEntry) -> u8 {
        match entry {
            WalEntry::AddSegment { .. } => 0,
            WalEntry::StartMerge { .. } => 1,
            WalEntry::CancelMerge { .. } => 2,
            WalEntry::EndMerge { .. } => 3,
            WalEntry::DeleteDocuments { .. } => 4,
            WalEntry::Checkpoint { .. } => 5,
        }
    }

    /// Encode a WAL entry into bytes suitable for appending.
    pub fn encode(entry: &WalEntry) -> PersistenceResult<Vec<u8>> {
        let payload =
            postcard::to_allocvec(entry).map_err(|e| PersistenceError::Encode(e.to_string()))?;
        let checksum = crc32fast::hash(&payload);

        let entry_type = Self::entry_type(entry);

        let length_u64 = 4u64 + 1u64 + 4u64 + (payload.len() as u64);
        let length = u32::try_from(length_u64)
            .map_err(|_| PersistenceError::Format("WAL entry too large".into()))?;

        // Small optimization: avoid `WriteBytesExt` overhead and repeated reallocations.
        let mut encoded = Vec::with_capacity(4 + 1 + 4 + payload.len());
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.push(entry_type);
        encoded.extend_from_slice(&checksum.to_le_bytes());
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    /// Decode the next WAL entry, returning `Ok(None)` at EOF.
    ///
    /// In `BestEffortTail` mode, common “torn tail” failures are treated as EOF:
    /// if we hit `UnexpectedEof` while decoding a record, stop and return entries
    /// up to that point.
    ///
    /// All other failures (CRC mismatch, decode/type mismatch) are treated as errors.
    pub fn decode<R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
    ) -> PersistenceResult<Option<WalEntry>> {
        use byteorder::{LittleEndian, ReadBytesExt};

        let Some(length) = Self::read_u32_len(reader, mode)? else {
            return Ok(None);
        };
        let entry_type = match reader.read_u8() {
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

        if length < 9 {
            return Err(PersistenceError::Format("WAL entry length < header".into()));
        }

        let payload_len = length as usize - 9;
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

        let entry: WalEntry =
            postcard::from_bytes(&payload).map_err(|e| PersistenceError::Decode(e.to_string()))?;

        let expected_type = Self::entry_type(&entry);
        if entry_type != expected_type {
            return Err(PersistenceError::Format(format!(
                "WAL entry type mismatch (header={entry_type}, decoded={expected_type})"
            )));
        }
        Ok(Some(entry))
    }
}

/// Error-handling posture for WAL replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReplayMode {
    /// Treat any corruption/truncation as an error.
    Strict,
    /// Treat a truncated tail (torn record) as EOF and return entries up to that point.
    ///
    /// Note: This does *not* mean “ignore corruption”. CRC/decode/type failures remain errors.
    BestEffortTail,
}

/// WAL writer that appends entries to numbered segment files under `wal/`.
pub struct WalWriter {
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
}

impl WalWriter {
    /// Create a new WAL writer.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        // Fast-by-default: buffer writes to reduce syscalls, and flush periodically.
        //
        // Contract note:
        // - `append` does not guarantee the bytes are visible to a reader until a `flush()`
        //   boundary (explicit or via policy). WAL replay is typically done after process
        //   restart, so this is an acceptable default for throughput-focused systems.
        Self::with_options(directory, FlushPolicy::EveryN(64), 64 * 1024)
    }

    /// Conservative default: no buffering + flush after each append.
    pub fn new_conservative(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self::with_options(directory, FlushPolicy::PerAppend, 0)
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
        }
    }

    /// Set the target maximum size of a WAL segment file (in bytes).
    ///
    /// Notes:
    /// - This is a hint used for segment rotation; it is not a strict hard cap because
    ///   a single entry can exceed the limit and will still be written.
    /// - This is primarily useful for tests and for deployments that want predictable
    ///   WAL segment sizing for truncation/GC behavior.
    pub fn set_segment_size_limit_bytes(&mut self, bytes: u64) {
        // Keep a minimum large enough to hold the header plus at least one record header.
        let min = WalSegmentHeader::SIZE as u64 + 16;
        self.segment_size_limit = bytes.max(min);
    }

    /// Resume appending to an existing WAL (if present).
    ///
    /// Behavior:
    /// - If no `wal/` files exist, this is equivalent to `WalWriter::new`.
    /// - If the last segment has a **torn tail record**, this function repairs it by
    ///   truncating the file back to the last valid record boundary, then continues.
    /// - Any *corruption* (CRC/type/format errors) is returned as an error.
    ///
    /// Note: this function uses the same defaults as `WalWriter::new` (buffered writer,
    /// periodic flush policy).
    pub fn resume(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        let directory: Arc<dyn Directory> = directory.into();

        // If wal/ doesn't exist, list_dir returns empty for `FsDirectory` and for
        // `MemoryDirectory` unless keys exist under that prefix.
        let wal_files = directory.list_dir("wal")?;
        let mut wal_segments: Vec<(u64, String)> = wal_files
            .into_iter()
            .filter(|n| n.ends_with(".log"))
            .filter_map(|n| {
                let raw = n.strip_prefix("wal_")?.strip_suffix(".log")?;
                let id = raw.parse::<u64>().ok()?;
                Some((id, n))
            })
            .collect();
        wal_segments.sort_by_key(|(id, _)| *id);

        if wal_segments.is_empty() {
            return Ok(Self::new(directory));
        }

        // Validate non-final segments strictly and track last entry id.
        let mut last_entry_id: u64 = 0;
        let mut last_seen_entry_id: Option<u64> = None;

        for (i, (segment_id, wal_file)) in wal_segments.iter().enumerate() {
            let wal_path = format!("wal/{wal_file}");
            let is_last = i + 1 == wal_segments.len();

            let mut f = directory.open_file(&wal_path)?;
            if is_last {
                // For the last segment, read the whole file so we can repair torn tails.
                let mut bytes = Vec::new();
                f.read_to_end(&mut bytes)?;

                let (valid_len, last_in_file) =
                    scan_last_segment_prefix(&bytes, last_seen_entry_id)?;

                if valid_len < bytes.len() {
                    // Torn tail: repair by truncating back to the last valid boundary.
                    directory.atomic_write(&wal_path, &bytes[..valid_len])?;
                    bytes.truncate(valid_len);
                }

                if let Some(id) = last_in_file {
                    last_entry_id = id;
                }

                // Prepare a writer positioned at the end of the repaired file.
                let mut w =
                    Self::with_options(directory.clone(), FlushPolicy::EveryN(64), 64 * 1024);
                w.wal_dir_ready = true;
                w.current_segment_id = *segment_id;
                w.current_entry_id = last_entry_id.saturating_add(1).max(1);
                w.current_offset = u64::try_from(bytes.len()).map_err(|_| {
                    PersistenceError::Format("WAL file length overflows u64".into())
                })?;
                w.current_path = Some(wal_path);
                w.current_file = None; // lazily reopened for append on first write
                return Ok(w);
            }

            // Non-last segment: decode strictly and update monotone entry_id tracking.
            let _h = WalSegmentHeader::read(&mut f)?;
            loop {
                match WalEntryOnDisk::decode(&mut f, WalReplayMode::Strict)? {
                    Some(e) => {
                        let entry_id = match &e {
                            WalEntry::AddSegment { entry_id, .. }
                            | WalEntry::StartMerge { entry_id, .. }
                            | WalEntry::CancelMerge { entry_id, .. }
                            | WalEntry::EndMerge { entry_id, .. }
                            | WalEntry::DeleteDocuments { entry_id, .. }
                            | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
                        };
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
                    None => break,
                }
            }
        }

        // Unreachable: loop always returns on the last segment.
        Err(PersistenceError::InvalidState(
            "WAL resume internal error: missing last segment".into(),
        ))
    }

    fn ensure_wal_dir(&mut self) -> PersistenceResult<()> {
        if !self.wal_dir_ready {
            self.directory.create_dir_all("wal")?;
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
                version: FORMAT_VERSION,
                start_entry_id,
                segment_id: self.current_segment_id,
            }
            .write(&mut file)?;
            // Only flush header eagerly under PerAppend. Under other policies, header bytes
            // are still written immediately, but flush is deferred.
            if self.flush_policy == FlushPolicy::PerAppend {
                file.flush()?;
            }
            self.current_offset = WalSegmentHeader::SIZE as u64;
            self.current_path = Some(wal_path.clone());
            self.current_file = Some(file);
        } else if self.current_file.is_none() {
            // Defensive fallback: if we lost the handle, reopen for append.
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

    /// Flush buffered bytes and attempt to make the current WAL segment durable on stable storage.
    ///
    /// This is an *opt-in* stronger guarantee than `flush()`:
    /// - `flush()` is a visibility boundary (userspace → OS / underlying writer).
    /// - `flush_and_sync()` additionally calls `sync_all` on the current segment file.
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
    pub fn append(&mut self, mut entry: WalEntry) -> PersistenceResult<u64> {
        let entry_id = self.current_entry_id;

        // Ensure entry_id fields are consistent (cheap invariant).
        match &mut entry {
            WalEntry::AddSegment { entry_id: id, .. }
            | WalEntry::StartMerge { entry_id: id, .. }
            | WalEntry::CancelMerge { entry_id: id, .. }
            | WalEntry::EndMerge { entry_id: id, .. }
            | WalEntry::DeleteDocuments { entry_id: id, .. }
            | WalEntry::Checkpoint { entry_id: id, .. } => *id = entry_id,
        }

        // Ensure we have an open segment before we decide about rotation.
        let _wal_path = self.ensure_segment_open(entry_id)?;

        let encoded = WalEntryOnDisk::encode(&entry)?;

        // Segment rotation check uses (already-written bytes + buffered bytes + next entry).
        let projected =
            self.current_offset + (self.write_buffer.len() as u64) + (encoded.len() as u64);
        if projected > self.segment_size_limit
            && self.current_offset > WalSegmentHeader::SIZE as u64
        {
            // Finish current segment cleanly: write any buffered bytes, flush according to policy,
            // then rotate.
            self.flush()?;
            self.current_segment_id += 1;
            self.current_offset = 0;
            self.current_path = None;
            self.current_file = None;
            self.since_flush = 0;

            // Open the new segment and continue.
            let _ = self.ensure_segment_open(entry_id)?;
        }

        self.write_buffer.extend_from_slice(&encoded);
        if self.write_buffer_limit > 0 && self.write_buffer.len() >= self.write_buffer_limit {
            self.drain_buffer_to_file()?;
        } else if self.write_buffer_limit == 0 {
            // Unbuffered mode: write immediately.
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

/// Scan the last WAL segment bytes and return:
/// - the valid prefix length (byte offset) that ends on a record boundary, and
/// - the last entry id present in that valid prefix (if any).
///
/// This is used to repair torn tail records by truncating the segment file.
fn scan_last_segment_prefix(
    bytes: &[u8],
    last_seen_entry_id: Option<u64>,
) -> PersistenceResult<(usize, Option<u64>)> {
    // If the file is too small to even contain a header, treat it as a torn create.
    // Returning 0 causes the caller to truncate it to empty and then rewrite a fresh header
    // on the next append.
    if bytes.len() < WalSegmentHeader::SIZE {
        return Ok((0, None));
    }

    let mut cur = std::io::Cursor::new(bytes);
    let header = match WalSegmentHeader::read(&mut cur) {
        Ok(h) => h,
        // If the header itself is torn/truncated, treat it as empty (torn create).
        Err(PersistenceError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok((0, None));
        }
        Err(e) => return Err(e),
    };

    let mut first_entry_id_in_segment: Option<u64> = None;
    let mut last_id = last_seen_entry_id;

    loop {
        let start_pos = cur.position() as usize;
        match WalEntryOnDisk::decode(&mut cur, WalReplayMode::BestEffortTail)? {
            Some(e) => {
                let entry_id = match &e {
                    WalEntry::AddSegment { entry_id, .. }
                    | WalEntry::StartMerge { entry_id, .. }
                    | WalEntry::CancelMerge { entry_id, .. }
                    | WalEntry::EndMerge { entry_id, .. }
                    | WalEntry::DeleteDocuments { entry_id, .. }
                    | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
                };
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
                // EOF at boundary OR a torn tail record. In either case, the last valid
                // boundary is the start of this would-be record.
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

/// WAL reader that replays entries from all segment files under `wal/`.
pub struct WalReader {
    directory: Arc<dyn Directory>,
}

impl WalReader {
    /// Create a new WAL reader.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// Replay all WAL entries in sorted segment-id order.
    pub fn replay(&self) -> PersistenceResult<Vec<WalEntry>> {
        self.replay_with_mode(WalReplayMode::Strict)
    }

    /// Best-effort replay: stop at first truncated tail record in the final segment.
    pub fn replay_best_effort(&self) -> PersistenceResult<Vec<WalEntry>> {
        self.replay_with_mode(WalReplayMode::BestEffortTail)
    }

    fn replay_with_mode(&self, mode: WalReplayMode) -> PersistenceResult<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let wal_files = self.directory.list_dir("wal")?;

        // IMPORTANT: do not sort lexicographically (`wal_10.log` < `wal_2.log`).
        // Instead, parse numeric segment ids and sort by that.
        let mut wal_segments: Vec<(u64, String)> = wal_files
            .into_iter()
            .filter(|n| n.ends_with(".log"))
            .filter_map(|n| {
                let raw = n.strip_prefix("wal_")?.strip_suffix(".log")?;
                let id = raw.parse::<u64>().ok()?;
                Some((id, n))
            })
            .collect();
        wal_segments.sort_by_key(|(id, _)| *id);
        let last_segment_id = wal_segments.last().map(|(id, _)| *id);

        for (segment_id, wal_file) in wal_segments {
            let wal_path = format!("wal/{wal_file}");
            let mut file = self.directory.open_file(&wal_path)?;
            // Best-effort tolerance: if the *final* segment has a torn header (crash during
            // create/write of the segment header), treat it as “no more WAL”.
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
            let mut last_seen_entry_id: Option<u64> = entries.last().map(|e| match e {
                WalEntry::AddSegment { entry_id, .. }
                | WalEntry::StartMerge { entry_id, .. }
                | WalEntry::CancelMerge { entry_id, .. }
                | WalEntry::EndMerge { entry_id, .. }
                | WalEntry::DeleteDocuments { entry_id, .. }
                | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
            });
            // Best-effort tail tolerance applies only to the final segment.
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
            loop {
                match WalEntryOnDisk::decode(&mut file, segment_mode)? {
                    Some(e) => {
                        if first_entry_id_in_segment.is_none() {
                            first_entry_id_in_segment = Some(match &e {
                                WalEntry::AddSegment { entry_id, .. }
                                | WalEntry::StartMerge { entry_id, .. }
                                | WalEntry::CancelMerge { entry_id, .. }
                                | WalEntry::EndMerge { entry_id, .. }
                                | WalEntry::DeleteDocuments { entry_id, .. }
                                | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
                            });
                        }
                        let entry_id = match &e {
                            WalEntry::AddSegment { entry_id, .. }
                            | WalEntry::StartMerge { entry_id, .. }
                            | WalEntry::CancelMerge { entry_id, .. }
                            | WalEntry::EndMerge { entry_id, .. }
                            | WalEntry::DeleteDocuments { entry_id, .. }
                            | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
                        };
                        if let Some(prev) = last_seen_entry_id {
                            if entry_id <= prev {
                                return Err(PersistenceError::Format(format!(
                                    "WAL entry_id is not strictly increasing (prev={prev}, got={entry_id})"
                                )));
                            }
                        }
                        last_seen_entry_id = Some(entry_id);
                        entries.push(e);
                    }
                    None => break,
                }
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

        Ok(entries)
    }
}

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
        let wal_files = self.directory.list_dir("wal")?;
        let mut wal_segments: Vec<(u64, String)> = wal_files
            .into_iter()
            .filter(|n| n.ends_with(".log"))
            .filter_map(|n| {
                let raw = n.strip_prefix("wal_")?.strip_suffix(".log")?;
                let id = raw.parse::<u64>().ok()?;
                Some((id, n))
            })
            .collect();
        wal_segments.sort_by_key(|(id, _)| *id);

        let mut out = Vec::new();
        for (segment_id, wal_file) in wal_segments {
            let path = format!("wal/{wal_file}");
            let mut f = self.directory.open_file(&path)?;
            let header = WalSegmentHeader::read(&mut f)?;
            let mut end: Option<u64> = None;
            let mut first: Option<u64> = None;
            loop {
                match WalEntryOnDisk::decode(&mut f, WalReplayMode::Strict)? {
                    Some(e) => {
                        let id = match &e {
                            WalEntry::AddSegment { entry_id, .. }
                            | WalEntry::StartMerge { entry_id, .. }
                            | WalEntry::CancelMerge { entry_id, .. }
                            | WalEntry::EndMerge { entry_id, .. }
                            | WalEntry::DeleteDocuments { entry_id, .. }
                            | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
                        };
                        if first.is_none() {
                            first = Some(id);
                        }
                        end = Some(id);
                    }
                    None => break,
                }
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
    /// Deletes a segment file if `end_entry_id <= last_entry_id`.
    /// Empty/torn segments (no `end_entry_id`) are not deleted.
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
        entries: &[WalEntry],
    ) {
        dir.create_dir_all("wal").unwrap();
        let path = format!("wal/wal_{seg_id}.log");
        let mut f = dir.create_file(&path).unwrap();
        WalSegmentHeader {
            magic: WAL_MAGIC,
            version: FORMAT_VERSION,
            start_entry_id,
            segment_id: seg_id,
        }
        .write(&mut f)
        .unwrap();
        for e in entries {
            let bytes = WalEntryOnDisk::encode(e).unwrap();
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
        dir.create_dir_all("wal").unwrap();

        write_wal_segment(
            &dir,
            1,
            1,
            &[WalEntry::AddSegment {
                entry_id: 1,
                segment_id: 1,
                doc_count: 1,
            }],
        );
        write_wal_segment(
            &dir,
            2,
            2,
            &[WalEntry::AddSegment {
                entry_id: 2,
                segment_id: 2,
                doc_count: 1,
            }],
        );

        // Tear inside the length prefix of the last record:
        // keep header + 1 byte of the record (so the u32 length is truncated).
        let bytes = read_all(&dir, "wal/wal_2.log");
        let truncated = &bytes[..WalSegmentHeader::SIZE + 1];
        dir.atomic_write("wal/wal_2.log", truncated).unwrap();

        let r = WalReader::new(dir.clone());
        assert!(r.replay().is_err());
        let entries = r.replay_best_effort().unwrap();
        assert_eq!(entries.len(), 1);
        let id = match &entries[0] {
            WalEntry::AddSegment { entry_id, .. } => *entry_id,
            _ => panic!("unexpected entry"),
        };
        assert_eq!(id, 1);
    }

    #[test]
    fn wal_best_effort_tolerates_torn_header_in_last_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        dir.create_dir_all("wal").unwrap();

        // Segment 1: one valid entry.
        write_wal_segment(
            &dir,
            1,
            1,
            &[WalEntry::AddSegment {
                entry_id: 1,
                segment_id: 1,
                doc_count: 1,
            }],
        );

        // Segment 2: torn header (fewer bytes than WalSegmentHeader::SIZE).
        // Important: to model a *torn* header (crash mid-write), truncate *inside the magic*,
        // so the reader sees `UnexpectedEof` rather than “bad magic”.
        let torn_header = vec![0u8; 3];
        dir.atomic_write("wal/wal_2.log", &torn_header).unwrap();

        let r = WalReader::new(dir.clone());
        // Strict must error (cannot read header).
        assert!(r.replay().is_err());

        // Best-effort should ignore the torn final segment and return the prefix.
        let out = r.replay_best_effort().unwrap();
        assert_eq!(out.len(), 1);
        let id = match &out[0] {
            WalEntry::AddSegment { entry_id, .. } => *entry_id,
            _ => panic!("unexpected entry"),
        };
        assert_eq!(id, 1);
    }

    #[test]
    fn wal_roundtrip_replay_in_memory() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::new(dir.clone());

        w.append(WalEntry::AddSegment {
            entry_id: 0,
            segment_id: 7,
            doc_count: 3,
        })
        .unwrap();

        w.append(WalEntry::DeleteDocuments {
            entry_id: 0,
            deletes: vec![(7, 1), (7, 2)],
        })
        .unwrap();

        w.flush().unwrap();

        let r = WalReader::new(dir);
        let entries = r.replay().unwrap();
        assert_eq!(entries.len(), 2);

        match &entries[0] {
            WalEntry::AddSegment {
                entry_id,
                segment_id,
                doc_count,
            } => {
                assert_eq!(*entry_id, 1);
                assert_eq!(*segment_id, 7);
                assert_eq!(*doc_count, 3);
            }
            other => panic!("unexpected entry[0]: {other:?}"),
        }

        match &entries[1] {
            WalEntry::DeleteDocuments { entry_id, deletes } => {
                assert_eq!(*entry_id, 2);
                assert_eq!(deletes, &vec![(7, 1), (7, 2)]);
            }
            other => panic!("unexpected entry[1]: {other:?}"),
        }
    }

    #[test]
    fn wal_rejects_bad_entry_type() {
        let entry = WalEntry::AddSegment {
            entry_id: 123,
            segment_id: 7,
            doc_count: 3,
        };
        let mut bytes = WalEntryOnDisk::encode(&entry).unwrap();
        // Flip the entry_type byte (length u32 = 4 bytes, then entry_type).
        bytes[4] ^= 0xFF;

        let mut cur = std::io::Cursor::new(bytes);
        let err = WalEntryOnDisk::decode(&mut cur, WalReplayMode::Strict).unwrap_err();
        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn wal_rejects_bad_checksum() {
        let entry = WalEntry::DeleteDocuments {
            entry_id: 1,
            deletes: vec![(7, 1)],
        };
        let mut bytes = WalEntryOnDisk::encode(&entry).unwrap();
        // Corrupt last byte of payload.
        *bytes.last_mut().unwrap() ^= 0xFF;

        let mut cur = std::io::Cursor::new(bytes);
        let err = WalEntryOnDisk::decode(&mut cur, WalReplayMode::Strict).unwrap_err();
        assert!(err.to_string().contains("crc mismatch"));
    }

    #[test]
    fn wal_reader_rejects_bad_magic() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        dir.create_dir_all("wal").unwrap();

        // Minimal file with wrong magic.
        let mut f = dir.create_file("wal/wal_1.log").unwrap();
        f.write_all(b"NOPE").unwrap();
        f.flush().unwrap();

        let r = WalReader::new(dir);
        let err = r.replay().unwrap_err();
        assert!(err.to_string().contains("invalid WAL magic"));
    }

    #[test]
    fn wal_reader_sorts_by_numeric_segment_id_not_lexicographic() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        // Create wal_10.log and wal_2.log such that lexicographic sorting would read 10 before 2.
        write_wal_segment(
            &dir,
            10,
            10,
            &[WalEntry::AddSegment {
                entry_id: 10,
                segment_id: 10,
                doc_count: 1,
            }],
        );
        write_wal_segment(
            &dir,
            2,
            2,
            &[WalEntry::AddSegment {
                entry_id: 2,
                segment_id: 2,
                doc_count: 1,
            }],
        );

        let r = WalReader::new(dir);
        let entries = r.replay().unwrap();
        assert_eq!(entries.len(), 2);

        let ids: Vec<u64> = entries
            .iter()
            .map(|e| match e {
                WalEntry::AddSegment { entry_id, .. }
                | WalEntry::StartMerge { entry_id, .. }
                | WalEntry::CancelMerge { entry_id, .. }
                | WalEntry::EndMerge { entry_id, .. }
                | WalEntry::DeleteDocuments { entry_id, .. }
                | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
            })
            .collect();

        assert_eq!(ids, vec![2, 10]);
    }

    #[test]
    fn wal_best_effort_only_tolerates_torn_tail_in_last_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        dir.create_dir_all("wal").unwrap();

        // Segment 1: one valid entry.
        write_wal_segment(
            &dir,
            1,
            1,
            &[WalEntry::AddSegment {
                entry_id: 1,
                segment_id: 1,
                doc_count: 1,
            }],
        );

        // Segment 2: write header + one entry, then tear the last few bytes.
        write_wal_segment(
            &dir,
            2,
            2,
            &[WalEntry::AddSegment {
                entry_id: 2,
                segment_id: 2,
                doc_count: 1,
            }],
        );
        // Tear the end of the last segment file to simulate a crash mid-record.
        let mut bytes = read_all(&dir, "wal/wal_2.log");
        bytes.truncate(bytes.len().saturating_sub(3));
        dir.atomic_write("wal/wal_2.log", &bytes).unwrap();

        let r = WalReader::new(dir.clone());
        // Strict replay should error (truncated record in last segment).
        assert!(r.replay().is_err());

        // Best-effort should return the prefix: segment 1 entry, and possibly none from seg2
        // depending on where the tear landed (we tore inside the record, so it should stop
        // before yielding that entry).
        let entries = r.replay_best_effort().unwrap();
        assert_eq!(entries.len(), 1);
        let id = match &entries[0] {
            WalEntry::AddSegment { entry_id, .. } => *entry_id,
            _ => panic!("unexpected entry"),
        };
        assert_eq!(id, 1);
    }

    #[test]
    fn wal_best_effort_does_not_ignore_corruption_in_non_last_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        dir.create_dir_all("wal").unwrap();

        // Segment 1: write header + one entry, then corrupt payload (CRC mismatch).
        write_wal_segment(
            &dir,
            1,
            1,
            &[WalEntry::AddSegment {
                entry_id: 1,
                segment_id: 1,
                doc_count: 1,
            }],
        );
        let mut bytes = read_all(&dir, "wal/wal_1.log");
        *bytes.last_mut().unwrap() ^= 0xFF;
        dir.atomic_write("wal/wal_1.log", &bytes).unwrap();

        // Segment 2: valid entry, making segment 1 non-final.
        write_wal_segment(
            &dir,
            2,
            2,
            &[WalEntry::AddSegment {
                entry_id: 2,
                segment_id: 2,
                doc_count: 1,
            }],
        );

        let r = WalReader::new(dir);
        // Best-effort must still error because corruption is in a non-final segment.
        assert!(r.replay_best_effort().is_err());
    }

    #[test]
    fn wal_flush_policy_does_not_change_bytes() {
        let make = |policy: FlushPolicy| {
            let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
            let mut w = WalWriter::with_options(dir.clone(), policy, 64 * 1024);
            w.append(WalEntry::AddSegment {
                entry_id: 0,
                segment_id: 7,
                doc_count: 3,
            })
            .unwrap();
            w.append(WalEntry::DeleteDocuments {
                entry_id: 0,
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
            let mut w = WalWriter::with_options(dir.clone(), FlushPolicy::Manual, buf_limit);
            for i in 0..100u64 {
                w.append(WalEntry::AddSegment {
                    entry_id: 0,
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

        // Create a WAL with two entries.
        {
            let mut w = WalWriter::new(dir.clone());
            w.append(WalEntry::AddSegment {
                entry_id: 0,
                segment_id: 1,
                doc_count: 3,
            })
            .unwrap();
            w.append(WalEntry::DeleteDocuments {
                entry_id: 0,
                deletes: vec![(1, 2)],
            })
            .unwrap();
            w.flush().unwrap();
        }

        // Resume and append one more entry; entry ids must continue.
        let mut w = WalWriter::resume(dir.clone()).unwrap();
        let id3 = w
            .append(WalEntry::AddSegment {
                entry_id: 0,
                segment_id: 2,
                doc_count: 7,
            })
            .unwrap();
        assert_eq!(id3, 3);
        w.flush().unwrap();

        let r = WalReader::new(dir);
        let entries = r.replay().unwrap();
        assert_eq!(entries.len(), 3);
        let ids: Vec<u64> = entries
            .iter()
            .map(|e| match e {
                WalEntry::AddSegment { entry_id, .. }
                | WalEntry::StartMerge { entry_id, .. }
                | WalEntry::CancelMerge { entry_id, .. }
                | WalEntry::EndMerge { entry_id, .. }
                | WalEntry::DeleteDocuments { entry_id, .. }
                | WalEntry::Checkpoint { entry_id, .. } => *entry_id,
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn wal_resume_repairs_torn_tail_then_allows_strict_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = crate::storage::FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);

        // Create a WAL with two entries.
        {
            let mut w = WalWriter::new(dir.clone());
            w.append(WalEntry::AddSegment {
                entry_id: 0,
                segment_id: 1,
                doc_count: 3,
            })
            .unwrap();
            w.append(WalEntry::DeleteDocuments {
                entry_id: 0,
                deletes: vec![(1, 2)],
            })
            .unwrap();
            w.flush().unwrap();
        }

        // Tear the last few bytes off the file, simulating a crash mid-record in the tail.
        let wal_path = "wal/wal_1.log";
        let Some(fs_path) = dir.file_path(wal_path) else {
            panic!("FsDirectory must return file_path()");
        };
        let mut bytes = std::fs::read(&fs_path).unwrap();
        bytes.truncate(bytes.len().saturating_sub(3));
        std::fs::write(&fs_path, &bytes).unwrap();

        // Strict replay should fail pre-repair.
        let r = WalReader::new(dir.clone());
        assert!(r.replay().is_err());

        // Resume should repair the torn tail, then appending should yield a strict-replayable WAL.
        let mut w = WalWriter::resume(dir.clone()).unwrap();
        let id2 = w
            .append(WalEntry::DeleteDocuments {
                entry_id: 0,
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
        // MemoryDirectory cannot provide stable-storage barriers.
        let mem: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::new(mem.clone());
        w.append(WalEntry::AddSegment {
            entry_id: 0,
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        let err = w.flush_and_sync().unwrap_err();
        assert!(matches!(err, PersistenceError::NotSupported(_)));

        // FsDirectory supports `file_path`, so sync succeeds.
        let tmp = tempfile::tempdir().unwrap();
        let fs = crate::storage::FsDirectory::new(tmp.path()).unwrap();
        let fs: Arc<dyn Directory> = Arc::new(fs);
        let mut w2 = WalWriter::new(fs.clone());
        w2.append(WalEntry::AddSegment {
            entry_id: 0,
            segment_id: 7,
            doc_count: 3,
        })
        .unwrap();
        w2.flush_and_sync().unwrap();

        let r = WalReader::new(fs);
        let out = r.replay().unwrap();
        assert_eq!(out.len(), 1);
    }
}
