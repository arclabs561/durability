//! Write-ahead log (WAL) for incremental updates.
//!
//! ## Generic design
//!
//! Entry IDs are assigned by the writer and stored in the frame header, not in
//! the payload. The core WAL stores raw payload bytes. With the default
//! `postcard` feature, `WalWriter<E>` and `WalReader<E>` also provide typed
//! serde/postcard append and replay helpers.
//!
//! ## Public invariants (must not change without a format bump)
//!
//! - **Segment files live under `wal/`** and are named `wal_<id>.log`.
//! - **Segment ordering**: segments are replayed by numeric `<id>` (not lexicographic).
//! - **Segment header**: `[WAL_MAGIC][WAL_FORMAT_VERSION][start_entry_id:u64][segment_id:u64]`
//!   (little-endian for integers).
//! - **Entry ids are strictly increasing** across the concatenated replay stream.
//! - **Entry framing**: `[length:u32][entry_id:u64][crc32:u32][payload bytes...]`.
//! - **Checksum**: format v3 uses `crc32fast` over `length | entry_id | payload`.
//!   Legacy v2 WAL segments used a payload-only checksum and remain readable.
//!
//! ## On-disk layout
//!
//! ```text
//! Segment file (wal/wal_<id>.log):
//!   [WAL_MAGIC:4][WAL_FORMAT_VERSION:u32][start_entry_id:u64][segment_id:u64]
//!   [entry 1][entry 2][...][entry N]
//!
//! Entry frame:
//!   [length:u32][entry_id:u64][crc32:u32][payload bytes...]
//!   length covers the entire frame (4 + 8 + 4 + payload_len).
//!   CRC covers length, entry_id, and payload bytes for v3 segments.
//! ```
//!
//! All integers are little-endian.
//!
//! ## Writer variants
//!
//! - [`WalWriter`]: single-owner writer. Use [`WalWriter::open`] as the recommended
//!   entry point (creates or resumes).
//! - [`SyncWalWriter`]: thread-safe wrapper around a single [`WalWriter`]. Multiple
//!   threads can call [`SyncWalWriter::append_bytes`] concurrently; durable byte
//!   appends are serialized through [`SyncWalWriter::append_bytes_durable`].
//!   The default `postcard` feature also provides typed `append` and
//!   `append_durable` helpers.
//!
//! ## Tuning
//!
//! - **Flush policy**: [`FlushPolicy::PerAppend`], [`FlushPolicy::EveryN`],
//!   [`FlushPolicy::Interval`], or [`FlushPolicy::Manual`]. Controls when the write
//!   buffer is pushed to the OS (not stable-storage sync -- use `flush_and_sync` for that).
//! - **Segment rotation**: by size ([`WalWriter::set_segment_size_limit_bytes`]) and/or
//!   age ([`WalWriter::set_segment_max_age`]).
//! - **Preallocation**: [`WalWriter::set_preallocate_bytes`] avoids block allocation on
//!   the write path.
//! - **Segment recycling**: [`WalWriter::set_recycle_capacity`] + [`WalWriter::recycle_segment`]
//!   reuses truncated segment files on the next rotation.
//! - **Observer**: [`WalObserver`] receives lifecycle events (append, flush, sync, rotate,
//!   truncation veto).
//!
//! ## Recovery posture
//!
//! `WalReader::replay_best_effort()` matches the common WAL recovery stance used by
//! Kafka/Bitcask/SQLite-style systems: scan forward validating checksums and stop at
//! the first *truncated* tail record (torn write) in the **final** segment.
//!
//! Corruption in non-final segments is always an error.
//!
//! ## Error policy
//!
//! The writer **poisons** itself after:
//! - A `write_all` failure during buffer drain (indeterminate file state).
//! - A segment creation failure during rotation (inconsistent segment ID).
//! - A `flush_and_sync` failure (false durability).
//!
//! A poisoned writer rejects all subsequent appends with `InvalidState`.

use crate::error::{PersistenceError, PersistenceResult};
use crate::formats::{WAL_FORMAT_VERSION, WAL_MAGIC};
use crate::storage::{self, Directory, FlushPolicy};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::sync::Arc;

const MAX_WAL_ENTRY_PAYLOAD_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Observer for WAL writer lifecycle events.
///
/// All methods have default no-op implementations. Implement only the events
/// you care about.
///
/// The observer is called synchronously on the write path, so implementations
/// should be lightweight (e.g. increment an atomic counter, push to a channel).
pub trait WalObserver: Send + Sync {
    /// An entry was appended to the WAL.
    fn on_append(&self, _entry_id: u64, _encoded_bytes: usize) {}
    /// The write buffer was flushed to the OS.
    fn on_flush(&self, _bytes_flushed: usize) {}
    /// The WAL segment was synced to stable storage.
    fn on_sync(&self) {}
    /// A segment rotation occurred.
    fn on_segment_rotate(&self, _old_segment_id: u64, _new_segment_id: u64) {}
    /// Called before a segment is deleted during truncation.
    ///
    /// Return `true` to allow deletion, `false` to retain the segment
    /// (e.g. a slow reader or snapshot still needs it).
    fn on_before_truncate(&self, _segment_id: u64, _path: &str) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Domain-specific entry type (reference implementation for segment-index WALs)
// ---------------------------------------------------------------------------

/// Segment-index WAL operations.
///
/// This is the concrete entry type for segment-based search indices. Use it as the
/// type parameter for `WalWriter<WalEntry>` / `WalReader<WalEntry>` when building
/// segment-lifecycle WALs.
///
/// For custom domains, define your own `#[derive(Serialize, Deserialize)]` enum
/// and use `WalWriter<YourEntry>` directly with the default `postcard` feature.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
///
/// Internal wire format; exposed for testing and fuzzing.
#[doc(hidden)]
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

#[derive(Debug, Clone, Copy)]
enum WalChecksumMode {
    PayloadOnly,
    Frame,
}

impl WalChecksumMode {
    fn for_version(version: u32) -> PersistenceResult<Self> {
        match version {
            2 => Ok(Self::PayloadOnly),
            WAL_FORMAT_VERSION => Ok(Self::Frame),
            _ => Err(PersistenceError::Format(format!(
                "WAL version mismatch (got {version}, expected {WAL_FORMAT_VERSION} or legacy 2)"
            ))),
        }
    }

    fn checksum(self, length: u32, entry_id: u64, payload: &[u8]) -> u32 {
        match self {
            Self::PayloadOnly => crc32fast::hash(payload),
            Self::Frame => wal_frame_checksum(length, entry_id, payload),
        }
    }
}

fn wal_frame_checksum(length: u32, entry_id: u64, payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&length.to_le_bytes());
    hasher.update(&entry_id.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

impl WalSegmentHeader {
    /// Number of bytes in the serialized header.
    pub const SIZE: usize = 4 + 4 + 8 + 8;

    /// Write the header to a stream.
    pub fn write<W: Write>(&self, writer: &mut W) -> PersistenceResult<()> {
        writer.write_all(&self.magic)?;
        writer.write_all(&self.version.to_le_bytes())?;
        writer.write_all(&self.start_entry_id.to_le_bytes())?;
        writer.write_all(&self.segment_id.to_le_bytes())?;
        Ok(())
    }

    /// Read the header from a stream.
    pub fn read<R: Read>(reader: &mut R) -> PersistenceResult<Self> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != WAL_MAGIC {
            return Err(PersistenceError::Format("invalid WAL magic".into()));
        }

        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        WalChecksumMode::for_version(version)?;

        reader.read_exact(&mut buf8)?;
        let start_entry_id = u64::from_le_bytes(buf8);
        reader.read_exact(&mut buf8)?;
        let segment_id = u64::from_le_bytes(buf8);
        Ok(Self {
            magic,
            version,
            start_entry_id,
            segment_id,
        })
    }
}

// ---------------------------------------------------------------------------
// On-disk entry framing (generic)
// ---------------------------------------------------------------------------

/// Encode/decode WAL entries on disk.
///
/// Frame layout: `[length:u32][entry_id:u64][crc32:u32][payload bytes...]`
/// where `length` = 4 (len) + 8 (entry_id) + 4 (crc) + payload_len.
///
/// Internal wire format; exposed for testing and fuzzing.
#[doc(hidden)]
pub struct WalEntryOnDisk;

enum WalRawDecode {
    Record { entry_id: u64, payload: Vec<u8> },
    PastLimit { entry_id: u64 },
}

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
        let len = u32::from_le_bytes(bytes);

        // A length of 0 can appear as trailing zero-padding from preallocation.
        // In BestEffortTail mode, treat it as EOF. A valid frame always has
        // length >= 16 (4 len + 8 entry_id + 4 crc), so 0 is never legitimate.
        if len == 0 && mode == WalReplayMode::BestEffortTail {
            return Ok(None);
        }

        // Validate minimum frame size before the caller reads entry_id + checksum.
        // Without this check, a corrupt length in [1, 15] would cause the caller to
        // consume bytes past the declared frame boundary, desyncing the stream.
        if len < 16 {
            return Err(PersistenceError::Format("WAL entry length < header".into()));
        }

        Ok(Some(len))
    }

    /// Encode raw WAL payload bytes into a framed entry suitable for appending.
    pub fn encode_bytes(entry_id: u64, payload: &[u8]) -> PersistenceResult<Vec<u8>> {
        if payload.len() > MAX_WAL_ENTRY_PAYLOAD_BYTES {
            return Err(PersistenceError::Format(format!(
                "WAL entry payload too large: {} bytes",
                payload.len()
            )));
        }
        // Frame: [length:u32][entry_id:u64][crc32:u32][payload...]
        let length_u64 = 4u64 + 8u64 + 4u64 + (payload.len() as u64);
        let length = u32::try_from(length_u64)
            .map_err(|_| PersistenceError::Format("WAL entry too large".into()))?;
        let checksum = wal_frame_checksum(length, entry_id, payload);

        let mut encoded = Vec::with_capacity(4 + 8 + 4 + payload.len());
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&entry_id.to_le_bytes());
        encoded.extend_from_slice(&checksum.to_le_bytes());
        encoded.extend_from_slice(payload);
        Ok(encoded)
    }

    /// Encode a postcard WAL entry into bytes suitable for appending.
    #[cfg(feature = "postcard")]
    pub fn encode<E: serde::Serialize>(entry_id: u64, entry: &E) -> PersistenceResult<Vec<u8>> {
        let payload =
            postcard::to_allocvec(entry).map_err(|e| PersistenceError::Encode(e.to_string()))?;
        Self::encode_bytes(entry_id, &payload)
    }

    /// Decode the next WAL entry, returning `Ok(None)` at EOF.
    ///
    /// Returns `(entry_id, payload_bytes)` for the caller to deserialize.
    pub fn decode_raw<R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
    ) -> PersistenceResult<Option<(u64, Vec<u8>)>> {
        Self::decode_raw_with_checksum(reader, mode, WalChecksumMode::Frame)
    }

    fn decode_raw_with_checksum<R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
        checksum_mode: WalChecksumMode,
    ) -> PersistenceResult<Option<(u64, Vec<u8>)>> {
        let Some(decoded) = Self::decode_raw_until(reader, mode, None, checksum_mode)? else {
            return Ok(None);
        };
        match decoded {
            WalRawDecode::Record { entry_id, payload } => Ok(Some((entry_id, payload))),
            WalRawDecode::PastLimit { .. } => {
                unreachable!("decode_raw does not set an entry limit")
            }
        }
    }

    fn decode_raw_until<R: Read>(
        reader: &mut R,
        mode: WalReplayMode,
        max_entry_id: Option<u64>,
        checksum_mode: WalChecksumMode,
    ) -> PersistenceResult<Option<WalRawDecode>> {
        let Some(length) = Self::read_u32_len(reader, mode)? else {
            return Ok(None);
        };

        let entry_id = {
            let mut buf = [0u8; 8];
            match reader.read_exact(&mut buf) {
                Ok(()) => u64::from_le_bytes(buf),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return match mode {
                        WalReplayMode::Strict => Err(e.into()),
                        WalReplayMode::BestEffortTail => Ok(None),
                    };
                }
                Err(e) => return Err(e.into()),
            }
        };

        if max_entry_id.is_some_and(|max| entry_id > max) {
            return Ok(Some(WalRawDecode::PastLimit { entry_id }));
        }

        let checksum = {
            let mut buf = [0u8; 4];
            match reader.read_exact(&mut buf) {
                Ok(()) => u32::from_le_bytes(buf),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return match mode {
                        WalReplayMode::Strict => Err(e.into()),
                        WalReplayMode::BestEffortTail => Ok(None),
                    };
                }
                Err(e) => return Err(e.into()),
            }
        };

        // length >= 16 is guaranteed by read_u32_len.
        let payload_len = length as usize - 16;
        if payload_len > MAX_WAL_ENTRY_PAYLOAD_BYTES {
            return Err(PersistenceError::Format(format!(
                "WAL entry payload too large: {payload_len} bytes"
            )));
        }
        let payload = match crate::storage::read_exact_bounded(reader, payload_len) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return match mode {
                    WalReplayMode::Strict => Err(e.into()),
                    WalReplayMode::BestEffortTail => Ok(None),
                };
            }
            Err(e) => return Err(e.into()),
        };

        let computed = checksum_mode.checksum(length, entry_id, &payload);
        if computed != checksum {
            return Err(PersistenceError::CrcMismatch {
                expected: checksum,
                actual: computed,
            });
        }

        Ok(Some(WalRawDecode::Record { entry_id, payload }))
    }

    /// Decode the next WAL entry and deserialize the payload.
    #[cfg(feature = "postcard")]
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
    validate_wal_segment_sequence(&segments)?;
    Ok(segments)
}

fn validate_wal_segment_sequence(segments: &[(u64, String)]) -> PersistenceResult<()> {
    let mut prev: Option<u64> = None;
    for &(id, _) in segments {
        if id == 0 {
            return Err(PersistenceError::Format(
                "WAL segment id must be greater than zero".into(),
            ));
        }
        if let Some(prev_id) = prev {
            if id == prev_id {
                return Err(PersistenceError::Format(format!(
                    "duplicate WAL segment id {id}"
                )));
            }
            let expected = prev_id.checked_add(1).ok_or_else(|| {
                PersistenceError::Format(format!("WAL segment id overflow after {prev_id}"))
            })?;
            if id != expected {
                return Err(PersistenceError::Format(format!(
                    "WAL segment id gap (prev={prev_id}, expected={expected}, got={id})"
                )));
            }
        }
        prev = Some(id);
    }
    Ok(())
}

fn validate_header_segment_id(header: WalSegmentHeader, segment_id: u64) -> PersistenceResult<()> {
    if header.segment_id != segment_id {
        return Err(PersistenceError::Format(format!(
            "WAL segment_id mismatch (filename={segment_id}, header={})",
            header.segment_id
        )));
    }
    Ok(())
}

fn validate_next_entry_id(prev: Option<u64>, entry_id: u64) -> PersistenceResult<()> {
    if let Some(prev) = prev {
        let expected = prev.checked_add(1).ok_or_else(|| {
            PersistenceError::Format(format!("WAL entry_id overflow after {prev}"))
        })?;
        if entry_id != expected {
            return Err(PersistenceError::Format(format!(
                "WAL entry_id gap or reorder (prev={prev}, expected={expected}, got={entry_id})"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// WalWriter (generic)
// ---------------------------------------------------------------------------

/// WAL writer that appends entries to numbered segment files under `wal/`.
///
/// An advisory lockfile (`wal/.lock`) is created when the writer initializes
/// its WAL directory and removed on drop. This catches accidental
/// double-instantiation (two `WalWriter` instances against the same directory)
/// but does not guarantee cross-process exclusion -- for that, use OS-level
/// file locking.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "postcard")]
/// # {
/// use durability::storage::MemoryDirectory;
/// use durability::walog::{WalWriter, WalReader};
///
/// #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
/// enum Op { Set(String, String), Del(String) }
///
/// let dir = MemoryDirectory::arc();
///
/// // open() creates a fresh WAL or resumes an existing one.
/// let mut w = WalWriter::<Op>::open(dir.clone()).unwrap();
/// w.append(&Op::Set("k".into(), "v".into())).unwrap();
/// w.flush().unwrap();
/// drop(w);
///
/// let records = WalReader::<Op>::new(dir).replay().unwrap();
/// assert_eq!(records.len(), 1);
/// assert_eq!(records[0].payload, Op::Set("k".into(), "v".into()));
/// # }
/// ```
pub struct WalWriter<E> {
    directory: Arc<dyn Directory>,
    current_segment_id: u64,
    current_entry_id: u64,
    current_offset: u64,
    segment_size_limit: u64,
    wal_dir_ready: bool,
    current_path: Option<String>,
    current_file: Option<Box<dyn Write + Send>>,
    flush_policy: FlushPolicy,
    since_flush: usize,
    write_buffer: Vec<u8>,
    write_buffer_limit: usize,
    holds_lock: bool,
    preallocate_bytes: u64,
    poisoned: bool,
    observer: Option<Arc<dyn WalObserver>>,
    last_flush_at: std::time::Instant,
    segment_created_at: Option<std::time::Instant>,
    segment_max_age: Option<std::time::Duration>,
    recycle_pool: VecDeque<String>,
    recycle_capacity: usize,
    _marker: PhantomData<E>,
}

impl<E> WalWriter<E> {
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
            holds_lock: false,
            preallocate_bytes: 0,
            poisoned: false,
            observer: None,
            last_flush_at: std::time::Instant::now(),
            segment_created_at: None,
            segment_max_age: None,
            recycle_pool: VecDeque::new(),
            recycle_capacity: 0,
            _marker: PhantomData,
        }
    }

    /// Attach an observer that receives WAL lifecycle events.
    pub fn set_observer(&mut self, observer: Arc<dyn WalObserver>) {
        self.observer = Some(observer);
    }

    /// Enable segment recycling with a pool of up to `capacity` files.
    ///
    /// When enabled, truncated segment files (from [`WalMaintenance::truncate_prefix`])
    /// can be fed back via [`recycle_segment`](Self::recycle_segment) and reused for
    /// new segments on the next rotation. This avoids filesystem inode allocation
    /// and directory journal overhead.
    ///
    /// Set to 0 (default) to disable recycling.
    pub fn set_recycle_capacity(&mut self, capacity: usize) {
        self.recycle_capacity = capacity;
    }

    /// Add a segment file path to the recycle pool for reuse.
    ///
    /// Typically called after [`WalMaintenance::truncate_prefix`] with the paths
    /// of deleted segments. If the pool is full, the oldest recycled file is deleted.
    ///
    /// Note: the caller must ensure the path still exists and is no longer needed
    /// for replay.
    pub fn recycle_segment(&mut self, path: String) {
        if self.recycle_capacity == 0 {
            let _ = self.directory.delete(&path);
            return;
        }
        if self.recycle_pool.len() >= self.recycle_capacity {
            if let Some(evicted) = self.recycle_pool.pop_front() {
                let _ = self.directory.delete(&evicted);
            }
        }
        self.recycle_pool.push_back(path);
    }

    /// Set the maximum age of a WAL segment before rotation.
    ///
    /// When set, segments are rotated when either the size limit or the age
    /// limit is exceeded. This prevents a low-write-rate deployment from
    /// keeping one segment active indefinitely, which would increase recovery
    /// time.
    pub fn set_segment_max_age(&mut self, age: std::time::Duration) {
        self.segment_max_age = Some(age);
    }

    /// Set the target maximum size of a WAL segment file (in bytes).
    pub fn set_segment_size_limit_bytes(&mut self, bytes: u64) {
        let min = WalSegmentHeader::SIZE as u64 + 16;
        self.segment_size_limit = bytes.max(min);
    }

    /// Set the preallocation size for new segment files (in bytes).
    ///
    /// When creating a new segment, the file is extended to this size to
    /// avoid filesystem block allocation on the write path. The file is
    /// truncated to actual size on segment rotation or writer drop.
    /// Set to 0 (default) to disable preallocation.
    ///
    /// Only effective on filesystem-backed directories (requires `file_path()`).
    pub fn set_preallocate_bytes(&mut self, bytes: u64) {
        self.preallocate_bytes = bytes;
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

    /// Return the current WAL segment ID.
    pub fn current_segment_id(&self) -> u64 {
        self.current_segment_id
    }

    /// Return the approximate byte offset within the current segment.
    pub fn current_segment_bytes(&self) -> u64 {
        self.current_offset + self.write_buffer.len() as u64
    }

    /// Append multiple entries atomically (single flush).
    ///
    /// All entries are buffered and written together, then flushed once.
    /// This amortizes the cost of `flush()` across multiple entries without
    /// thread coordination.
    ///
    /// Returns the entry IDs assigned to each entry (in order).
    /// If any entry fails to encode, no entries are written.
    #[cfg(feature = "postcard")]
    pub fn append_batch(&mut self, entries: &[E]) -> PersistenceResult<Vec<u64>>
    where
        E: serde::Serialize,
    {
        self.append_batch_postcard(entries)
    }

    #[cfg(feature = "postcard")]
    fn append_batch_encoded(
        &mut self,
        encoded_pairs: Vec<(u64, Vec<u8>)>,
    ) -> PersistenceResult<Vec<u64>> {
        if self.poisoned {
            return Err(PersistenceError::InvalidState(
                "WAL writer is poisoned after a prior write error".into(),
            ));
        }
        if encoded_pairs.is_empty() {
            return Ok(Vec::new());
        }

        // Write all entries.
        let mut ids = Vec::with_capacity(encoded_pairs.len());
        for (entry_id, encoded) in encoded_pairs {
            let encoded_len = encoded.len();
            let _wal_path = self.ensure_segment_open(entry_id)?;
            self.rotate_if_needed(entry_id, encoded_len as u64)?;
            self.buffer_encoded(&encoded)?;
            if let Some(obs) = &self.observer {
                obs.on_append(entry_id, encoded_len);
            }
            self.current_entry_id = entry_id + 1;
            ids.push(entry_id);
        }

        // Single flush for the entire batch.
        self.flush()?;
        Ok(ids)
    }

    #[cfg(feature = "postcard")]
    fn append_batch_postcard(&mut self, entries: &[E]) -> PersistenceResult<Vec<u64>>
    where
        E: serde::Serialize,
    {
        // Pre-encode all entries to detect errors before writing any.
        let mut encoded_pairs: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (next_id, entry) in (self.current_entry_id..).zip(entries.iter()) {
            let encoded = WalEntryOnDisk::encode(next_id, entry)?;
            encoded_pairs.push((next_id, encoded));
        }
        self.append_batch_encoded(encoded_pairs)
    }

    /// Open a WAL directory, creating it if empty or resuming if it exists.
    ///
    /// This is the recommended entry point: it calls [`WalWriter::resume`] if
    /// WAL segments already exist, or [`WalWriter::new`] if the directory is empty.
    pub fn open(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        let directory: Arc<dyn Directory> = directory.into();
        let segments = enumerate_wal_segments(&*directory)?;
        if segments.is_empty() {
            Ok(Self::new(directory))
        } else {
            Self::resume(directory)
        }
    }

    /// Resume appending to an existing WAL (if present).
    ///
    /// If no `wal/` files exist, this is equivalent to `WalWriter::new`.
    /// If the last segment has a **torn tail record**, this function repairs it by
    /// truncating the file back to the last valid record boundary, then continues.
    ///
    /// This respects the active-writer lock. If a prior process crashed while
    /// holding `wal/.lock`, remove the stale lock yourself or use
    /// [`WalWriter::resume_after_crash`].
    pub fn resume(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        let directory: Arc<dyn Directory> = directory.into();
        Self::resume_inner(directory, false)
    }

    /// Resume after a known writer crash by removing `wal/.lock` first.
    ///
    /// Only call this when the process that held the writer is known to be gone.
    /// Calling it against a live writer defeats the advisory single-writer guard.
    pub fn resume_after_crash(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        let directory: Arc<dyn Directory> = directory.into();
        Self::resume_inner(directory, true)
    }

    fn resume_inner(
        directory: Arc<dyn Directory>,
        remove_stale_lock: bool,
    ) -> PersistenceResult<Self> {
        if remove_stale_lock {
            let _ = directory.delete("wal/.lock");
        }

        let wal_segments = enumerate_wal_segments(&*directory)?;

        if wal_segments.is_empty() {
            return Ok(Self::new(directory));
        }

        let mut w = Self::with_options(directory.clone(), FlushPolicy::EveryN(64), 64 * 1024);
        w.acquire_wal_lock()?;

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
                    scan_last_segment_prefix(&bytes, last_seen_entry_id, *segment_id)?;

                if valid_len < bytes.len() {
                    directory.atomic_write(&wal_path, &bytes[..valid_len])?;
                    bytes.truncate(valid_len);
                }

                if let Some(id) = last_in_file {
                    last_entry_id = id;
                }

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
            let header = WalSegmentHeader::read(&mut f)?;
            validate_header_segment_id(header, *segment_id)?;
            let checksum_mode = WalChecksumMode::for_version(header.version)?;
            while let Some((entry_id, _payload)) = WalEntryOnDisk::decode_raw_with_checksum(
                &mut f,
                WalReplayMode::Strict,
                checksum_mode,
            )? {
                validate_next_entry_id(last_seen_entry_id, entry_id)?;
                last_seen_entry_id = Some(entry_id);
                last_entry_id = entry_id;
            }
        }

        Err(PersistenceError::InvalidState(
            "WAL resume internal error: missing last segment".into(),
        ))
    }

    fn acquire_wal_lock(&mut self) -> PersistenceResult<()> {
        self.directory.create_dir_all("wal")?;
        // Advisory lockfile: catch accidental double-instantiation.
        // Use O_CREAT|O_EXCL (create_new) on real filesystems for atomic acquire.
        // Falls back to exists()+write() for non-filesystem backends (e.g. MemoryDirectory).
        if let Some(lock_fs_path) = self.directory.file_path("wal/.lock") {
            if let Some(parent) = lock_fs_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_fs_path)
            {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(PersistenceError::InvalidState(
                        "WAL lockfile wal/.lock exists; another WalWriter may be active. \
                         Remove the lockfile manually if this is a stale lock from a crash."
                            .into(),
                    ));
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            // Non-filesystem backend: best-effort TOCTOU check.
            if self.directory.exists("wal/.lock") {
                return Err(PersistenceError::InvalidState(
                    "WAL lockfile wal/.lock exists; another WalWriter may be active. \
                     Remove the lockfile manually if this is a stale lock from a crash."
                        .into(),
                ));
            }
            self.directory.atomic_write("wal/.lock", b"locked")?;
        }
        self.holds_lock = true;
        Ok(())
    }

    fn ensure_wal_dir(&mut self) -> PersistenceResult<()> {
        if !self.wal_dir_ready {
            self.acquire_wal_lock()?;
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
            // Try to reuse a recycled segment file.
            if let Some(recycled) = self.recycle_pool.pop_front() {
                self.directory.atomic_rename(&recycled, &wal_path)?;
            }
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
            self.segment_created_at = Some(std::time::Instant::now());
            // Preallocate on filesystem backends to avoid block allocation on write path.
            if self.preallocate_bytes > 0 {
                if let Some(fs_path) = self.directory.file_path(&wal_path) {
                    let target = self.preallocate_bytes.max(WalSegmentHeader::SIZE as u64);
                    let _ = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&fs_path)
                        .and_then(|f| f.set_len(target));
                }
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
        let flushed_bytes = self.write_buffer.len();
        let f = self
            .current_file
            .as_mut()
            .expect("segment file must be open");
        if let Err(e) = f.write_all(&self.write_buffer) {
            // After a partial write_all failure, the file and buffer are in an
            // indeterminate state (some bytes may have been written). Poison the
            // writer to prevent further use, which would duplicate bytes.
            self.poisoned = true;
            self.write_buffer.clear();
            return Err(e.into());
        }
        self.current_offset += flushed_bytes as u64;
        self.write_buffer.clear();
        if let Some(obs) = &self.observer {
            obs.on_flush(flushed_bytes);
        }
        Ok(())
    }

    /// Flush the current segment file if open.
    pub fn flush(&mut self) -> PersistenceResult<()> {
        self.drain_buffer_to_file()?;
        if let Some(f) = self.current_file.as_mut() {
            f.flush()?;
        }
        self.since_flush = 0;
        self.last_flush_at = std::time::Instant::now();
        Ok(())
    }

    /// Flush buffered bytes and attempt to make the current WAL segment durable.
    ///
    /// On sync failure, the writer is **poisoned** -- subsequent appends will
    /// return `InvalidState`. This prevents callers from continuing to write
    /// under a false impression of durability.
    ///
    /// Returns `NotSupported` if the underlying `Directory` does not provide `file_path()`.
    pub fn flush_and_sync(&mut self) -> PersistenceResult<()> {
        self.flush()?;
        let Some(path) = self.current_path.as_deref() else {
            return Ok(());
        };
        if let Err(e) = storage::sync_file(&*self.directory, path) {
            self.poisoned = true;
            return Err(e);
        }
        if let Err(e) = storage::sync_parent_dir(&*self.directory, path) {
            self.poisoned = true;
            return Err(e);
        }
        if let Some(obs) = &self.observer {
            obs.on_sync();
        }
        Ok(())
    }

    /// Truncate the current segment to its actual written size.
    ///
    /// Called on segment rotation and drop to reclaim preallocated space.
    fn truncate_current_segment(&self) {
        if self.preallocate_bytes == 0 {
            return;
        }
        if let Some(path) = self.current_path.as_deref() {
            if let Some(fs_path) = self.directory.file_path(path) {
                let _ = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&fs_path)
                    .and_then(|f| f.set_len(self.current_offset));
            }
        }
    }

    /// Rotate to a new segment if size or age limits are exceeded.
    fn rotate_if_needed(&mut self, entry_id: u64, encoded_len: u64) -> PersistenceResult<()> {
        let projected = self.current_offset + (self.write_buffer.len() as u64) + encoded_len;
        let size_exceeded = projected > self.segment_size_limit;
        let age_exceeded = match (self.segment_max_age, self.segment_created_at) {
            (Some(max_age), Some(created)) => created.elapsed() >= max_age,
            _ => false,
        };
        if (size_exceeded || age_exceeded) && self.current_offset > WalSegmentHeader::SIZE as u64 {
            self.flush()?;
            self.truncate_current_segment();
            let old_segment_id = self.current_segment_id;
            self.current_segment_id += 1;
            self.current_offset = 0;
            self.current_path = None;
            self.current_file = None;
            self.since_flush = 0;
            if let Some(obs) = &self.observer {
                obs.on_segment_rotate(old_segment_id, self.current_segment_id);
            }
            if let Err(e) = self.ensure_segment_open(entry_id) {
                // Poison: segment_id was advanced but the new segment file could
                // not be created. The writer state is inconsistent.
                self.poisoned = true;
                return Err(e);
            }
        }
        Ok(())
    }

    /// Buffer encoded bytes and drain if the write buffer is full.
    fn buffer_encoded(&mut self, encoded: &[u8]) -> PersistenceResult<()> {
        self.write_buffer.extend_from_slice(encoded);
        if self.write_buffer_limit == 0 || self.write_buffer.len() >= self.write_buffer_limit {
            self.drain_buffer_to_file()?;
        }
        Ok(())
    }

    /// Apply the configured flush policy after an append.
    fn apply_flush_policy(&mut self) -> PersistenceResult<()> {
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
            FlushPolicy::Interval(d) => {
                if self.last_flush_at.elapsed() >= d {
                    self.flush()?;
                }
            }
            FlushPolicy::Manual => {}
        }
        Ok(())
    }

    /// Append raw payload bytes, returning the assigned entry id.
    pub fn append_bytes(&mut self, payload: &[u8]) -> PersistenceResult<u64> {
        let entry_id = self.current_entry_id;
        let encoded = WalEntryOnDisk::encode_bytes(entry_id, payload)?;
        self.append_encoded(entry_id, &encoded)
    }

    /// Append a postcard-encoded entry, returning its assigned entry id.
    #[cfg(feature = "postcard")]
    pub fn append(&mut self, entry: &E) -> PersistenceResult<u64>
    where
        E: serde::Serialize,
    {
        let entry_id = self.current_entry_id;
        let encoded = WalEntryOnDisk::encode(entry_id, entry)?;
        self.append_encoded(entry_id, &encoded)
    }

    fn append_encoded(&mut self, entry_id: u64, encoded: &[u8]) -> PersistenceResult<u64> {
        if self.poisoned {
            return Err(PersistenceError::InvalidState(
                "WAL writer is poisoned after a prior write error".into(),
            ));
        }
        let _wal_path = self.ensure_segment_open(entry_id)?;
        let encoded_len = encoded.len();

        self.rotate_if_needed(entry_id, encoded_len as u64)?;
        self.buffer_encoded(encoded)?;
        self.apply_flush_policy()?;

        if let Some(obs) = &self.observer {
            obs.on_append(entry_id, encoded_len);
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
    segment_id: u64,
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
    validate_header_segment_id(header, segment_id)?;
    let checksum_mode = WalChecksumMode::for_version(header.version)?;

    let mut first_entry_id_in_segment: Option<u64> = None;
    let mut last_id = last_seen_entry_id;

    loop {
        let start_pos = cur.position() as usize;
        match WalEntryOnDisk::decode_raw_with_checksum(
            &mut cur,
            WalReplayMode::BestEffortTail,
            checksum_mode,
        )? {
            Some((entry_id, _payload)) => {
                if first_entry_id_in_segment.is_none() {
                    first_entry_id_in_segment = Some(entry_id);
                }
                validate_next_entry_id(last_id, entry_id)?;
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

impl<E> Drop for WalWriter<E> {
    fn drop(&mut self) {
        // Warn in debug builds if the write buffer has unflushed data.
        // This catches the common mistake of dropping without calling flush().
        #[cfg(debug_assertions)]
        if !self.write_buffer.is_empty() {
            eprintln!(
                "durability: WalWriter dropped with {} unflushed bytes in write buffer",
                self.write_buffer.len()
            );
        }
        // Truncate preallocated segment to actual written size.
        if self.preallocate_bytes > 0 {
            if let Some(path) = self.current_path.as_deref() {
                if let Some(fs_path) = self.directory.file_path(path) {
                    let _ = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&fs_path)
                        .and_then(|f| f.set_len(self.current_offset));
                }
            }
        }
        // Clean up recycled segment files.
        for path in self.recycle_pool.drain(..) {
            let _ = self.directory.delete(&path);
        }
        if self.holds_lock {
            let _ = self.directory.delete("wal/.lock");
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

impl<E> WalReader<E> {
    /// Create a new WAL reader.
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> Self {
        Self {
            directory: directory.into(),
            _marker: PhantomData,
        }
    }

    /// Replay all WAL entries as raw payload bytes in sorted segment-id order.
    pub fn replay_bytes(&self) -> PersistenceResult<Vec<WalRecord<Vec<u8>>>> {
        let mut records = Vec::new();
        self.replay_bytes_each_inner(WalReplayMode::Strict, None, |r| {
            records.push(r);
            Ok(())
        })?;
        Ok(records)
    }

    /// Best-effort raw replay: stop at first truncated tail record in the final segment.
    pub fn replay_bytes_best_effort(&self) -> PersistenceResult<Vec<WalRecord<Vec<u8>>>> {
        let mut records = Vec::new();
        self.replay_bytes_each_inner(WalReplayMode::BestEffortTail, None, |r| {
            records.push(r);
            Ok(())
        })?;
        Ok(records)
    }

    /// Streaming raw replay: call `apply` for each WAL entry (strict mode).
    ///
    /// Unlike [`replay_bytes`](Self::replay_bytes), this does not collect entries
    /// into a `Vec`, making it suitable for large WALs.
    pub fn replay_bytes_each(
        &self,
        apply: impl FnMut(WalRecord<Vec<u8>>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64> {
        self.replay_bytes_each_inner(WalReplayMode::Strict, None, apply)
    }

    /// Streaming raw best-effort replay.
    pub fn replay_bytes_each_best_effort(
        &self,
        apply: impl FnMut(WalRecord<Vec<u8>>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64> {
        self.replay_bytes_each_inner(WalReplayMode::BestEffortTail, None, apply)
    }

    /// Streaming raw replay with explicit mode selection.
    pub fn replay_bytes_each_with_mode(
        &self,
        mode: WalReplayMode,
        apply: impl FnMut(WalRecord<Vec<u8>>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64> {
        self.replay_bytes_each_inner(mode, None, apply)
    }

    /// Count entries in the WAL without collecting them.
    pub fn entry_count(&self) -> PersistenceResult<u64> {
        self.replay_bytes_each_inner(WalReplayMode::Strict, None, |_| Ok(()))
    }

    /// Count entries (best-effort mode).
    pub fn entry_count_best_effort(&self) -> PersistenceResult<u64> {
        self.replay_bytes_each_inner(WalReplayMode::BestEffortTail, None, |_| Ok(()))
    }

    /// Replay all WAL entries in sorted segment-id order.
    #[cfg(feature = "postcard")]
    pub fn replay(&self) -> PersistenceResult<Vec<WalRecord<E>>>
    where
        E: serde::de::DeserializeOwned,
    {
        let mut records = Vec::new();
        self.replay_each_inner(WalReplayMode::Strict, None, |r| {
            records.push(r);
            Ok(())
        })?;
        Ok(records)
    }

    /// Best-effort replay: stop at first truncated tail record in the final segment.
    #[cfg(feature = "postcard")]
    pub fn replay_best_effort(&self) -> PersistenceResult<Vec<WalRecord<E>>>
    where
        E: serde::de::DeserializeOwned,
    {
        let mut records = Vec::new();
        self.replay_each_inner(WalReplayMode::BestEffortTail, None, |r| {
            records.push(r);
            Ok(())
        })?;
        Ok(records)
    }

    /// Streaming replay: call `apply` for each WAL entry (strict mode).
    ///
    /// Unlike [`replay`](Self::replay), this does not collect entries into a `Vec`,
    /// making it suitable for large WALs that would otherwise exhaust memory.
    /// Returns the number of entries replayed.
    #[cfg(feature = "postcard")]
    pub fn replay_each(
        &self,
        apply: impl FnMut(WalRecord<E>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64>
    where
        E: serde::de::DeserializeOwned,
    {
        self.replay_each_inner(WalReplayMode::Strict, None, apply)
    }

    /// Streaming best-effort replay: call `apply` for each WAL entry.
    ///
    /// Best-effort: stops at first truncated tail record in the final segment.
    /// Returns the number of entries replayed.
    #[cfg(feature = "postcard")]
    pub fn replay_each_best_effort(
        &self,
        apply: impl FnMut(WalRecord<E>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64>
    where
        E: serde::de::DeserializeOwned,
    {
        self.replay_each_inner(WalReplayMode::BestEffortTail, None, apply)
    }

    /// Streaming replay with explicit mode selection.
    ///
    /// Combines [`replay_each`](Self::replay_each) and
    /// [`replay_each_best_effort`](Self::replay_each_best_effort) behind a mode parameter.
    /// Returns the number of entries replayed.
    #[cfg(feature = "postcard")]
    pub fn replay_each_with_mode(
        &self,
        mode: WalReplayMode,
        apply: impl FnMut(WalRecord<E>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64>
    where
        E: serde::de::DeserializeOwned,
    {
        self.replay_each_inner(mode, None, apply)
    }

    #[cfg(feature = "postcard")]
    pub(crate) fn replay_each_with_mode_until(
        &self,
        mode: WalReplayMode,
        max_entry_id: Option<u64>,
        apply: impl FnMut(WalRecord<E>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64>
    where
        E: serde::de::DeserializeOwned,
    {
        self.replay_each_inner(mode, max_entry_id, apply)
    }

    #[cfg(feature = "postcard")]
    fn replay_each_inner(
        &self,
        mode: WalReplayMode,
        max_entry_id: Option<u64>,
        mut apply: impl FnMut(WalRecord<E>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64>
    where
        E: serde::de::DeserializeOwned,
    {
        self.replay_bytes_each_inner(mode, max_entry_id, |record| {
            let payload: E = postcard::from_bytes(&record.payload)
                .map_err(|e| PersistenceError::Decode(e.to_string()))?;
            apply(WalRecord {
                entry_id: record.entry_id,
                payload,
            })
        })
    }

    fn replay_bytes_each_inner(
        &self,
        mode: WalReplayMode,
        max_entry_id: Option<u64>,
        mut apply: impl FnMut(WalRecord<Vec<u8>>) -> PersistenceResult<()>,
    ) -> PersistenceResult<u64> {
        let wal_segments = enumerate_wal_segments(&*self.directory)?;
        let last_segment_id = wal_segments.last().map(|(id, _)| *id);
        let mut count = 0u64;
        let mut last_seen_entry_id: Option<u64> = None;

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
            validate_header_segment_id(header, segment_id)?;
            let checksum_mode = WalChecksumMode::for_version(header.version)?;

            let mut first_entry_id_in_segment: Option<u64> = None;

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
                let decoded = match max_entry_id {
                    Some(max) => WalEntryOnDisk::decode_raw_until(
                        &mut file,
                        segment_mode,
                        Some(max),
                        checksum_mode,
                    )?,
                    None => WalEntryOnDisk::decode_raw_until(
                        &mut file,
                        segment_mode,
                        None,
                        checksum_mode,
                    )?,
                };

                match decoded {
                    Some(WalRawDecode::Record { entry_id, payload }) => {
                        if first_entry_id_in_segment.is_none() {
                            first_entry_id_in_segment = Some(entry_id);
                        }
                        validate_next_entry_id(last_seen_entry_id, entry_id)?;
                        last_seen_entry_id = Some(entry_id);
                        apply(WalRecord { entry_id, payload })?;
                        count += 1;
                    }
                    Some(WalRawDecode::PastLimit { entry_id }) => {
                        if first_entry_id_in_segment.is_none() {
                            first_entry_id_in_segment = Some(entry_id);
                        }
                        validate_next_entry_id(last_seen_entry_id, entry_id)?;
                        if let Some(first_id) = first_entry_id_in_segment {
                            if first_id != header.start_entry_id {
                                return Err(PersistenceError::Format(format!(
                                    "WAL segment start_entry_id mismatch (header={}, first_entry={})",
                                    header.start_entry_id, first_id
                                )));
                            }
                        }
                        return Ok(count);
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

        Ok(count)
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
        let mut last_seen_entry_id: Option<u64> = None;
        for (segment_id, wal_file) in wal_segments {
            let path = format!("wal/{wal_file}");
            let mut f = self.directory.open_file(&path)?;
            let header = WalSegmentHeader::read(&mut f)?;
            validate_header_segment_id(header, segment_id)?;
            let checksum_mode = WalChecksumMode::for_version(header.version)?;
            let mut end: Option<u64> = None;
            let mut first: Option<u64> = None;
            while let Some((entry_id, _payload)) = WalEntryOnDisk::decode_raw_with_checksum(
                &mut f,
                WalReplayMode::Strict,
                checksum_mode,
            )? {
                validate_next_entry_id(last_seen_entry_id, entry_id)?;
                if first.is_none() {
                    first = Some(entry_id);
                }
                last_seen_entry_id = Some(entry_id);
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
    /// Returns the number of deleted segment files. To recycle deleted segments
    /// instead of destroying them, use [`truncate_to_recycle`](Self::truncate_to_recycle)
    /// and feed the returned paths to [`WalWriter::recycle_segment`].
    pub fn truncate_prefix(&self, last_entry_id: u64) -> PersistenceResult<usize> {
        self.truncate_prefix_with_observer(last_entry_id, None)
    }

    /// Identify segments eligible for truncation without deleting them.
    ///
    /// Returns paths of segment files fully covered by `last_entry_id`.
    /// The caller is responsible for either deleting them or recycling them
    /// via [`WalWriter::recycle_segment`].
    pub fn truncate_to_recycle(&self, last_entry_id: u64) -> PersistenceResult<Vec<String>> {
        let ranges = self.segment_ranges_strict()?;
        let mut paths = Vec::new();
        for seg in ranges {
            let Some(end) = seg.end_entry_id else {
                continue;
            };
            if end <= last_entry_id {
                paths.push(seg.path);
            }
        }
        Ok(paths)
    }

    /// Delete WAL segments covered by a checkpoint, consulting an observer.
    ///
    /// If an observer is provided, each eligible segment is passed to
    /// [`WalObserver::on_before_truncate`]. Segments where the observer returns
    /// `false` are retained (e.g. a slow reader still needs them).
    pub fn truncate_prefix_with_observer(
        &self,
        last_entry_id: u64,
        observer: Option<&dyn WalObserver>,
    ) -> PersistenceResult<usize> {
        let ranges = self.segment_ranges_strict()?;
        let mut deleted = 0usize;
        for seg in ranges {
            let Some(end) = seg.end_entry_id else {
                continue;
            };
            if end <= last_entry_id {
                if let Some(obs) = observer {
                    if !obs.on_before_truncate(seg.segment_id, &seg.path) {
                        // Preserve the WAL as a contiguous suffix. If an earlier
                        // segment is retained, later segments must remain too.
                        break;
                    }
                }
                self.directory.delete(&seg.path)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

// Compile-time assertions: WalWriter and WalReader are Send when E: Send.
#[allow(dead_code)]
const _: () = {
    fn assert_send<T: Send>() {}
    fn check() {
        assert_send::<WalWriter<String>>();
        assert_send::<WalReader<String>>();
    }
};

// ---------------------------------------------------------------------------
// SyncWalWriter (thread-safe wrapper)
// ---------------------------------------------------------------------------

/// Thread-safe WAL writer.
///
/// Wraps a [`WalWriter`] in a `Mutex`, allowing multiple threads to append
/// concurrently. Durable operations are serialized through the same mutex:
/// [`append_bytes_durable`](Self::append_bytes_durable) appends one payload,
/// then syncs the writer if that entry is newer than the last synced entry.
/// The default `postcard` feature also provides a typed `append_durable`
/// helper.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "postcard")]
/// # {
/// use durability::storage::MemoryDirectory;
/// use durability::walog::{SyncWalWriter, WalReader};
///
/// #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
/// enum Op { Inc, Dec }
///
/// let dir = MemoryDirectory::arc();
/// let sw = SyncWalWriter::<Op>::open(dir.clone()).unwrap();
///
/// // Multiple threads can call append concurrently.
/// let id1 = sw.append(&Op::Inc).unwrap();
/// let id2 = sw.append(&Op::Dec).unwrap();
/// sw.flush().unwrap();
/// drop(sw);
///
/// let records = WalReader::<Op>::new(dir).replay().unwrap();
/// assert_eq!(records.len(), 2);
/// # }
/// ```
pub struct SyncWalWriter<E> {
    state: std::sync::Mutex<SyncState<E>>,
}

struct SyncState<E> {
    writer: WalWriter<E>,
    last_synced_entry_id: u64,
}

impl<E> SyncWalWriter<E> {
    /// Create a new thread-safe WAL writer (fails if segments already exist).
    pub fn new(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        Ok(Self::from_writer(WalWriter::new(directory)))
    }

    /// Open a WAL directory (create or resume).
    pub fn open(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        Ok(Self::from_writer(WalWriter::open(directory)?))
    }

    /// Resume an existing WAL.
    pub fn resume(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        Ok(Self::from_writer(WalWriter::resume(directory)?))
    }

    /// Resume after a known writer crash by removing `wal/.lock` first.
    ///
    /// Only call this when the process that held the writer is known to be gone.
    pub fn resume_after_crash(directory: impl Into<Arc<dyn Directory>>) -> PersistenceResult<Self> {
        Ok(Self::from_writer(WalWriter::resume_after_crash(directory)?))
    }

    /// Wrap an existing `WalWriter`.
    pub fn from_writer(writer: WalWriter<E>) -> Self {
        let last = writer.last_entry_id().unwrap_or(0);
        Self {
            state: std::sync::Mutex::new(SyncState {
                writer,
                last_synced_entry_id: last,
            }),
        }
    }

    /// Append an entry. Does not sync; call [`flush`](Self::flush) or
    /// [`flush_and_sync`](Self::flush_and_sync) afterward.
    #[cfg(feature = "postcard")]
    pub fn append(&self, entry: &E) -> PersistenceResult<u64>
    where
        E: serde::Serialize,
    {
        self.state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?
            .writer
            .append(entry)
    }

    /// Append multiple entries as a batch.
    #[cfg(feature = "postcard")]
    pub fn append_batch(&self, entries: &[E]) -> PersistenceResult<Vec<u64>>
    where
        E: serde::Serialize,
    {
        self.state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?
            .writer
            .append_batch(entries)
    }

    /// Append raw payload bytes. Does not sync; call [`flush`](Self::flush) or
    /// [`flush_and_sync`](Self::flush_and_sync) afterward.
    pub fn append_bytes(&self, payload: &[u8]) -> PersistenceResult<u64> {
        self.state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?
            .writer
            .append_bytes(payload)
    }

    /// Append an entry and wait until it is durable on stable storage.
    ///
    /// If another thread previously synced past this entry's ID, skip the
    /// redundant sync. With the current mutex-based writer this mainly covers
    /// entries appended before this call, not a concurrent write barrier.
    #[cfg(feature = "postcard")]
    pub fn append_durable(&self, entry: &E) -> PersistenceResult<u64>
    where
        E: serde::Serialize,
    {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?;
        let id = state.writer.append(entry)?;
        if id > state.last_synced_entry_id {
            state.writer.flush_and_sync()?;
            state.last_synced_entry_id = state.writer.last_entry_id().unwrap_or(id);
        }
        Ok(id)
    }

    /// Append raw payload bytes and wait until they are durable on stable storage.
    pub fn append_bytes_durable(&self, payload: &[u8]) -> PersistenceResult<u64> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?;
        let id = state.writer.append_bytes(payload)?;
        if id > state.last_synced_entry_id {
            state.writer.flush_and_sync()?;
            state.last_synced_entry_id = state.writer.last_entry_id().unwrap_or(id);
        }
        Ok(id)
    }

    /// Flush the write buffer to the OS.
    pub fn flush(&self) -> PersistenceResult<()> {
        self.state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?
            .writer
            .flush()
    }

    /// Flush and sync to stable storage (covers all appended entries).
    pub fn flush_and_sync(&self) -> PersistenceResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?;
        state.writer.flush_and_sync()?;
        state.last_synced_entry_id = state.writer.last_entry_id().unwrap_or(0);
        Ok(())
    }

    /// Return the last assigned entry ID.
    pub fn last_entry_id(&self) -> Option<u64> {
        self.state.lock().ok()?.writer.last_entry_id()
    }

    /// Access the inner writer under the lock (for configuration).
    ///
    /// Call this before starting concurrent writes to set segment size limits,
    /// observers, preallocation, etc.
    pub fn configure(&self, f: impl FnOnce(&mut WalWriter<E>)) -> PersistenceResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PersistenceError::LockFailed {
                resource: "SyncWalWriter".into(),
                reason: "mutex poisoned".into(),
            })?;
        f(&mut state.writer);
        Ok(())
    }
}

// SyncWalWriter is Send + Sync by construction (Mutex<WalWriter<E>>).
#[allow(dead_code)]
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn check() {
        assert_send_sync::<SyncWalWriter<String>>();
    }
};

#[cfg(test)]
mod raw_tests {
    use super::*;
    use crate::storage::MemoryDirectory;

    #[test]
    fn wal_roundtrip_bytes() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<()>::new(dir.clone());
        assert_eq!(w.append_bytes(b"one").unwrap(), 1);
        assert_eq!(w.append_bytes(b"two").unwrap(), 2);
        w.flush().unwrap();
        drop(w);

        let records = WalReader::<()>::new(dir).replay_bytes().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].entry_id, 1);
        assert_eq!(records[0].payload, b"one");
        assert_eq!(records[1].entry_id, 2);
        assert_eq!(records[1].payload, b"two");
    }

    #[test]
    fn sync_wal_roundtrip_bytes() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let sw = SyncWalWriter::<()>::open(dir.clone()).unwrap();
        assert_eq!(sw.append_bytes(b"alpha").unwrap(), 1);
        assert_eq!(sw.append_bytes(b"beta").unwrap(), 2);
        sw.flush().unwrap();
        drop(sw);

        let records = WalReader::<()>::new(dir).replay_bytes().unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| r.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"alpha".as_slice(), b"beta".as_slice()]
        );
    }
}

#[cfg(all(test, feature = "postcard"))]
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

    fn encode_legacy_wal_entry(entry_id: u64, entry: &WalEntry) -> Vec<u8> {
        let payload = postcard::to_allocvec(entry).unwrap();
        let length = u32::try_from(4usize + 8usize + 4usize + payload.len()).unwrap();
        let checksum = crc32fast::hash(&payload);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&entry_id.to_le_bytes());
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes
    }

    fn write_legacy_wal_segment(
        dir: &Arc<dyn Directory>,
        seg_id: u64,
        start_entry_id: u64,
        entries: &[(u64, WalEntry)],
    ) {
        dir.create_dir_all("wal").unwrap();
        let path = format!("wal/wal_{seg_id}.log");
        let mut f = dir.create_file(&path).unwrap();
        WalSegmentHeader {
            magic: WAL_MAGIC,
            version: 2,
            start_entry_id,
            segment_id: seg_id,
        }
        .write(&mut f)
        .unwrap();
        for (eid, e) in entries {
            f.write_all(&encode_legacy_wal_entry(*eid, e)).unwrap();
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
    fn wal_reader_rejects_entry_id_header_corruption() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        write_wal_segment(
            &dir,
            1,
            1,
            &[
                (
                    1,
                    WalEntry::AddSegment {
                        segment_id: 1,
                        doc_count: 1,
                    },
                ),
                (
                    3,
                    WalEntry::AddSegment {
                        segment_id: 3,
                        doc_count: 1,
                    },
                ),
            ],
        );

        let mut bytes = read_all(&dir, "wal/wal_1.log");
        let first = WalEntryOnDisk::encode(
            1,
            &WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 1,
            },
        )
        .unwrap();
        let second_entry_id = WalSegmentHeader::SIZE + first.len() + 4;
        bytes[second_entry_id] ^= 0x01; // 3 -> 2 in little-endian entry_id
        dir.atomic_write("wal/wal_1.log", &bytes).unwrap();

        let err = WalReader::<WalEntry>::new(dir).replay().unwrap_err();
        assert!(matches!(err, PersistenceError::CrcMismatch { .. }));
    }

    #[test]
    fn wal_reader_rejects_length_header_corruption() {
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
        bytes[WalSegmentHeader::SIZE] += 1;
        bytes.push(0xAA);
        dir.atomic_write("wal/wal_1.log", &bytes).unwrap();

        let err = WalReader::<WalEntry>::new(dir).replay().unwrap_err();
        assert!(matches!(err, PersistenceError::CrcMismatch { .. }));
    }

    #[test]
    fn wal_reader_replays_legacy_payload_only_checksum_segment() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        write_legacy_wal_segment(
            &dir,
            1,
            1,
            &[
                (
                    1,
                    WalEntry::AddSegment {
                        segment_id: 1,
                        doc_count: 1,
                    },
                ),
                (
                    2,
                    WalEntry::DeleteDocuments {
                        deletes: vec![(1, 0)],
                    },
                ),
            ],
        );

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].entry_id, 1);
        assert_eq!(records[1].entry_id, 2);
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
            9,
            9,
            &[(
                9,
                WalEntry::AddSegment {
                    segment_id: 9,
                    doc_count: 1,
                },
            )],
        );

        let r = WalReader::<WalEntry>::new(dir);
        let records = r.replay().unwrap();
        assert_eq!(records.len(), 2);

        let ids: Vec<u64> = records.iter().map(|r| r.entry_id).collect();
        assert_eq!(ids, vec![9, 10]);
    }

    #[test]
    fn wal_reader_rejects_internal_segment_gap() {
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
            3,
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
        let err = r.replay().unwrap_err();
        assert!(err.to_string().contains("segment id gap"));
    }

    #[test]
    fn wal_segment_sequence_rejects_duplicate_max_without_panic() {
        let err = validate_wal_segment_sequence(&[
            (u64::MAX, "wal_18446744073709551615.log".to_string()),
            (u64::MAX, "wal_18446744073709551615-copy.log".to_string()),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("duplicate WAL segment id"));
    }

    #[test]
    fn wal_reader_rejects_filename_header_segment_mismatch() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        write_wal_segment(
            &dir,
            2,
            1,
            &[(
                1,
                WalEntry::AddSegment {
                    segment_id: 1,
                    doc_count: 1,
                },
            )],
        );
        dir.atomic_rename("wal/wal_2.log", "wal/wal_1.log").unwrap();

        let r = WalReader::<WalEntry>::new(dir);
        let err = r.replay().unwrap_err();
        assert!(err.to_string().contains("segment_id mismatch"));
    }

    #[test]
    fn wal_reader_rejects_entry_id_gap_across_segments() {
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
            3,
            &[(
                3,
                WalEntry::AddSegment {
                    segment_id: 3,
                    doc_count: 1,
                },
            )],
        );

        let r = WalReader::<WalEntry>::new(dir);
        let err = r.replay().unwrap_err();
        assert!(err.to_string().contains("entry_id gap"));
    }

    #[test]
    fn wal_reader_rejects_entry_id_overflow_without_panic() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        write_wal_segment(
            &dir,
            1,
            u64::MAX,
            &[(
                u64::MAX,
                WalEntry::AddSegment {
                    segment_id: u64::MAX,
                    doc_count: 1,
                },
            )],
        );
        write_wal_segment(
            &dir,
            2,
            0,
            &[(
                0,
                WalEntry::AddSegment {
                    segment_id: 0,
                    doc_count: 1,
                },
            )],
        );

        let r = WalReader::<WalEntry>::new(dir);
        let err = r.replay().unwrap_err();
        assert!(err.to_string().contains("entry_id overflow"));
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

    #[test]
    fn wal_append_batch_writes_atomically() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::new(dir.clone());

        let entries = vec![
            WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 10,
            },
            WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 20,
            },
            WalEntry::DeleteDocuments {
                deletes: vec![(1, 5)],
            },
        ];

        let ids = w.append_batch(&entries).unwrap();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(w.last_entry_id(), Some(3));
        assert_eq!(w.next_entry_id(), 4);

        let r = WalReader::<WalEntry>::new(dir);
        let records = r.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].entry_id, 1);
        assert_eq!(records[2].entry_id, 3);
    }

    #[test]
    fn wal_append_batch_empty_is_noop() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::new(dir.clone());

        let ids = w.append_batch(&[]).unwrap();
        assert!(ids.is_empty());
        assert_eq!(w.last_entry_id(), None);
    }

    #[test]
    fn wal_entry_count_matches_replay_len() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::new(dir.clone());

        for i in 0..15u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i + 1,
                doc_count: 0,
            })
            .unwrap();
        }
        w.flush().unwrap();
        drop(w);

        let r = WalReader::<WalEntry>::new(dir);
        assert_eq!(r.entry_count().unwrap(), 15);
        assert_eq!(r.replay().unwrap().len(), 15);
    }

    #[test]
    fn wal_metadata_accessors() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::new(dir.clone());

        assert_eq!(w.current_segment_id(), 1);
        assert_eq!(w.current_segment_bytes(), 0);

        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 10,
        })
        .unwrap();
        w.flush().unwrap();

        // After writing, segment bytes should be > header size
        assert!(w.current_segment_bytes() > WalSegmentHeader::SIZE as u64);
    }

    #[test]
    fn wal_observer_receives_events() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct TestObserver {
            appends: AtomicUsize,
            bytes_appended: AtomicUsize,
            flushes: AtomicUsize,
            bytes_flushed: AtomicUsize,
            rotations: AtomicUsize,
        }

        impl TestObserver {
            fn new() -> Self {
                Self {
                    appends: AtomicUsize::new(0),
                    bytes_appended: AtomicUsize::new(0),
                    flushes: AtomicUsize::new(0),
                    bytes_flushed: AtomicUsize::new(0),
                    rotations: AtomicUsize::new(0),
                }
            }
        }

        impl super::WalObserver for TestObserver {
            fn on_append(&self, _entry_id: u64, encoded_bytes: usize) {
                self.appends.fetch_add(1, Ordering::Relaxed);
                self.bytes_appended
                    .fetch_add(encoded_bytes, Ordering::Relaxed);
            }
            fn on_flush(&self, bytes_flushed: usize) {
                self.flushes.fetch_add(1, Ordering::Relaxed);
                self.bytes_flushed
                    .fetch_add(bytes_flushed, Ordering::Relaxed);
            }
            fn on_segment_rotate(&self, _old: u64, _new: u64) {
                self.rotations.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let obs = Arc::new(TestObserver::new());
        let mut w = WalWriter::<WalEntry>::with_flush_policy(
            dir.clone(),
            crate::storage::FlushPolicy::Manual,
        );
        w.set_observer(obs.clone() as Arc<dyn super::WalObserver>);

        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        w.append(&WalEntry::AddSegment {
            segment_id: 2,
            doc_count: 2,
        })
        .unwrap();
        w.flush().unwrap();

        assert_eq!(obs.appends.load(Ordering::Relaxed), 2);
        assert!(obs.bytes_appended.load(Ordering::Relaxed) > 0);
        assert!(obs.flushes.load(Ordering::Relaxed) >= 1);
        assert!(obs.bytes_flushed.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn wal_observer_rotation_event() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct RotationCounter(AtomicUsize);
        impl super::WalObserver for RotationCounter {
            fn on_segment_rotate(&self, _old: u64, _new: u64) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let obs = Arc::new(RotationCounter(AtomicUsize::new(0)));

        let mut w = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            crate::storage::FlushPolicy::PerAppend,
            0,
        );
        w.set_segment_size_limit_bytes(100); // very small, forces rotation
        w.set_observer(obs.clone() as Arc<dyn super::WalObserver>);

        for i in 0..10u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i + 1,
                doc_count: i as u32,
            })
            .unwrap();
        }
        w.flush().unwrap();

        assert!(
            obs.0.load(Ordering::Relaxed) >= 1,
            "expected at least one rotation"
        );
    }

    #[test]
    fn wal_observer_append_batch_events() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct BatchObserver {
            appends: AtomicUsize,
            flushes: AtomicUsize,
        }

        impl super::WalObserver for BatchObserver {
            fn on_append(&self, _entry_id: u64, _encoded_bytes: usize) {
                self.appends.fetch_add(1, Ordering::Relaxed);
            }
            fn on_flush(&self, _bytes_flushed: usize) {
                self.flushes.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let obs = Arc::new(BatchObserver {
            appends: AtomicUsize::new(0),
            flushes: AtomicUsize::new(0),
        });
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        w.set_observer(obs.clone() as Arc<dyn super::WalObserver>);

        let entries = vec![
            WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 1,
            },
            WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 2,
            },
            WalEntry::AddSegment {
                segment_id: 3,
                doc_count: 3,
            },
        ];
        w.append_batch(&entries).unwrap();

        assert_eq!(obs.appends.load(Ordering::Relaxed), 3);
        assert!(obs.flushes.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn wal_open_creates_fresh_then_resumes() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        // First open: creates fresh WAL.
        let mut w = WalWriter::<WalEntry>::open(dir.clone()).unwrap();
        let id1 = w
            .append(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 3,
            })
            .unwrap();
        assert_eq!(id1, 1);
        w.flush().unwrap();
        drop(w);

        // Second open: resumes from existing WAL.
        let mut w = WalWriter::<WalEntry>::open(dir.clone()).unwrap();
        let id2 = w
            .append(&WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 7,
            })
            .unwrap();
        assert_eq!(id2, 2);
        w.flush().unwrap();
        drop(w);

        let r = WalReader::<WalEntry>::new(dir);
        let records = r.replay().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].entry_id, 1);
        assert_eq!(records[1].entry_id, 2);
    }

    #[test]
    fn wal_flush_policy_interval() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            crate::storage::FlushPolicy::Interval(std::time::Duration::from_millis(50)),
            64 * 1024,
        );

        // First append: no flush yet (timer just started).
        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();

        // Reader should see nothing (data in write buffer, interval not elapsed).
        let r = WalReader::<WalEntry>::new(dir.clone());
        // Can't read unflushed data from MemoryDirectory's in-place writer,
        // so just verify the writer hasn't flushed yet.
        assert!(w.current_segment_bytes() > 0);

        // Sleep past the interval.
        std::thread::sleep(std::time::Duration::from_millis(60));

        // Next append triggers the interval-based flush.
        w.append(&WalEntry::AddSegment {
            segment_id: 2,
            doc_count: 2,
        })
        .unwrap();

        // Explicit flush for the final entry.
        w.flush().unwrap();
        drop(w);

        let records = r.replay().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn wal_segment_max_age_triggers_rotation() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            crate::storage::FlushPolicy::PerAppend,
            0,
        );
        w.set_segment_max_age(std::time::Duration::from_millis(50));

        // First entry goes to segment 1.
        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        assert_eq!(w.current_segment_id(), 1);

        // Sleep past the age limit.
        std::thread::sleep(std::time::Duration::from_millis(60));

        // Next entry should trigger age-based rotation.
        w.append(&WalEntry::AddSegment {
            segment_id: 2,
            doc_count: 2,
        })
        .unwrap();
        assert!(
            w.current_segment_id() > 1,
            "expected rotation due to age, still on segment {}",
            w.current_segment_id()
        );
        w.flush().unwrap();
        drop(w);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn wal_truncate_prefix_respects_observer() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            crate::storage::FlushPolicy::PerAppend,
            0,
        );
        w.set_segment_size_limit_bytes(80); // force multiple segments

        for i in 0..6u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i + 1,
                doc_count: i as u32,
            })
            .unwrap();
        }
        w.flush().unwrap();
        drop(w);

        // Observer that blocks deletion of segment 1.
        struct PinSegment1;
        impl super::WalObserver for PinSegment1 {
            fn on_before_truncate(&self, segment_id: u64, _path: &str) -> bool {
                segment_id != 1 // retain segment 1
            }
        }

        let maint = WalMaintenance::new(dir.clone());
        let ranges = maint.segment_ranges_strict().unwrap();
        assert!(ranges.len() > 1, "need multiple segments for this test");

        let last_entry = ranges.last().unwrap().end_entry_id.unwrap();
        let obs = PinSegment1;
        let deleted = maint
            .truncate_prefix_with_observer(last_entry, Some(&obs))
            .unwrap();

        // Segment 1 should be retained.
        let after = maint.segment_ranges_strict().unwrap();
        assert!(
            after.iter().any(|s| s.segment_id == 1),
            "segment 1 should be retained by observer"
        );
        // Later segments stay too; deleting around a retained segment would
        // create an internal WAL hole.
        assert_eq!(deleted, 0);
        assert_eq!(after.len(), ranges.len());
    }

    #[test]
    fn wal_observer_on_sync_fires() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct SyncCounter(AtomicUsize);
        impl super::WalObserver for SyncCounter {
            fn on_sync(&self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let dir: Arc<dyn Directory> =
            Arc::new(crate::storage::FsDirectory::new(tmp.path()).unwrap());
        let obs = Arc::new(SyncCounter(AtomicUsize::new(0)));
        let mut w = WalWriter::<WalEntry>::new(dir.clone());
        w.set_observer(obs.clone() as Arc<dyn super::WalObserver>);

        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        w.flush_and_sync().unwrap();

        assert_eq!(obs.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn wal_flush_and_sync_poisons_on_failure() {
        // MemoryDirectory's sync_file returns NotSupported, which should poison.
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::new(dir.clone());

        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();

        // flush_and_sync fails on MemoryDirectory (no file_path).
        let err = w.flush_and_sync().unwrap_err();
        assert!(matches!(
            err,
            crate::error::PersistenceError::NotSupported(_)
        ));

        // Writer should now be poisoned.
        let err2 = w.append(&WalEntry::AddSegment {
            segment_id: 2,
            doc_count: 2,
        });
        assert!(err2.is_err());
        assert!(err2.unwrap_err().to_string().contains("poisoned"));
    }

    #[test]
    fn wal_segment_recycling() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        // Phase 1: write entries across multiple segments.
        let mut w = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            crate::storage::FlushPolicy::PerAppend,
            0,
        );
        w.set_segment_size_limit_bytes(80); // force rotation
        w.set_recycle_capacity(2);

        for i in 0..5u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i + 1,
                doc_count: i as u32,
            })
            .unwrap();
        }
        w.flush().unwrap();

        // Count segments before truncation.
        let maint = WalMaintenance::new(dir.clone());
        let ranges = maint.segment_ranges_strict().unwrap();
        assert!(ranges.len() > 1, "need multiple segments");

        // Truncate covered segments and recycle them instead of deleting.
        let last_covered = ranges[0].end_entry_id.unwrap();
        let recyclable = maint.truncate_to_recycle(last_covered).unwrap();
        for path in recyclable {
            w.recycle_segment(path);
        }

        // Phase 2: write more entries -- these should reuse recycled segment files.
        for i in 5..10u64 {
            w.append(&WalEntry::AddSegment {
                segment_id: i + 1,
                doc_count: i as u32,
            })
            .unwrap();
        }
        w.flush().unwrap();
        drop(w);

        // All entries from phase 2 and un-truncated phase 1 should be replayable.
        let r = WalReader::<WalEntry>::new(dir);
        let records = r.replay().unwrap();
        // We truncated entries up to last_covered; remaining entries start after.
        assert!(records.len() >= 5, "expected at least phase 2 entries");
        // Entry IDs must be strictly increasing.
        for win in records.windows(2) {
            assert!(win[1].entry_id > win[0].entry_id);
        }
    }

    #[test]
    fn sync_wal_writer_basic() {
        let dir = MemoryDirectory::arc();
        let sw = SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap();

        let id1 = sw
            .append(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 1,
            })
            .unwrap();
        let id2 = sw
            .append(&WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 2,
            })
            .unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);

        sw.flush().unwrap();
        assert_eq!(sw.last_entry_id(), Some(2));
        drop(sw);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn sync_wal_writer_concurrent_appends() {
        let dir = MemoryDirectory::arc();
        let sw = Arc::new(SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap());

        let mut handles = Vec::new();
        for t in 0..4u64 {
            let sw = sw.clone();
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::new();
                for i in 0..25u64 {
                    let id = sw
                        .append(&WalEntry::AddSegment {
                            segment_id: t * 100 + i,
                            doc_count: 0,
                        })
                        .unwrap();
                    ids.push(id);
                }
                ids
            }));
        }

        let mut all_ids: Vec<u64> = Vec::new();
        for h in handles {
            all_ids.extend(h.join().unwrap());
        }
        sw.flush().unwrap();
        drop(sw);

        // All 100 entries should be replayable with strictly increasing IDs.
        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 100);
        for w in records.windows(2) {
            assert!(w[1].entry_id > w[0].entry_id);
        }

        // Every assigned ID should appear exactly once.
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), 100);
    }

    #[test]
    fn sync_wal_writer_append_durable_on_fs() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = crate::storage::FsDirectory::arc(tmp.path()).unwrap();
        let sw = SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap();

        let id = sw
            .append_durable(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 5,
            })
            .unwrap();
        assert_eq!(id, 1);

        // A second durable append should sync its own entry and still replay.
        let id2 = sw
            .append_durable(&WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 10,
            })
            .unwrap();
        assert_eq!(id2, 2);
        drop(sw);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn sync_wal_writer_configure() {
        let dir = MemoryDirectory::arc();
        let sw = SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap();

        sw.configure(|w| {
            w.set_segment_size_limit_bytes(500);
        })
        .unwrap();

        for i in 0..20u64 {
            sw.append(&WalEntry::AddSegment {
                segment_id: i + 1,
                doc_count: 0,
            })
            .unwrap();
        }
        sw.flush().unwrap();
        drop(sw);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 20);
    }

    #[test]
    fn sync_wal_writer_resume() {
        let dir = MemoryDirectory::arc();

        // Write some entries, then drop.
        {
            let sw = SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap();
            sw.append(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 1,
            })
            .unwrap();
            sw.flush().unwrap();
        }

        // Resume picks up the last entry ID.
        let sw = SyncWalWriter::<WalEntry>::resume(dir.clone()).unwrap();
        let id = sw
            .append(&WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 2,
            })
            .unwrap();
        assert_eq!(id, 2);
        sw.flush().unwrap();
        drop(sw);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn sync_wal_writer_resume_after_crash() {
        let dir = MemoryDirectory::arc();

        {
            let sw = SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap();
            sw.append(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 1,
            })
            .unwrap();
            sw.flush().unwrap();
        }

        dir.atomic_write("wal/.lock", b"stale").unwrap();
        let sw = SyncWalWriter::<WalEntry>::resume_after_crash(dir.clone()).unwrap();
        let id = sw
            .append(&WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 2,
            })
            .unwrap();
        assert_eq!(id, 2);
        sw.flush().unwrap();
        drop(sw);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn sync_wal_writer_append_batch() {
        let dir = MemoryDirectory::arc();
        let sw = SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap();

        let entries = vec![
            WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 1,
            },
            WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 2,
            },
        ];
        let ids = sw.append_batch(&entries).unwrap();
        assert_eq!(ids, vec![1, 2]);
        sw.flush().unwrap();
        drop(sw);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn sync_wal_writer_concurrent_mixed_durable() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = crate::storage::FsDirectory::arc(tmp.path()).unwrap();
        let sw = Arc::new(SyncWalWriter::<WalEntry>::open(dir.clone()).unwrap());

        let mut handles = Vec::new();
        // Thread 1: append without sync
        let sw1 = sw.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..10u64 {
                sw1.append(&WalEntry::AddSegment {
                    segment_id: 100 + i,
                    doc_count: 0,
                })
                .unwrap();
            }
        }));
        // Thread 2: durable appends interleaved with non-durable appends.
        let sw2 = sw.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..10u64 {
                sw2.append_durable(&WalEntry::AddSegment {
                    segment_id: 200 + i,
                    doc_count: 0,
                })
                .unwrap();
            }
        }));

        for h in handles {
            h.join().unwrap();
        }
        sw.flush().unwrap();
        drop(sw);

        let records = WalReader::<WalEntry>::new(dir).replay().unwrap();
        assert_eq!(records.len(), 20);
        for w in records.windows(2) {
            assert!(w[1].entry_id > w[0].entry_id);
        }
    }

    #[test]
    fn wal_recycle_capacity_zero_deletes() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            crate::storage::FlushPolicy::PerAppend,
            0,
        );
        w.set_recycle_capacity(0); // disabled

        w.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        w.flush().unwrap();

        // Manually call recycle_segment with capacity 0 -- should delete.
        dir.atomic_write("wal/old.log", b"dummy").unwrap();
        assert!(dir.exists("wal/old.log"));
        w.recycle_segment("wal/old.log".to_string());
        assert!(!dir.exists("wal/old.log"));
    }

    #[test]
    fn wal_recycle_pool_eviction() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w = WalWriter::<WalEntry>::with_options(
            dir.clone(),
            crate::storage::FlushPolicy::PerAppend,
            0,
        );
        w.set_recycle_capacity(2);

        // Create 3 dummy files and recycle all 3.
        for i in 0..3u64 {
            let path = format!("wal/old_{i}.log");
            dir.atomic_write(&path, b"dummy").unwrap();
            w.recycle_segment(path);
        }

        // Pool capacity is 2, so old_0 should have been evicted (deleted).
        assert!(!dir.exists("wal/old_0.log"), "oldest should be evicted");
        assert!(dir.exists("wal/old_1.log"), "second should be retained");
        assert!(dir.exists("wal/old_2.log"), "newest should be retained");
    }

    #[test]
    fn wal_recycled_missing_path_errors_instead_of_silent_fallback() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
        let mut w =
            WalWriter::<WalEntry>::with_options(dir, crate::storage::FlushPolicy::PerAppend, 0);
        w.set_recycle_capacity(1);
        w.recycle_segment("wal/missing.log".to_string());

        let err = w
            .append(&WalEntry::AddSegment {
                segment_id: 1,
                doc_count: 1,
            })
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn wal_lockfile_prevents_double_writer() {
        let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

        let mut w1 = WalWriter::<WalEntry>::new(dir.clone());
        w1.append(&WalEntry::AddSegment {
            segment_id: 1,
            doc_count: 1,
        })
        .unwrap();
        w1.flush().unwrap();

        // Second writer on same dir should fail
        let mut w2 = WalWriter::<WalEntry>::with_flush_policy(
            dir.clone(),
            crate::storage::FlushPolicy::PerAppend,
        );
        let err = w2.append(&WalEntry::AddSegment {
            segment_id: 2,
            doc_count: 1,
        });
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("lockfile"));

        let err = match WalWriter::<WalEntry>::resume(dir.clone()) {
            Ok(_) => panic!("resume should reject a live lockfile"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("lockfile"));

        // Drop first writer, leave a stale lock, then explicit crash resume works.
        drop(w1);
        dir.atomic_write("wal/.lock", b"stale").unwrap();
        let mut w3 = WalWriter::<WalEntry>::resume_after_crash(dir.clone()).unwrap();
        let id = w3
            .append(&WalEntry::AddSegment {
                segment_id: 2,
                doc_count: 1,
            })
            .unwrap();
        assert_eq!(id, 2);
        w3.flush().unwrap();
    }
}
