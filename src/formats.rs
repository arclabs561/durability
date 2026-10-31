//! On-disk format constants and small shared structs.

/// Magic bytes for checkpoint files.
pub const CHECKPOINT_MAGIC: [u8; 4] = *b"VCKP";
/// Magic bytes for record log files.
pub const RECORDLOG_MAGIC: [u8; 4] = *b"VRLG";
/// Magic bytes for WAL segment files.
pub const WAL_MAGIC: [u8; 4] = *b"VWAL";
/// Original on-disk format version, shared by version 1 record logs and
/// legacy checkpoints.
///
/// Version 1 record logs checksum only the payload of each record. Readers and
/// writers still accept them; new record logs use [`RECORDLOG_FORMAT_VERSION`].
pub const FORMAT_VERSION: u32 = 1;

/// On-disk format version for new recordlog files.
///
/// Version 2: record checksum covers `len | payload`, so a zero-filled frame
/// (`len = 0`, `crc = 0`) no longer decodes as a valid empty record.
/// Version 1 ([`FORMAT_VERSION`]): checksum covers the payload bytes only.
pub const RECORDLOG_FORMAT_VERSION: u32 = 2;

/// On-disk format version for checkpoint files.
///
/// Version 2: checksum covers `magic | version | last_applied_id | payload_len | payload`.
/// Version 1: legacy checksum covers payload bytes only.
pub const CHECKPOINT_FORMAT_VERSION: u32 = 2;

/// On-disk format version for WAL segment files.
///
/// Version 2: entry_id moved from payload to frame header; type byte removed.
/// Frame: `[length:u32][entry_id:u64][crc32:u32][payload bytes...]`.
/// Version 3: frame checksum covers `length | entry_id | payload`.
pub const WAL_FORMAT_VERSION: u32 = 3;
