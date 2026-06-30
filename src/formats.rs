//! On-disk format constants and small shared structs.

/// Magic bytes for checkpoint files.
pub const CHECKPOINT_MAGIC: [u8; 4] = *b"VCKP";
/// Magic bytes for record log files.
pub const RECORDLOG_MAGIC: [u8; 4] = *b"VRLG";
/// Magic bytes for WAL segment files.
pub const WAL_MAGIC: [u8; 4] = *b"VWAL";
/// On-disk format version for recordlog and checkpoint primitives.
///
/// Bump this when the recordlog or checkpoint framing changes.
pub const FORMAT_VERSION: u32 = 1;

/// On-disk format version for WAL segment files.
///
/// Version 2: entry_id moved from payload to frame header; type byte removed.
/// Frame: `[length:u32][entry_id:u64][crc32:u32][payload bytes...]`.
pub const WAL_FORMAT_VERSION: u32 = 2;
