//! On-disk format constants and small shared structs.

use serde::{Deserialize, Serialize};

/// Magic bytes for segment files.
pub const SEGMENT_MAGIC: &[u8; 4] = b"VCNT";
/// Magic bytes for checkpoint files.
pub const CHECKPOINT_MAGIC: [u8; 4] = *b"VCKP";
/// Magic bytes for record log files.
pub const RECORDLOG_MAGIC: [u8; 4] = *b"VRLG";
/// Magic bytes for WAL segment files.
pub const WAL_MAGIC: [u8; 4] = *b"VWAL";
/// Current on-disk format version for durability primitives.
pub const FORMAT_VERSION: u32 = 1;

/// Byte offsets for segment sections (term dict, postings, etc.).
#[derive(Debug, Clone, Default)]
pub struct SegmentOffsets {
    /// Start offset of the term dictionary section.
    pub term_dict_offset: u64,
    /// Length of the term dictionary section.
    pub term_dict_len: u64,
    /// Start offset of the term-info section.
    pub term_info_offset: u64,
    /// Length of the term-info section.
    pub term_info_len: u64,
    /// Start offset of the postings section.
    pub postings_offset: u64,
    /// Length of the postings section.
    pub postings_len: u64,
    /// Start offset of the doc-lengths section.
    pub doc_lengths_offset: u64,
    /// Length of the doc-lengths section.
    pub doc_lengths_len: u64,
}

/// Footer stored at the end of a segment directory/file family.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SegmentFooter {
    /// Magic bytes (should equal `SEGMENT_MAGIC`).
    pub magic: [u8; 4],
    /// Format version (should equal `FORMAT_VERSION`).
    pub format_version: u32,
    /// Offset of term dictionary section.
    pub term_dict_offset: u64,
    /// Length of term dictionary section.
    pub term_dict_len: u64,
    /// Offset of postings section.
    pub postings_offset: u64,
    /// Length of postings section.
    pub postings_len: u64,
    /// Number of documents in the segment.
    pub doc_count: u32,
    /// Maximum document id stored in this segment.
    pub max_doc_id: u32,
    /// Checksum over segment metadata/payload (implementation-defined).
    pub checksum: u32,
}

impl SegmentFooter {
    /// Construct a footer from basic segment stats and offsets.
    pub fn new(doc_count: u32, max_doc_id: u32, offsets: SegmentOffsets) -> Self {
        Self {
            magic: *SEGMENT_MAGIC,
            format_version: FORMAT_VERSION,
            term_dict_offset: offsets.term_dict_offset,
            term_dict_len: offsets.term_dict_len,
            postings_offset: offsets.postings_offset,
            postings_len: offsets.postings_len,
            doc_count,
            max_doc_id,
            checksum: 0,
        }
    }
}
