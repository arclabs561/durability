//! Error types for `durability`.

use std::path::PathBuf;

/// Result type for durability/persistence operations.
pub type PersistenceResult<T> = Result<T, PersistenceError>;

/// Errors returned by the `durability` crate.
#[derive(thiserror::Error, Debug)]
pub enum PersistenceError {
    /// I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Format error (corrupt, unexpected, unsupported).
    #[error("format error: {0}")]
    Format(String),

    /// Format error with optional expected/actual context.
    ///
    /// `expected`/`actual` exist to make “format mismatch” debugging concrete without
    /// forcing every caller into bespoke error enums.
    #[error("format error: {message}")]
    FormatDetail {
        /// Short, human-readable description of the mismatch.
        message: String,
        /// Optional “expected” value (stringified) for debugging.
        expected: Option<String>,
        /// Optional “actual” value (stringified) for debugging.
        actual: Option<String>,
    },

    /// CRC mismatch (data corruption detected).
    #[error("crc mismatch (expected {expected:#010x}, got {actual:#010x})")]
    CrcMismatch {
        /// CRC stored in the file/record header.
        expected: u32,
        /// CRC computed from the bytes that were read.
        actual: u32,
    },

    /// Encoding error.
    #[error("encode error: {0}")]
    Encode(String),

    /// Decoding error.
    #[error("decode error: {0}")]
    Decode(String),

    /// Invalid state (operation not allowed in current state).
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// Invalid configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Operation not supported.
    #[error("operation not supported: {0}")]
    NotSupported(String),

    /// Lock acquisition failed (concurrent access conflict).
    #[error("lock failed on {resource}: {reason}")]
    LockFailed {
        /// What we were trying to lock (file path, in-memory map, etc.).
        resource: String,
        /// Human-readable reason (poisoned lock, OS error, etc.).
        reason: String,
    },

    /// Resource not found (file/segment/etc).
    #[error("not found: {0}")]
    NotFound(String),

    /// Requested path does not exist.
    #[error("missing path: {0}")]
    MissingPath(PathBuf),
}
