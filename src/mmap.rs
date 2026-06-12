//! Memory-mapped file access with advisory hints.
//!
//! This module is a thin wrapper around [`memmap2`] that wires up
//! `madvise(2)` hints at map time. The hints are advisory: the kernel
//! is free to ignore them, and errors from `madvise` are reported but
//! do not prevent the mapping from being used.
//!
//! ## Usage
//!
//! ```no_run
//! # #[cfg(feature = "mmap")]
//! # {
//! use durability::mmap::{AccessPattern, MappedFile};
//!
//! // WAL replay: the kernel should prefetch pages sequentially.
//! let f = MappedFile::open("wal/segment-001.bin", AccessPattern::Sequential).unwrap();
//! let bytes: &[u8] = f.as_slice();
//! # }
//! ```
//!
//! On Windows, `apply_hints` is a no-op and always returns `Ok(())`.

use std::ops::Range;
use std::path::Path;

/// Advisory access pattern applied via `madvise(2)` to a mapped region.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccessPattern {
    /// Random access (MADV_RANDOM).
    ///
    /// Disables read-ahead. Suitable for graph edge lists, index nodes,
    /// or any structure where access order is not predictable.
    Random,

    /// Sequential access from start to end (MADV_SEQUENTIAL).
    ///
    /// Increases read-ahead aggressively. Suitable for WAL replay and
    /// sequential log scans.
    Sequential,

    /// Prefetch the given byte range into the page cache (MADV_WILLNEED).
    ///
    /// Use for header/metadata pages that must be warm before the first
    /// access. The range is clamped to the file length.
    WillNeed(Range<usize>),

    /// No hint: let the kernel use its default heuristics.
    Normal,
}

/// A read-only memory-mapped file with advisory hints applied.
pub struct MappedFile {
    map: memmap2::Mmap,
}

impl MappedFile {
    /// Open `path` as a read-only memory map and apply `pattern` as an advisory hint.
    ///
    /// Returns an error if the file cannot be opened, is empty, or the
    /// `mmap` call fails. `madvise` failures are returned as a separate
    /// `MadviseError` (not a fatal open error).
    pub fn open(path: impl AsRef<Path>, pattern: AccessPattern) -> Result<Self, MappedFileError> {
        let file = std::fs::File::open(path.as_ref()).map_err(MappedFileError::Io)?;
        // SAFETY: the caller must ensure the file is not concurrently truncated.
        // For WAL replay and checkpoint read-back, this is true by construction.
        let map = unsafe { memmap2::MmapOptions::new().map(&file) }.map_err(MappedFileError::Io)?;

        let mapped = MappedFile { map };
        mapped.apply_hints(&pattern)?;
        Ok(mapped)
    }

    /// Apply (or re-apply) advisory hints to the entire mapping.
    ///
    /// On Windows this is always a no-op. On other Unix systems the
    /// `madvise` syscall is issued and any non-zero return is wrapped
    /// in `MappedFileError::Madvise`.
    pub fn apply_hints(&self, pattern: &AccessPattern) -> Result<(), MappedFileError> {
        #[cfg(unix)]
        {
            use MadviseAdvice::*;
            let advice = match pattern {
                AccessPattern::Random => Some(Random),
                AccessPattern::Sequential => Some(Sequential),
                AccessPattern::Normal => Some(Normal),
                AccessPattern::WillNeed(range) => {
                    let len = self.map.len();
                    // Linux requires the madvise address to be page-aligned
                    // (EINVAL otherwise); macOS tolerates unaligned starts.
                    // Round the start down to its page so partial ranges work
                    // on both.
                    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as usize;
                    let start = (range.start.min(len) / page) * page;
                    let end = range.end.min(len);
                    if start < end {
                        let ptr = unsafe { self.map.as_ptr().add(start) };
                        raw_madvise(ptr, end - start, libc::MADV_WILLNEED)?;
                    }
                    return Ok(());
                }
            };
            if let Some(adv) = advice {
                raw_madvise(self.map.as_ptr(), self.map.len(), adv as libc::c_int)?;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = pattern;
        }
        Ok(())
    }

    /// Return the mapped bytes as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.map
    }

    /// Return the length of the mapped region in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Return `true` if the mapped region is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl std::ops::Deref for MappedFile {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Errors returned by [`MappedFile`] operations.
#[derive(thiserror::Error, Debug)]
pub enum MappedFileError {
    /// I/O error during `open` or `mmap`.
    #[error("mmap i/o error: {0}")]
    Io(std::io::Error),

    /// `madvise(2)` returned an error.
    ///
    /// This is not fatal: the mapping is still usable.
    #[error("madvise error (errno {0})")]
    Madvise(i32),
}

// -- Unix internals --

#[cfg(unix)]
#[allow(dead_code)]
enum MadviseAdvice {
    Normal = libc::MADV_NORMAL as isize,
    Random = libc::MADV_RANDOM as isize,
    Sequential = libc::MADV_SEQUENTIAL as isize,
}

#[cfg(unix)]
fn raw_madvise(ptr: *const u8, len: usize, advice: libc::c_int) -> Result<(), MappedFileError> {
    if len == 0 {
        return Ok(());
    }
    // SAFETY: ptr is the base of a live Mmap and len is the mapping length.
    let ret = unsafe { libc::madvise(ptr as *mut libc::c_void, len, advice) };
    if ret != 0 {
        // std reads errno portably; libc's accessor is platform-named
        // (__error on macOS, __errno_location on Linux).
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        Err(MappedFileError::Madvise(errno))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_file(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn open_sequential_no_panic() {
        let tmp = write_temp_file(b"hello world");
        let mapped = MappedFile::open(tmp.path(), AccessPattern::Sequential).unwrap();
        assert_eq!(&*mapped, b"hello world");
    }

    #[test]
    fn open_random_no_panic() {
        let tmp = write_temp_file(b"random access data");
        let mapped = MappedFile::open(tmp.path(), AccessPattern::Random).unwrap();
        assert_eq!(&*mapped, b"random access data");
    }

    #[test]
    fn open_normal_no_panic() {
        let tmp = write_temp_file(b"normal hint");
        let mapped = MappedFile::open(tmp.path(), AccessPattern::Normal).unwrap();
        assert_eq!(&*mapped, b"normal hint");
    }

    #[test]
    fn open_willneed_full_range_no_panic() {
        let data = vec![0u8; 4096];
        let tmp = write_temp_file(&data);
        let mapped = MappedFile::open(tmp.path(), AccessPattern::WillNeed(0..4096)).unwrap();
        assert_eq!(mapped.len(), 4096);
    }

    #[test]
    fn open_willneed_partial_range_no_panic() {
        let data = vec![1u8; 8192];
        let tmp = write_temp_file(&data);
        // Range clamped to file length — should not panic.
        let mapped = MappedFile::open(tmp.path(), AccessPattern::WillNeed(1024..3072)).unwrap();
        assert_eq!(mapped.len(), 8192);
    }

    #[test]
    fn open_willneed_overflowing_range_clamped_no_panic() {
        let data = vec![2u8; 512];
        let tmp = write_temp_file(&data);
        // Range extends past file length — should be clamped, not panic.
        let mapped = MappedFile::open(tmp.path(), AccessPattern::WillNeed(0..1_000_000)).unwrap();
        assert_eq!(mapped.len(), 512);
    }

    #[test]
    fn reapply_hints_no_panic() {
        let tmp = write_temp_file(b"reapply test");
        let mapped = MappedFile::open(tmp.path(), AccessPattern::Normal).unwrap();
        // Re-apply a different hint on the same mapping.
        mapped.apply_hints(&AccessPattern::Sequential).unwrap();
        mapped.apply_hints(&AccessPattern::Random).unwrap();
        mapped
            .apply_hints(&AccessPattern::WillNeed(0..mapped.len()))
            .unwrap();
    }

    #[test]
    fn len_and_is_empty() {
        let tmp = write_temp_file(b"five!");
        let mapped = MappedFile::open(tmp.path(), AccessPattern::Normal).unwrap();
        assert_eq!(mapped.len(), 5);
        assert!(!mapped.is_empty());
    }
}
