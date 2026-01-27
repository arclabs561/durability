//! Generic replay helpers.
//!
//! These helpers exist to keep “checkpoint + log suffix” logic out of consumers.

use crate::error::{PersistenceError, PersistenceResult};
use crate::recordlog::{RecordLogReadMode, RecordLogReader};
use crate::storage::Directory;
use std::sync::Arc;

/// Replay postcard-decoded records from a log.
///
/// Returns the number of records applied.
pub fn replay_postcard<T: serde::de::DeserializeOwned>(
    dir: Arc<dyn Directory>,
    log_path: &str,
    mut apply: impl FnMut(T) -> PersistenceResult<()>,
) -> PersistenceResult<u64> {
    let reader = RecordLogReader::new(dir.clone(), log_path);
    let mut count = 0u64;
    if !dir.exists(log_path) {
        return Ok(0);
    }
    let mut f = match reader.open_stream() {
        Ok(f) => f,
        Err(PersistenceError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Best-effort: if the file exists but is too small to even contain a header,
            // treat it as an empty log (crash during initial creation).
            return Ok(0);
        }
        Err(e) => return Err(e),
    };
    while let Some(rec) = reader.next_record_inner(&mut *f, RecordLogReadMode::BestEffort)? {
        let v: T = postcard::from_bytes(&rec.payload)
            .map_err(|e| PersistenceError::Decode(e.to_string()))?;
        apply(v)?;
        count += 1;
    }
    Ok(count)
}

/// Replay postcard-decoded records from a log, skipping the first `skip` records.
///
/// This is the simplest “checkpoint + suffix” adapter when your checkpoint stores
/// `last_record_id` as a 1-based count.
pub fn replay_postcard_since<T: serde::de::DeserializeOwned>(
    dir: Arc<dyn Directory>,
    log_path: &str,
    skip: u64,
    mut apply: impl FnMut(T) -> PersistenceResult<()>,
) -> PersistenceResult<u64> {
    let reader = RecordLogReader::new(dir.clone(), log_path);
    let mut count = 0u64;
    if !dir.exists(log_path) {
        return Ok(0);
    }
    let mut f = match reader.open_stream() {
        Ok(f) => f,
        Err(PersistenceError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Best-effort: if the file exists but is too small to even contain a header,
            // treat it as an empty log (crash during initial creation).
            return Ok(0);
        }
        Err(e) => return Err(e),
    };
    let mut record_id = 0u64;
    while let Some(rec) = reader.next_record_inner(&mut *f, RecordLogReadMode::BestEffort)? {
        record_id += 1;
        if record_id <= skip {
            continue;
        }
        let v: T = postcard::from_bytes(&rec.payload)
            .map_err(|e| PersistenceError::Decode(e.to_string()))?;
        apply(v)?;
        count += 1;
    }
    Ok(count)
}
