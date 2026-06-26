//! Crash-consistency eval: truncate a record log at every byte offset and
//! verify recovery is always clean.
//!
//! A crash can leave the log truncated at any byte (a torn final record). The
//! durability contract: BestEffort recovery returns the longest valid PREFIX of
//! committed records, never a corrupted record, never a panic, and never an
//! error. This exhaustively simulates a crash at every possible offset and
//! checks that invariant, which is exactly the integration-level property that
//! per-record unit tests do not cover.
//!
//! ```sh
//! cargo run --release --example crash_recovery_eval
//! ```

use std::fs::OpenOptions;
use std::process::ExitCode;

use durability::recordlog::{RecordLogReadMode, RecordLogReader, RecordLogWriter};
use durability::storage::FsDirectory;

fn main() -> ExitCode {
    let tmp = match tempfile::tempdir() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("tempdir failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let dir = FsDirectory::arc(tmp.path()).expect("fs directory");
    let path = "log.bin";

    // Commit N records, each fully flushed+synced so all are durable.
    const N: usize = 100;
    let committed: Vec<String> = (0..N).map(|i| format!("record-{i:05}-payload")).collect();
    {
        let mut w = RecordLogWriter::new(dir.clone(), path);
        for r in &committed {
            w.append_postcard(r).expect("append");
        }
        w.flush_and_sync().expect("flush_and_sync");
    }

    let file = tmp.path().join(path);
    let full_len = std::fs::metadata(&file).expect("metadata").len();
    println!("committed {N} records into {full_len} bytes; truncating at every offset");

    // Truncate from full length down to 0 (monotonic shrink); at each length,
    // recover and check the prefix invariant.
    let mut max_recovered = 0usize;
    let mut violations = 0u64;
    let mut panics_or_errors = 0u64;

    for len in (0..=full_len).rev() {
        OpenOptions::new()
            .write(true)
            .open(&file)
            .expect("open")
            .set_len(len)
            .expect("set_len");

        let reader = RecordLogReader::new(dir.clone(), path);
        let recovered = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reader.read_all_postcard::<String>(RecordLogReadMode::BestEffort)
        })) {
            Ok(Ok(v)) => v,
            Ok(Err(_)) => {
                panics_or_errors += 1; // BestEffort must not error on a torn tail
                continue;
            }
            Err(_) => {
                panics_or_errors += 1; // recovery must not panic
                continue;
            }
        };

        max_recovered = max_recovered.max(recovered.len());
        // Invariant: recovered is a clean prefix of the committed records.
        if recovered.len() > committed.len()
            || recovered
                .iter()
                .enumerate()
                .any(|(i, r)| r != &committed[i])
        {
            violations += 1;
        }
    }

    println!("max records recovered (at full length): {max_recovered} / {N}");
    println!("prefix-invariant violations: {violations}");
    println!("BestEffort errors/panics on a torn tail: {panics_or_errors}");

    let ok = violations == 0 && panics_or_errors == 0 && max_recovered == N;
    if ok {
        println!("\nPASS: every truncation recovered a clean committed prefix");
        ExitCode::SUCCESS
    } else {
        eprintln!("\nFAIL: crash-recovery invariant violated");
        ExitCode::FAILURE
    }
}
