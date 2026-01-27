#![no_main]

use durability::storage::{Directory, MemoryDirectory};
use durability::walog::{WalMaintenance, WalReader};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

/// Interpret input bytes as a bounded set of WAL segment blobs.
///
/// Layout (very simple, deterministic):
/// - byte 0: n = number of segments (0..=15)
/// - then n chunks: [len:u16 LE][len bytes of payload]
fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let n = (data[0] % 16) as usize;
    let mut i = 1usize;

    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
    dir.create_dir_all("wal").ok();

    for seg_id in 1..=n {
        if i + 2 > data.len() {
            break;
        }
        let len = u16::from_le_bytes([data[i], data[i + 1]]) as usize;
        i += 2;
        let end = (i + len).min(data.len());
        let blob = &data[i..end];
        i = end;

        // Cap per-segment size to keep the fuzzer fast.
        let blob = if blob.len() > 4096 { &blob[..4096] } else { blob };
        let _ = dir.atomic_write(&format!("wal/wal_{seg_id}.log"), blob);
    }

    let r = WalReader::new(dir.clone());
    let _ = r.replay_best_effort();
    let _ = r.replay();

    // Maintenance logic should also never panic.
    let m = WalMaintenance::new(dir);
    let _ = m.segment_ranges_strict();
});

