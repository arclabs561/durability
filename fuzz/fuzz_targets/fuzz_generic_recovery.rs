#![no_main]

use durability::recover::{recover_with_wal, RecoveryOptions};
use durability::storage::{Directory, MemoryDirectory};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

// Feed random bytes as checkpoint file and WAL segments.
// Verify `recover_with_wal` never panics regardless of input.
//
// Layout:
// - byte 0: flags (bit 0: has checkpoint, bit 1: best-effort mode,
//   bit 2: point-in-time ceiling)
// - if point-in-time ceiling: byte cutoff (0..=31)
// - if has checkpoint: [len:u16 LE][len bytes of checkpoint blob]
// - then n WAL segments: [len:u16 LE][len bytes of segment blob]
//   (segments until input exhausted)
fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let flags = data[0];
    let has_checkpoint = flags & 1 != 0;
    let best_effort = flags & 2 != 0;
    let point_in_time = flags & 4 != 0;
    let mut i = 1usize;

    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());

    let cutoff = if point_in_time && i < data.len() {
        let cutoff = (data[i] % 32) as u64;
        i += 1;
        Some(cutoff)
    } else {
        None
    };

    // Write checkpoint blob if flagged.
    let ckpt_path = if has_checkpoint && i + 2 <= data.len() {
        let len = u16::from_le_bytes([data[i], data[i + 1]]) as usize;
        i += 2;
        let end = (i + len).min(data.len());
        let blob = &data[i..end];
        i = end;
        let blob = if blob.len() > 4096 {
            &blob[..4096]
        } else {
            blob
        };
        dir.create_dir_all("checkpoints").ok();
        let _ = dir.atomic_write("checkpoints/fuzz.chk", blob);
        Some("checkpoints/fuzz.chk")
    } else {
        None
    };

    // Write WAL segments.
    dir.create_dir_all("wal").ok();
    let mut seg_id = 1u64;
    while i + 2 <= data.len() {
        let len = u16::from_le_bytes([data[i], data[i + 1]]) as usize;
        i += 2;
        let end = (i + len).min(data.len());
        let blob = &data[i..end];
        i = end;
        let blob = if blob.len() > 4096 {
            &blob[..4096]
        } else {
            blob
        };
        let _ = dir.atomic_write(&format!("wal/wal_{seg_id}.log"), blob);
        seg_id += 1;
        if seg_id > 16 {
            break;
        }
    }

    // Use a simple counter as the checkpoint and entry type.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct FuzzCheckpoint {
        n: u64,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct FuzzEntry {
        v: u64,
    }

    let mut options = if best_effort {
        RecoveryOptions::best_effort()
    } else {
        RecoveryOptions::strict()
    };
    options.up_to_entry_id = cutoff;

    // Must not panic.
    let _ = recover_with_wal::<FuzzCheckpoint, FuzzEntry, _>(
        &dir,
        ckpt_path,
        options,
        |ckpt| ckpt.map(|c| c.n).unwrap_or(0),
        |state, _id, entry| {
            *state = state.wrapping_add(entry.v);
        },
    );
});
