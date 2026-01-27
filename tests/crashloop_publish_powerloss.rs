//! Crash-loop power-loss harness for checkpoint publishing.
//!
//! This is inspired by `sled`-style crash-loop tests: we run a sequence of operations,
//! periodically "crash + power-loss", and assert recovery always returns the last
//! durably-published state.
//!
//! Model:
//! - We treat a successful `CheckpointPublisher::publish_checkpoint` as a durable barrier.
//! - A simulated power loss discards all filesystem state since the last barrier by restoring
//!   a snapshot.
//! - After restore, strict `recover_latest()` must succeed and match the last snapshot state.
//!
//! This test does **not** claim to be a filesystem model; it is a regression harness that:
//! - repeatedly exercises publish/truncate + resume logic
//! - forces replay/restore cycles
//! - ensures we never "forget" a committed checkpoint across a power-loss restore
//!
//! If you want additional coverage for IO errors (not crashes), see the existing fault-injection
//! tests in `fault_publish_*`.

use durability::publish::CheckpointPublisher;
use durability::recover::{RecoveredSegment, RecoveredState, RecoveryManager};
use durability::storage::{Directory, FsDirectory};
use durability::walog::{WalEntry, WalWriter};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Copy)]
struct TinyRng(u64);
impl TinyRng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn gen_range_u64(&mut self, lo: u64, hi_exclusive: u64) -> u64 {
        debug_assert!(lo < hi_exclusive);
        lo + (self.next_u64() % (hi_exclusive - lo))
    }
    fn gen_bool(&mut self, numerator: u64, denominator: u64) -> bool {
        self.next_u64() % denominator < numerator
    }
}

fn ensure_empty_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path).unwrap();
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    ensure_empty_dir(dst);
    if !src.exists() {
        return;
    }
    fn rec(src: &Path, dst: &Path) {
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let name = entry.file_name();
            let dst_path = dst.join(name);
            let ft = entry.file_type().unwrap();
            if ft.is_dir() {
                std::fs::create_dir_all(&dst_path).unwrap();
                rec(&path, &dst_path);
            } else if ft.is_file() {
                if let Some(parent) = dst_path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::copy(&path, &dst_path).unwrap();
            }
        }
    }
    rec(src, dst);
}

fn normalize_state(state: &RecoveredState) -> BTreeMap<u64, (u32, BTreeSet<u32>)> {
    let mut out = BTreeMap::new();
    for RecoveredSegment {
        segment_id,
        doc_count,
        deleted_docs,
    } in &state.segments
    {
        out.insert(
            *segment_id,
            (
                *doc_count,
                deleted_docs.iter().copied().collect::<BTreeSet<u32>>(),
            ),
        );
    }
    out
}

fn assert_state_eq(a: &RecoveredState, b: &RecoveredState) {
    assert_eq!(normalize_state(a), normalize_state(b));
    assert_eq!(a.last_entry_id, b.last_entry_id);
}

fn durable_barrier_snapshot(work: &Path, durable: &Path) {
    copy_dir_recursive(work, durable);
}

fn restore_power_loss(work: &Path, durable: &Path) {
    copy_dir_recursive(durable, work);
}

fn run_seed(seed: u64) {
    let tmp = tempfile::tempdir().unwrap();
    let root: PathBuf = tmp.path().to_path_buf();
    let work = root.join("work");
    let durable = root.join("durable");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::create_dir_all(&durable).unwrap();
    durable_barrier_snapshot(&work, &durable);

    let mk_dir = || -> Arc<dyn Directory> { Arc::new(FsDirectory::new(&work).unwrap()) };

    // Expected state of the last durable barrier.
    let mut expected_dir = mk_dir();
    let expected_mgr = RecoveryManager::new(expected_dir.clone());
    let mut expected = expected_mgr.recover(None).unwrap();
    // Ensure our snapshot corresponds to the state we record.
    durable_barrier_snapshot(&work, &durable);

    let mut dir = mk_dir();
    let mut wal = WalWriter::new(dir.clone());
    wal.set_segment_size_limit_bytes(512); // keep segments small so truncation paths get exercised

    let mut rng = TinyRng::new(seed);
    let mut next_segment_id: u64 = 1;
    let mut segment_doc_counts: BTreeMap<u64, u32> = BTreeMap::new();

    for step in 0..250u64 {
        let action = rng.gen_range_u64(0, 10);
        match action {
            // Add segment
            0..=4 => {
                if next_segment_id <= 32 {
                    let doc_count = rng.gen_range_u64(1, 64) as u32;
                    let seg = next_segment_id;
                    next_segment_id += 1;
                    segment_doc_counts.insert(seg, doc_count);
                    wal.append(WalEntry::AddSegment {
                        entry_id: 0,
                        segment_id: seg,
                        doc_count,
                    })
                    .unwrap();
                }
            }
            // Delete one doc (maybe against a missing segment; recovery ignores those)
            5..=7 => {
                if next_segment_id > 1 {
                    let seg = rng.gen_range_u64(1, next_segment_id);
                    let max_doc = segment_doc_counts.get(&seg).copied().unwrap_or(1).max(1);
                    let doc = rng.gen_range_u64(0, max_doc as u64) as u32;
                    wal.append(WalEntry::DeleteDocuments {
                        entry_id: 0,
                        deletes: vec![(seg, doc)],
                    })
                    .unwrap();
                }
            }
            // Try to publish a checkpoint (durable barrier)
            8 => {
                // Avoid doing heavy publish every time.
                if !rng.gen_bool(1, 3) {
                    continue;
                }

                wal.flush().unwrap();

                // Compute checkpoint from current strict recovery view.
                let mgr = RecoveryManager::new(dir.clone());
                let before = mgr.recover(None).unwrap();
                let ckpt_state = RecoveryManager::to_checkpoint_state(&before);
                let last = before.last_entry_id;

                let mut publisher_wal = WalWriter::resume(dir.clone()).unwrap();
                publisher_wal.set_segment_size_limit_bytes(512);

                let res = CheckpointPublisher::new(dir.clone()).publish_checkpoint(
                    &mut publisher_wal,
                    &ckpt_state,
                    last,
                    "checkpoints/latest.chk",
                );
                let _pub_res = res.unwrap();

                // After a successful publish, strict latest recovery must work.
                let after = mgr.recover_latest().unwrap();

                // Snapshot "disk" after this durable barrier.
                durable_barrier_snapshot(&work, &durable);

                // Update expected durable state.
                expected = after;

                // Continue with a fresh WAL writer (simulate app restarting after maintenance).
                wal = WalWriter::resume(dir.clone()).unwrap();
                wal.set_segment_size_limit_bytes(512);
            }
            // Crash + power loss restore
            _ => {
                // Simulate crash: drop the WAL writer (close file handles).
                drop(wal);

                // Power loss: discard all state since last durable snapshot.
                restore_power_loss(&work, &durable);

                // Re-open directory and verify recovery equals expected durable state.
                expected_dir = mk_dir();
                let mgr = RecoveryManager::new(expected_dir.clone());
                let got = mgr.recover_latest().unwrap_or_else(|e| {
                    panic!("recover_latest failed after restore (seed={seed}, step={step}): {e}")
                });
                assert_state_eq(&got, &expected);

                // Continue appending to WAL after restart.
                dir = mk_dir();
                wal = WalWriter::resume(dir.clone()).unwrap();
                wal.set_segment_size_limit_bytes(512);
            }
        }
    }

    // Final restore and check.
    drop(wal);
    restore_power_loss(&work, &durable);
    let mgr = RecoveryManager::new(mk_dir());
    let got = mgr.recover_latest().unwrap();
    assert_state_eq(&got, &expected);
}

#[test]
fn crashloop_publish_powerloss() {
    // Multiple deterministic seeds to widen coverage while keeping runtime bounded.
    for seed in 0..16u64 {
        run_seed(seed);
    }
}
