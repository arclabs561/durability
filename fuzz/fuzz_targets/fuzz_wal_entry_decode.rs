#![no_main]

use durability::walog::{WalEntryOnDisk, WalReplayMode};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cur = std::io::Cursor::new(data);
    let _ = WalEntryOnDisk::decode(&mut cur, WalReplayMode::BestEffortTail);
});

