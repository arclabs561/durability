#![no_main]

use durability::walog::{WalEntry, WalEntryOnDisk, WalReplayMode};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cur = std::io::Cursor::new(data);
    let _ = WalEntryOnDisk::decode::<WalEntry, _>(&mut cur, WalReplayMode::BestEffortTail);
});
