#![no_main]

use durability::checkpoint::CheckpointHeader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cur = std::io::Cursor::new(data);
    let _ = CheckpointHeader::read(&mut cur);
});

