#![no_main]

use durability::recordlog::{RecordLogReadMode, RecordLogReader};
use durability::storage::{Directory, MemoryDirectory};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    let dir: Arc<dyn Directory> = Arc::new(MemoryDirectory::new());
    // Put arbitrary bytes under the expected log path.
    dir.atomic_write("log.bin", data).ok();
    let r = RecordLogReader::new(dir, "log.bin");
    let _ = r.read_all(RecordLogReadMode::BestEffort);
});

