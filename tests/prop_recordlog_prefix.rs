//! Property-based “fuzz-like” tests for `recordlog`.

use durability::recordlog::{RecordLogReadMode, RecordLogReader, RecordLogWriter};
use durability::storage::{Directory, FsDirectory};
use proptest::prelude::*;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Msg {
    // Include diverse scripts (Unicode scalar values).
    // We keep the schema tiny so proptest cases stay cheap.
    text: String,
}

fn diverse_texts() -> Vec<String> {
    vec![
        "Marie Curie discovered radium in Paris.".into(),
        "習近平在北京會見了普京。".into(),
        "التقى محمد بن سلمان بالرئيس في الرياض".into(),
        "Путин встретился с Си Цзиньпином в Москве.".into(),
        "Dr. 田中 presented her research at MIT's AI conference.".into(),
        "Москва (Moskva/Moscow) hosted the 東京 (Tokyo) delegation.".into(),
        "François Müller and José García met in São Paulo.".into(),
        "Pelé, Madonna, and Cher performed at the concert.".into(),
        "Her Majesty Queen Elizabeth II met Prime Minister शर्मा.".into(),
    ]
}

fn arb_msgs() -> impl Strategy<Value = Vec<Msg>> {
    let texts = diverse_texts();
    prop::collection::vec(
        prop::sample::select(texts).prop_map(|t| Msg { text: t }),
        0..30,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Integration-test crates don't have a stable lib.rs/main.rs path for proptest's
        // SourceParallel persistence mode. Disable persistence; we still get shrinking.
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    // This is fuzz-like: it generates many truncation positions.
    #[test]
    fn recordlog_best_effort_returns_prefix_under_truncation(
        msgs in arb_msgs(),
        tear_extra in 0usize..64
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = FsDirectory::new(tmp.path()).unwrap();
        let dir: Arc<dyn Directory> = Arc::new(dir);

        let mut w = RecordLogWriter::new(dir.clone(), "log.bin");
        for m in &msgs {
            w.append_postcard(m).unwrap();
        }
        w.flush().unwrap();

        // Tear some bytes off the end (simulate crash/torn write).
        if dir.exists("log.bin") {
            let Some(path) = dir.file_path("log.bin") else {
                panic!("FsDirectory must return file_path()");
            };
            let mut bytes = std::fs::read(&path).unwrap();
            if !bytes.is_empty() {
                let drop = tear_extra.min(bytes.len());
                bytes.truncate(bytes.len().saturating_sub(drop));
                std::fs::write(&path, bytes).unwrap();
            }
        }

        let r = RecordLogReader::new(dir.clone(), "log.bin");
        let out: Vec<Msg> = r.read_all_postcard(RecordLogReadMode::BestEffort).unwrap();

        // Property: output is a prefix of the original sequence (no garbage, no reordering).
        prop_assert!(out.len() <= msgs.len());
        prop_assert_eq!(&msgs[..out.len()], &out[..]);
    }
}
