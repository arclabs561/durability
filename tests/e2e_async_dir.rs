//! Integration tests for `AsyncDirectory` and `BlockingBridge`.
#![cfg(feature = "async")]

use durability::async_dir::{AsyncDirectory, BlockingBridge};
use durability::storage::FsDirectory;

#[tokio::test]
async fn blocking_bridge_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let bridge = BlockingBridge::new(FsDirectory::new(tmp.path()).unwrap());

    bridge.create_dir_all("subdir").await.unwrap();
    bridge
        .write_file("subdir/hello.txt", b"world".to_vec())
        .await
        .unwrap();

    assert!(bridge.exists("subdir/hello.txt").await.unwrap());

    let data = bridge.read_file("subdir/hello.txt").await.unwrap();
    assert_eq!(data, b"world");
}

#[tokio::test]
async fn blocking_bridge_atomic_write_and_list() {
    let tmp = tempfile::tempdir().unwrap();
    let bridge = BlockingBridge::new(FsDirectory::new(tmp.path()).unwrap());

    bridge.create_dir_all("wal").await.unwrap();
    bridge
        .atomic_write("wal/seg1.log", b"data1".to_vec())
        .await
        .unwrap();
    bridge
        .atomic_write("wal/seg2.log", b"data2".to_vec())
        .await
        .unwrap();

    let entries = bridge.list_dir("wal").await.unwrap();
    assert!(entries.contains(&"seg1.log".to_string()));
    assert!(entries.contains(&"seg2.log".to_string()));
}

#[tokio::test]
async fn blocking_bridge_append() {
    let tmp = tempfile::tempdir().unwrap();
    let bridge = BlockingBridge::new(FsDirectory::new(tmp.path()).unwrap());

    bridge
        .write_file("log.bin", b"first".to_vec())
        .await
        .unwrap();
    bridge.append("log.bin", b"second".to_vec()).await.unwrap();

    let data = bridge.read_file("log.bin").await.unwrap();
    assert_eq!(data, b"firstsecond");
}

#[tokio::test]
async fn blocking_bridge_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let bridge = BlockingBridge::new(FsDirectory::new(tmp.path()).unwrap());

    bridge
        .write_file("temp.txt", b"gone".to_vec())
        .await
        .unwrap();
    assert!(bridge.exists("temp.txt").await.unwrap());

    bridge.delete("temp.txt").await.unwrap();
    assert!(!bridge.exists("temp.txt").await.unwrap());
}

#[tokio::test]
async fn blocking_bridge_file_path() {
    let tmp = tempfile::tempdir().unwrap();
    let bridge = BlockingBridge::new(FsDirectory::new(tmp.path()).unwrap());

    let path = bridge.file_path("some/file.txt");
    assert!(path.is_some());
    assert!(path.unwrap().ends_with("some/file.txt"));
}

#[tokio::test]
async fn blocking_bridge_inner_accessors() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();
    let bridge = BlockingBridge::new(fs);

    // inner() returns a reference
    let _inner: &FsDirectory = bridge.inner();
    // inner_arc() returns a clone of the Arc
    let arc = bridge.inner_arc();
    // file_path is on Directory trait, call via deref
    use durability::storage::Directory;
    assert!(arc.file_path("test").is_some());
}

#[tokio::test]
async fn blocking_bridge_from_arc() {
    let tmp = tempfile::tempdir().unwrap();
    let arc = std::sync::Arc::new(FsDirectory::new(tmp.path()).unwrap());
    let bridge = BlockingBridge::from_arc(arc);

    bridge.write_file("test.txt", b"ok".to_vec()).await.unwrap();
    let data = bridge.read_file("test.txt").await.unwrap();
    assert_eq!(data, b"ok");
}
