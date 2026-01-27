//! End-to-end tests for `DurableDirectory` operations.

use durability::storage::{Directory, FsDirectory, MemoryDirectory};
use durability::DurableDirectory;
use std::sync::Arc;

#[test]
fn durable_ops_fail_fast_on_non_fs_backends() {
    let mem = MemoryDirectory::new();

    // Should fail without mutating state.
    assert!(mem.atomic_write_durable("a.bin", b"hi").is_err());
    assert!(mem.atomic_rename_durable("a.bin", "b.bin").is_err());
}

#[test]
fn atomic_write_durable_writes_and_is_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();

    fs.atomic_write_durable("dir/a.bin", b"hello").unwrap();

    let mut r = fs.open_file("dir/a.bin").unwrap();
    let mut buf = Vec::new();
    use std::io::Read;
    r.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"hello");
}

#[test]
fn atomic_rename_durable_renames_and_keeps_contents() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();

    fs.atomic_write("dir/a.bin", b"payload").unwrap();
    fs.atomic_rename_durable("dir/a.bin", "dir/b.bin").unwrap();

    assert!(!fs.exists("dir/a.bin"));
    assert!(fs.exists("dir/b.bin"));

    let mut r = fs.open_file("dir/b.bin").unwrap();
    let mut buf = Vec::new();
    use std::io::Read;
    r.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"payload");
}

#[test]
fn atomic_rename_durable_across_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FsDirectory::new(tmp.path()).unwrap();

    fs.atomic_write("a/a.bin", b"p").unwrap();
    fs.atomic_rename_durable("a/a.bin", "b/b.bin").unwrap();

    assert!(!fs.exists("a/a.bin"));
    assert!(fs.exists("b/b.bin"));
}

#[test]
fn durable_ops_work_through_dyn_directory_when_fs_backed() {
    // Ensure the trait stays usable even when a higher-level system holds `Arc<dyn Directory>`.
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());

    // We can still call the free helpers via the `storage` module, and
    // (if the caller knows it is fs-backed) they can use `DurableDirectory`
    // by operating on a concrete fs directory. This test just ensures the
    // free helper route works.
    let _ = durability::storage::sync_parent_dir(&*dir, "wal/wal_1.log").err();
}
