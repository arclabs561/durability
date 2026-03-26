//! Tests for FsDirectory path traversal rejection.
//!
//! Validates that `..`, absolute paths, and prefix components are rejected
//! by all FsDirectory operations (F1 security fix).

use durability::error::PersistenceError;
use durability::storage::{Directory, FsDirectory};
use std::sync::Arc;

fn make_dir() -> (tempfile::TempDir, Arc<dyn Directory>) {
    let tmp = tempfile::tempdir().unwrap();
    let dir: Arc<dyn Directory> = Arc::new(FsDirectory::new(tmp.path()).unwrap());
    (tmp, dir)
}

fn assert_invalid_config(result: Result<impl Sized, PersistenceError>) {
    match result {
        Err(PersistenceError::InvalidConfig(msg)) => {
            assert!(msg.contains("..") || msg.contains("absolute") || msg.contains("prefix"));
        }
        Err(other) => panic!("expected InvalidConfig, got: {other}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn rejects_parent_dir_traversal_in_create_file() {
    let (_tmp, dir) = make_dir();
    assert_invalid_config(dir.create_file("../escape.txt").map(|_| ()));
}

#[test]
fn rejects_parent_dir_traversal_in_open_file() {
    let (_tmp, dir) = make_dir();
    assert_invalid_config(dir.open_file("subdir/../../escape.txt").map(|_| ()));
}

#[test]
fn rejects_absolute_path_in_create_file() {
    let (_tmp, dir) = make_dir();
    assert_invalid_config(dir.create_file("/etc/passwd").map(|_| ()));
}

#[test]
fn rejects_parent_traversal_in_exists() {
    let (_tmp, dir) = make_dir();
    // exists returns false for invalid paths (doesn't error).
    assert!(!dir.exists("../Cargo.toml"));
}

#[test]
fn rejects_parent_traversal_in_delete() {
    let (_tmp, dir) = make_dir();
    assert_invalid_config(dir.delete("../something"));
}

#[test]
fn rejects_parent_traversal_in_atomic_rename() {
    let (_tmp, dir) = make_dir();
    dir.atomic_write("source.txt", b"data").unwrap();
    assert_invalid_config(dir.atomic_rename("source.txt", "../escaped.txt"));
}

#[test]
fn rejects_parent_traversal_in_atomic_write() {
    let (_tmp, dir) = make_dir();
    assert_invalid_config(dir.atomic_write("../escape.bin", b"bad"));
}

#[test]
fn rejects_parent_traversal_in_list_dir() {
    let (_tmp, dir) = make_dir();
    assert_invalid_config(dir.list_dir("../"));
}

#[test]
fn rejects_parent_traversal_in_file_path() {
    let (_tmp, dir) = make_dir();
    assert!(dir.file_path("../escape").is_none());
}

#[test]
fn normal_paths_still_work() {
    let (_tmp, dir) = make_dir();

    dir.create_dir_all("a/b/c").unwrap();
    dir.atomic_write("a/b/c/file.txt", b"hello").unwrap();
    assert!(dir.exists("a/b/c/file.txt"));

    let mut content = Vec::new();
    use std::io::Read;
    dir.open_file("a/b/c/file.txt")
        .unwrap()
        .read_to_end(&mut content)
        .unwrap();
    assert_eq!(content, b"hello");

    let entries = dir.list_dir("a/b").unwrap();
    assert_eq!(entries, vec!["c"]);

    assert!(dir.file_path("a/b/c/file.txt").is_some());
}

#[test]
fn dot_component_is_allowed() {
    let (_tmp, dir) = make_dir();
    dir.create_dir_all("./subdir").unwrap();
    dir.atomic_write("./subdir/ok.txt", b"fine").unwrap();
    assert!(dir.exists("./subdir/ok.txt"));
}
