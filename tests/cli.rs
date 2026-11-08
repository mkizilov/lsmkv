use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use lsmkv::{Db, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "cli-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn cli(path: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lsmkv-cli"));
    command.arg(args[0]).arg(path).args(&args[1..]);
    command.output().unwrap()
}

#[test]
fn cli_put_get_scan_stats_compact_verify_delete() {
    let dir = TestDir::new();
    assert!(cli(&dir.0, &["put", "alpha", "one"]).status.success());
    assert!(cli(&dir.0, &["put", "beta", "two"]).status.success());
    let get = cli(&dir.0, &["get", "alpha"]);
    assert!(get.status.success());
    assert_eq!(get.stdout, b"one\n");
    let scan = cli(&dir.0, &["scan", "alpha", "beta"]);
    assert!(scan.status.success());
    assert_eq!(scan.stdout, b"616c706861\t6f6e65\n");
    assert!(cli(&dir.0, &["compact"]).status.success());
    let stats = cli(&dir.0, &["stats"]);
    assert!(stats.status.success());
    let stats = String::from_utf8(stats.stdout).unwrap();
    assert!(stats.contains("level 1:"));
    assert!(stats.contains("cache hit rate:"));
    assert!(stats.contains("write amplification:"));
    assert!(stats.contains("bloom block reads avoided:"));
    assert!(cli(&dir.0, &["verify"]).status.success());
    assert!(cli(&dir.0, &["delete", "alpha"]).status.success());
    assert!(!cli(&dir.0, &["get", "alpha"]).status.success());
}

#[test]
fn verify_detects_corruption_without_recovery_truncation() {
    use std::io::Write;
    let dir = TestDir::new();
    {
        let db = Db::open(&dir.0, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
    }
    let wal = dir.0.join("WAL");
    let mut file = fs::OpenOptions::new().append(true).open(&wal).unwrap();
    file.write_all(&[1, 2, 3]).unwrap();
    drop(file);
    let before = fs::read(&wal).unwrap();
    assert!(!cli(&dir.0, &["verify"]).status.success());
    assert_eq!(fs::read(&wal).unwrap(), before);
}

#[test]
fn verify_detects_sstable_corruption() {
    let dir = TestDir::new();
    {
        let db = Db::open(&dir.0, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }
    let table = fs::read_dir(&dir.0)
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| entry.file_name().to_string_lossy().ends_with(".sst"))
        .unwrap()
        .path();
    let mut bytes = fs::read(&table).unwrap();
    bytes[0] ^= 1;
    fs::write(&table, bytes).unwrap();
    assert!(!cli(&dir.0, &["verify"]).status.success());
}

#[test]
fn verify_detects_manifest_checksum_corruption() {
    let dir = TestDir::new();
    drop(Db::open(&dir.0, Options::default()).unwrap());
    let manifest = dir.0.join("MANIFEST-000001");
    let mut bytes = fs::read(&manifest).unwrap();
    bytes[12] ^= 1;
    fs::write(&manifest, &bytes).unwrap();
    assert!(!cli(&dir.0, &["verify"]).status.success());
    assert_eq!(fs::read(manifest).unwrap(), bytes);
}

#[test]
fn read_command_does_not_create_a_missing_database() {
    let dir = TestDir::new();
    let missing = dir.0.join("missing");
    assert!(!cli(&missing, &["get", "key"]).status.success());
    assert!(!missing.exists());
}
