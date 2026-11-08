use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use lsmkv::{Db, Error, Options, WriteBatch};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "db-{}-{}",
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

#[test]
fn writes_survive_reopen_and_sequence_continues() {
    let dir = TestDir::new();
    {
        let db = Db::open(&dir.0, Options::default()).unwrap();
        db.put(b"a", b"old").unwrap();
        db.put(b"b", b"gone").unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"new").delete(b"b").put(b"c", b"three");
        db.write(batch).unwrap();
        db.sync().unwrap();
    }
    {
        let db = Db::open(&dir.0, Options::default()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), None);
        assert_eq!(
            db.scan(..).unwrap(),
            vec![
                (b"a".to_vec(), b"new".to_vec()),
                (b"c".to_vec(), b"three".to_vec())
            ]
        );
        db.put(b"a", b"latest").unwrap();
        db.delete(b"c").unwrap();
    }
    let db = Db::open(&dir.0, Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"latest".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), None);
}

#[test]
fn torn_batch_is_never_partially_replayed() {
    let dir = TestDir::new();
    let wal = dir.0.join("WAL");
    let db = Db::open(&dir.0, Options::default()).unwrap();
    db.put(b"before", b"present").unwrap();
    let prefix = fs::metadata(&wal).unwrap().len() as usize;
    let mut batch = WriteBatch::new();
    batch.put(b"one", b"1").put(b"two", b"2").delete(b"before");
    db.write(batch).unwrap();
    drop(db);
    let full = fs::read(&wal).unwrap();
    for end in prefix..full.len() {
        fs::write(&wal, &full[..end]).unwrap();
        let db = Db::open(&dir.0, Options::default()).unwrap();
        assert_eq!(db.get(b"before").unwrap(), Some(b"present".to_vec()));
        assert_eq!(db.get(b"one").unwrap(), None);
        assert_eq!(db.get(b"two").unwrap(), None);
        drop(db);
        assert_eq!(fs::metadata(&wal).unwrap().len(), prefix as u64);
    }
    fs::write(&wal, &full).unwrap();
    let db = Db::open(&dir.0, Options::default()).unwrap();
    assert_eq!(db.get(b"before").unwrap(), None);
    assert_eq!(db.get(b"one").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"two").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn competing_open_is_rejected_and_lock_is_released() {
    let dir = TestDir::new();
    let db = Db::open(&dir.0, Options::default()).unwrap();
    assert!(matches!(
        Db::open(&dir.0, Options::default()),
        Err(Error::Locked)
    ));
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("lock_child")
        .env("LSMKV_LOCK_TEST_PATH", &dir.0)
        .status()
        .unwrap();
    assert!(status.success());
    drop(db);
    assert!(Db::open(&dir.0, Options::default()).is_ok());
}

#[test]
fn lock_child() {
    if let Some(path) = std::env::var_os("LSMKV_LOCK_TEST_PATH") {
        assert!(matches!(
            Db::open(path, Options::default()),
            Err(Error::Locked)
        ));
    }
}

#[test]
fn rejects_nonmonotonic_wal_sequences() {
    use lsmkv::wal::{Operation, SyncPolicy, WalWriter};
    let dir = TestDir::new();
    let path = dir.0.join("WAL");
    let writer = WalWriter::open(&path, SyncPolicy::EveryWrite).unwrap();
    writer
        .append(&[Operation::Delete {
            sequence: 7,
            key: b"x".to_vec(),
        }])
        .unwrap();
    writer.close().unwrap();
    assert!(matches!(
        Db::open(&dir.0, Options::default()),
        Err(Error::InvalidSequence)
    ));
    assert!(
        OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap()
            .metadata()
            .unwrap()
            .len()
            > 0
    );
}

#[test]
fn checksum_corruption_is_not_silently_truncated() {
    let dir = TestDir::new();
    {
        let db = Db::open(&dir.0, Options::default()).unwrap();
        db.put(b"first", b"value").unwrap();
        db.put(b"second", b"value").unwrap();
    }
    let path = dir.0.join("WAL");
    let mut bytes = fs::read(&path).unwrap();
    bytes[10] ^= 1;
    fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        Db::open(&dir.0, Options::default()),
        Err(Error::Corruption("corrupt WAL record"))
    ));
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn orphan_tables_without_manifest_do_not_create_an_empty_database() {
    let dir = TestDir::new();
    let orphan = dir.0.join("SST-00000000000000000001.sst");
    fs::write(&orphan, b"unknown table").unwrap();
    assert!(matches!(
        Db::open(&dir.0, Options::default()),
        Err(Error::Corruption("SSTables exist without a manifest"))
    ));
    assert_eq!(fs::read(orphan).unwrap(), b"unknown table");
    assert!(!dir.0.join("CURRENT").exists());
}
