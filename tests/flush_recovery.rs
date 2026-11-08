use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use lsmkv::memtable::{InternalKey, Kind};
use lsmkv::sstable::SstableWriter;
use lsmkv::{Db, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("flush-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

#[test]
fn several_flushes_preserve_overwrites_deletes_and_recovery() {
    let dir = TestDir::new();
    let options = Options { memtable_size: 320, ..Options::default() };
    let mut expected = BTreeMap::new();
    {
        let db = Db::open(&dir.0, options.clone()).unwrap();
        for i in 0..200u32 {
            let key = format!("key-{:02}", i % 37).into_bytes();
            if i % 9 == 0 {
                db.delete(&key).unwrap();
                expected.remove(&key);
            } else {
                let value = format!("value-{i}").into_bytes();
                db.put(&key, &value).unwrap();
                expected.insert(key, value);
            }
            for (key, value) in &expected {
                assert_eq!(db.get(key).unwrap().as_ref(), Some(value));
            }
        }
        db.sync().unwrap();
        assert_eq!(db.scan(..).unwrap(), expected.clone().into_iter().collect::<Vec<_>>());
        let tables = fs::read_dir(&dir.0).unwrap().filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".sst")).count();
        assert!(tables >= 2);
    }
    let db = Db::open(&dir.0, options).unwrap();
    for (key, value) in &expected {
        assert_eq!(db.get(key).unwrap().as_ref(), Some(value));
    }
    assert_eq!(db.scan(..).unwrap(), expected.into_iter().collect::<Vec<_>>());
}

#[test]
fn orphan_sstable_after_crash_is_ignored_and_wals_replayed_once() {
    let dir = TestDir::new();
    {
        let db = Db::open(&dir.0, Options::default()).unwrap();
        db.put(b"a", b"old").unwrap();
        db.put(b"b", b"value").unwrap();
        db.put(b"a", b"new").unwrap();
        db.sync().unwrap();
    }
    // Reproduce the on-disk state after rotation and SST publication, before
    // the manifest edit: the old WAL still exists and the new WAL is empty.
    fs::write(dir.0.join("WAL-00000000000000000001"), b"").unwrap();
    let mut writer = SstableWriter::new(Vec::new());
    writer.add(&InternalKey::new(b"a".to_vec(), 3, Kind::Put), b"new").unwrap();
    writer.add(&InternalKey::new(b"a".to_vec(), 1, Kind::Put), b"old").unwrap();
    writer.add(&InternalKey::new(b"b".to_vec(), 2, Kind::Put), b"value").unwrap();
    fs::write(dir.0.join("SST-00000000000000000002.sst"), writer.finish().unwrap()).unwrap();
    let options = Options { memtable_size: 1, ..Options::default() };
    {
        let db = Db::open(&dir.0, options.clone()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"value".to_vec()));
        assert_eq!(db.scan(..).unwrap().len(), 2);
        db.put(b"c", b"next").unwrap();
        db.sync().unwrap();
    }
    let db = Db::open(&dir.0, options).unwrap();
    assert_eq!(db.scan(..).unwrap(), vec![
        (b"a".to_vec(), b"new".to_vec()),
        (b"b".to_vec(), b"value".to_vec()),
        (b"c".to_vec(), b"next".to_vec()),
    ]);
}

#[test]
fn stale_wal_after_manifest_publication_is_not_replayed_twice() {
    let dir = TestDir::new();
    let initial = Options { memtable_size: 400, ..Options::default() };
    let db = Db::open(&dir.0, initial.clone()).unwrap();
    db.put(b"a", b"original").unwrap();
    let stale = fs::read(dir.0.join("WAL")).unwrap();
    db.put(b"b", vec![1; 600]).unwrap();
    db.sync().unwrap();
    drop(db);
    // Simulate a crash after the manifest sync but before unlinking the old WAL.
    fs::write(dir.0.join("WAL"), stale).unwrap();
    let db = Db::open(&dir.0, initial).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"original".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(vec![1; 600]));
    db.put(b"a", b"new").unwrap();
    assert_eq!(db.scan(..).unwrap().len(), 2);
    db.sync().unwrap();
    drop(db);
    let db = Db::open(&dir.0, Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
}
