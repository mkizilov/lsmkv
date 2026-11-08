use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use lsmkv::{Db, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("snapshot-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

#[test]
fn snapshot_remains_consistent_across_writes_and_flushes() {
    let dir = TestDir::new();
    let options = Options { memtable_size: 1, ..Options::default() };
    let db = Db::open(&dir.0, options).unwrap();
    db.put(b"a", b"old-a").unwrap();
    db.put(b"b", b"old-b").unwrap();
    let snapshot = db.snapshot().unwrap();
    db.put(b"a", b"new-a").unwrap();
    db.delete(b"b").unwrap();
    db.put(b"c", b"new-c").unwrap();
    db.sync().unwrap();
    assert_eq!(snapshot.get(b"a").unwrap(), Some(b"old-a".to_vec()));
    assert_eq!(snapshot.get(b"b").unwrap(), Some(b"old-b".to_vec()));
    assert_eq!(snapshot.get(b"c").unwrap(), None);
    assert_eq!(snapshot.scan(..).unwrap(), vec![(b"a".to_vec(), b"old-a".to_vec()), (b"b".to_vec(), b"old-b".to_vec())]);
    assert_eq!(snapshot.scan(b"b".to_vec()..b"c".to_vec()).unwrap(), vec![(b"b".to_vec(), b"old-b".to_vec())]);
    assert_eq!(db.scan(..).unwrap(), vec![(b"a".to_vec(), b"new-a".to_vec()), (b"c".to_vec(), b"new-c".to_vec())]);
}

#[test]
fn snapshot_taken_before_any_writes_is_empty() {
    let dir = TestDir::new();
    let db = Db::open(&dir.0, Options::default()).unwrap();
    let snapshot = db.snapshot().unwrap();
    db.put(b"key", b"later").unwrap();
    assert_eq!(snapshot.get(b"key").unwrap(), None);
    assert!(snapshot.scan(..).unwrap().is_empty());
}

#[test]
fn snapshot_stays_stable_during_concurrent_writes() {
    let dir = TestDir::new();
    let db = Db::open(&dir.0, Options { memtable_size: 256, ..Options::default() }).unwrap();
    db.put(b"key", b"original").unwrap();
    let snapshot = db.snapshot().unwrap();
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            for i in 0..30 { db.put(b"key", format!("new-{i}").into_bytes()).unwrap(); }
        });
        for _ in 0..30 {
            assert_eq!(snapshot.get(b"key").unwrap(), Some(b"original".to_vec()));
        }
        writer.join().unwrap();
    });
    db.sync().unwrap();
    assert_eq!(snapshot.get(b"key").unwrap(), Some(b"original".to_vec()));
    assert_eq!(db.get(b"key").unwrap(), Some(b"new-29".to_vec()));
}
