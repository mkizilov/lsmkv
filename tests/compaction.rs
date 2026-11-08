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
        let path = root.join(format!("compact-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

#[test]
fn compaction_preserves_live_snapshots_and_reopen() {
    let dir = TestDir::new();
    let options = Options { memtable_size: 1, l0_compaction_trigger: 1, l0_stop_writes_trigger: 2, target_file_size: 256, ..Options::default() };
    let db = Db::open(&dir.0, options.clone()).unwrap();
    db.put(b"a", b"old").unwrap();
    db.put(b"b", b"old-b").unwrap();
    let snapshot = db.snapshot().unwrap();
    db.put(b"a", b"new").unwrap();
    db.delete(b"b").unwrap();
    for i in 0..30 { db.put(format!("key-{i:02}").into_bytes(), vec![i as u8; 40]).unwrap(); }
    db.sync().unwrap();
    assert_eq!(snapshot.get(b"a").unwrap(), Some(b"old".to_vec()));
    assert_eq!(snapshot.get(b"b").unwrap(), Some(b"old-b".to_vec()));
    assert_eq!(snapshot.scan(..).unwrap(), vec![(b"a".to_vec(), b"old".to_vec()), (b"b".to_vec(), b"old-b".to_vec())]);
    assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
    let stats = db.stats();
    assert!(stats.bytes_written_per_level.get(&0).copied().unwrap_or(0) > 0);
    assert!(stats.bytes_written_per_level.get(&1).copied().unwrap_or(0) > 0);
    assert!(stats.write_amplification > 0.0);
    drop(snapshot);
    drop(db);
    let db = Db::open(&dir.0, options).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
    for i in 0..30 { assert_eq!(db.get(format!("key-{i:02}").as_bytes()).unwrap(), Some(vec![i as u8; 40])); }
}
