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
        let path = root.join(format!("stats-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

#[test]
fn stats_report_live_files_and_cache_activity() {
    let dir = TestDir::new();
    let db = Db::open(&dir.0, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    let stats = db.stats();
    assert_eq!(stats.files_per_level.get(&0), Some(&1));
    assert!(stats.live_bytes_per_level.get(&0).copied().unwrap_or(0) > 0);
    assert!(stats.cache_hits > 0);
    assert!(stats.cache_misses > 0);
    assert!(stats.cache_hit_rate > 0.0 && stats.cache_hit_rate < 1.0);
    assert!(stats.write_amplification > 0.0);
}
