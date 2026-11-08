use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lsmkv::memtable::{InternalKey, Kind};
use lsmkv::sstable::{BlockCache, SstableReader, SstableWriter};
use lsmkv::{Db, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("bloom-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

#[test]
fn negative_filter_lookups_update_shared_db_stats() {
    let dir = TestDir::new();
    let options = Options { block_cache_capacity: 0, ..Options::default() };
    let db = Db::open(&dir.0, options.clone()).unwrap();
    let path = dir.0.join("table.sst");
    let mut writer = SstableWriter::with_options(Vec::new(), &options).unwrap();
    let latest = InternalKey::new(b"a".to_vec(), 2, Kind::Delete);
    let older = InternalKey::new(b"a".to_vec(), 1, Kind::Put);
    let last = InternalKey::new(b"z".to_vec(), 1, Kind::Put);
    writer.add(&latest, &[]).unwrap();
    writer.add(&older, b"old").unwrap();
    writer.add(&last, b"last").unwrap();
    fs::write(&path, writer.finish().unwrap()).unwrap();
    let cache = Arc::new(BlockCache::new(&options));
    let reader = SstableReader::open(path, cache).unwrap();
    assert_eq!(reader.get(&latest).unwrap(), Some(Vec::new()));
    assert_eq!(reader.get(&older).unwrap(), Some(b"old".to_vec()));
    assert_eq!(reader.get(&last).unwrap(), Some(b"last".to_vec()));
    assert_eq!(db.stats().bloom_filter_block_reads_avoided, 0);
    for n in 0..100 {
        let missing = InternalKey::new(format!("m-{n:03}").into_bytes(), 1, Kind::Put);
        assert_eq!(reader.get(&missing).unwrap(), None);
    }
    assert!(db.stats().bloom_filter_block_reads_avoided > 90);
}

#[test]
fn zero_bits_produces_compatible_empty_filter_and_invalid_configuration_fails() {
    let dir = TestDir::new();
    let options = Options { bloom_bits_per_key: 0, ..Options::default() };
    let db = Db::open(&dir.0, options.clone()).unwrap();
    let path = dir.0.join("legacy.sst");
    let mut writer = SstableWriter::with_options(Vec::new(), &options).unwrap();
    let key = InternalKey::new(b"z".to_vec(), 1, Kind::Put);
    writer.add(&key, b"value").unwrap();
    fs::write(&path, writer.finish().unwrap()).unwrap();
    let reader = SstableReader::open(path, Arc::new(BlockCache::new(&options))).unwrap();
    assert_eq!(reader.get(&key).unwrap(), Some(b"value".to_vec()));
    assert_eq!(reader.get(&InternalKey::new(b"a".to_vec(), 1, Kind::Put)).unwrap(), None);
    assert_eq!(db.stats().bloom_filter_block_reads_avoided, 0);
    let invalid = Options { bloom_bits_per_key: 31, ..Options::default() };
    assert!(SstableWriter::with_options(Vec::new(), &invalid).is_err());
}

#[test]
fn a_cached_candidate_does_not_count_as_an_avoided_disk_read() {
    let dir = TestDir::new();
    let options = Options::default();
    let db = Db::open(&dir.0, options.clone()).unwrap();
    let path = dir.0.join("table.sst");
    let key = InternalKey::new(b"z".to_vec(), 1, Kind::Put);
    let mut writer = SstableWriter::with_options(Vec::new(), &options).unwrap();
    writer.add(&key, b"value").unwrap();
    fs::write(&path, writer.finish().unwrap()).unwrap();
    let reader = SstableReader::open(path, Arc::new(BlockCache::new(&options))).unwrap();
    assert_eq!(reader.get(&key).unwrap(), Some(b"value".to_vec()));
    for n in 0..100 {
        let missing = InternalKey::new(format!("m-{n:03}").into_bytes(), 1, Kind::Put);
        assert_eq!(reader.get(&missing).unwrap(), None);
    }
    assert_eq!(db.stats().bloom_filter_block_reads_avoided, 0);
}
