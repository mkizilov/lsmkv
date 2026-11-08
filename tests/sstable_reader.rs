use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lsmkv::memtable::{InternalKey, Kind};
use lsmkv::sstable::{BlockCache, SstableReader, SstableWriter};
use lsmkv::{Error, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "sst-{}-{}",
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
fn random_sorted_entries_round_trip_and_seek() {
    let dir = TestDir::new();
    let path = dir.0.join("table");
    let mut seed = 0x1234_5678_9abc_def0u64;
    let mut expected = BTreeMap::new();
    for seq in 0..1500u64 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let key = InternalKey::new((seed % 1000).to_be_bytes().to_vec(), seq, Kind::Put);
        expected.insert(key, vec![seed as u8; (seed % 40) as usize]);
    }
    let mut writer = SstableWriter::new(Vec::new());
    for (key, value) in &expected {
        writer.add(key, value).unwrap();
    }
    fs::write(&path, writer.finish().unwrap()).unwrap();
    let cache = Arc::new(BlockCache::new(&Options::default()));
    let reader = SstableReader::open(&path, cache).unwrap();
    for (key, value) in &expected {
        assert_eq!(reader.get(key).unwrap().as_ref(), Some(value));
    }
    let actual: Vec<_> = reader.iter().map(Result::unwrap).collect();
    let sorted: Vec<_> = expected
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(actual, sorted);
    let mut iter = reader.iter();
    for _ in 0..500 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let target = InternalKey::new((seed % 1100).to_be_bytes().to_vec(), seed, Kind::Put);
        let next = expected
            .range(target.clone()..)
            .next()
            .map(|(k, v)| (k.clone(), v.clone()));
        iter.seek(&target).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), next);
    }
}

#[test]
fn every_single_byte_corruption_is_rejected() {
    let dir = TestDir::new();
    let path = dir.0.join("table");
    let mut writer = SstableWriter::new(Vec::new());
    for i in 0..20u64 {
        let key = InternalKey::new(format!("key-{i:02}").into_bytes(), i, Kind::Put);
        writer.add(&key, &[i as u8; 5]).unwrap();
    }
    let original = writer.finish().unwrap();
    let cache = Arc::new(BlockCache::new(&Options::default()));
    fs::write(&path, &original).unwrap();
    assert!(SstableReader::open(&path, Arc::clone(&cache)).is_ok());
    for offset in 0..original.len() {
        let mut damaged = original.clone();
        damaged[offset] ^= 1;
        fs::write(&path, damaged).unwrap();
        assert!(
            matches!(
                SstableReader::open(&path, Arc::clone(&cache)),
                Err(Error::Corruption(_))
            ),
            "byte {offset}"
        );
    }
    for end in 0..original.len() {
        fs::write(&path, &original[..end]).unwrap();
        assert!(
            matches!(
                SstableReader::open(&path, Arc::clone(&cache)),
                Err(Error::Corruption(_))
            ),
            "truncation {end}"
        );
    }
}

#[test]
fn shared_cache_does_not_mix_tables_and_empty_tables_work() {
    let dir = TestDir::new();
    let first = dir.0.join("first");
    let second = dir.0.join("second");
    let empty = dir.0.join("empty");
    let key = InternalKey::new(b"same".to_vec(), 1, Kind::Put);
    for (path, value) in [(&first, b"one"), (&second, b"two")] {
        let mut writer = SstableWriter::new(Vec::new());
        writer.add(&key, value).unwrap();
        fs::write(path, writer.finish().unwrap()).unwrap();
    }
    fs::write(&empty, SstableWriter::new(Vec::new()).finish().unwrap()).unwrap();
    let cache = Arc::new(BlockCache::new(&Options::default()));
    let a = SstableReader::open(first, Arc::clone(&cache)).unwrap();
    let b = SstableReader::open(second, Arc::clone(&cache)).unwrap();
    let c = SstableReader::open(empty, cache).unwrap();
    assert_eq!(a.get(&key).unwrap(), Some(b"one".to_vec()));
    assert_eq!(b.get(&key).unwrap(), Some(b"two".to_vec()));
    assert_eq!(a.get(&key).unwrap(), Some(b"one".to_vec()));
    assert_eq!(c.get(&key).unwrap(), None);
    assert!(c.iter().next().is_none());
}
