use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::manifest::{self, Edit};
use crate::memtable::{InternalKey, Kind};
use crate::sstable::SstableWriter;
use crate::{Db, Error, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("levels-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

fn write_table(dir: &TestDir, number: u64, entries: Vec<(InternalKey, Vec<u8>)>) {
    let file = File::create(crate::flush::table_path(&dir.0, number)).unwrap();
    let mut writer = SstableWriter::new(file);
    for (key, value) in entries { writer.add(&key, &value).unwrap(); }
    writer.finish().unwrap().sync_all().unwrap();
}

fn key(user: &[u8], sequence: u64, kind: Kind) -> InternalKey {
    InternalKey::new(user.to_vec(), sequence, kind)
}

#[test]
fn reads_and_merges_versions_across_levels() {
    let dir = TestDir::new();
    drop(Db::open(&dir.0, Options::default()).unwrap());
    write_table(&dir, 1, vec![(key(b"a", 1, Kind::Put), b"deep-a".to_vec()), (key(b"b", 2, Kind::Put), b"deep-b".to_vec())]);
    write_table(&dir, 2, vec![(key(b"x", 3, Kind::Put), b"deep-x".to_vec()), (key(b"z", 4, Kind::Put), b"deep-z".to_vec())]);
    write_table(&dir, 3, vec![(key(b"a", 5, Kind::Put), b"middle-a".to_vec()), (key(b"b", 6, Kind::Delete), Vec::new())]);
    write_table(&dir, 4, vec![(key(b"a", 7, Kind::Put), b"new-a".to_vec()), (key(b"c", 8, Kind::Put), b"new-c".to_vec()), (key(b"x", 9, Kind::Delete), Vec::new())]);
    manifest::append(&dir.0, &Edit { added: vec![(2, 2), (2, 1), (1, 3), (0, 4)], removed: Vec::new(), next_file: 5, last_sequence: 9 }).unwrap();
    let db = Db::open(&dir.0, Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"new-a".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"x").unwrap(), None);
    assert_eq!(db.get(b"z").unwrap(), Some(b"deep-z".to_vec()));
    assert_eq!(db.get_at(b"a", 5).unwrap(), Some(b"middle-a".to_vec()));
    assert_eq!(db.get_at(b"a", 4).unwrap(), Some(b"deep-a".to_vec()));
    assert_eq!(db.get_at(b"b", 5).unwrap(), Some(b"deep-b".to_vec()));
    assert_eq!(db.get_at(b"x", 8).unwrap(), Some(b"deep-x".to_vec()));
    assert_eq!(db.scan_at(.., 5).unwrap(), vec![
        (b"a".to_vec(), b"middle-a".to_vec()),
        (b"b".to_vec(), b"deep-b".to_vec()),
        (b"x".to_vec(), b"deep-x".to_vec()),
        (b"z".to_vec(), b"deep-z".to_vec()),
    ]);
    assert_eq!(db.scan(..).unwrap(), vec![
        (b"a".to_vec(), b"new-a".to_vec()),
        (b"c".to_vec(), b"new-c".to_vec()),
        (b"z".to_vec(), b"deep-z".to_vec()),
    ]);
}

#[test]
fn overlapping_deeper_level_ranges_are_rejected() {
    let dir = TestDir::new();
    drop(Db::open(&dir.0, Options::default()).unwrap());
    write_table(&dir, 1, vec![(key(b"a", 1, Kind::Put), b"a".to_vec()), (key(b"c", 2, Kind::Put), b"c".to_vec())]);
    write_table(&dir, 2, vec![(key(b"b", 3, Kind::Put), b"b".to_vec())]);
    manifest::append(&dir.0, &Edit { added: vec![(1, 1), (1, 2)], removed: Vec::new(), next_file: 3, last_sequence: 3 }).unwrap();
    assert!(matches!(Db::open(&dir.0, Options::default()), Err(Error::Corruption("overlapping table ranges"))));
}
