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
        let path = root.join(format!(
            "levels-{}-{}",
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

fn write_table(dir: &TestDir, number: u64, entries: Vec<(InternalKey, Vec<u8>)>) {
    let file = File::create(crate::flush::table_path(&dir.0, number)).unwrap();
    let mut writer = SstableWriter::new(file);
    for (key, value) in entries {
        writer.add(&key, &value).unwrap();
    }
    writer.finish().unwrap().sync_all().unwrap();
}

fn key(user: &[u8], sequence: u64, kind: Kind) -> InternalKey {
    InternalKey::new(user.to_vec(), sequence, kind)
}

#[test]
fn reads_and_merges_versions_across_levels() {
    let dir = TestDir::new();
    drop(Db::open(&dir.0, Options::default()).unwrap());
    write_table(
        &dir,
        1,
        vec![
            (key(b"a", 1, Kind::Put), b"deep-a".to_vec()),
            (key(b"b", 2, Kind::Put), b"deep-b".to_vec()),
        ],
    );
    write_table(
        &dir,
        2,
        vec![
            (key(b"x", 3, Kind::Put), b"deep-x".to_vec()),
            (key(b"z", 4, Kind::Put), b"deep-z".to_vec()),
        ],
    );
    write_table(
        &dir,
        3,
        vec![
            (key(b"a", 5, Kind::Put), b"middle-a".to_vec()),
            (key(b"b", 6, Kind::Delete), Vec::new()),
        ],
    );
    write_table(
        &dir,
        4,
        vec![
            (key(b"a", 7, Kind::Put), b"new-a".to_vec()),
            (key(b"c", 8, Kind::Put), b"new-c".to_vec()),
            (key(b"x", 9, Kind::Delete), Vec::new()),
        ],
    );
    manifest::append(
        &dir.0,
        &Edit {
            added: vec![(2, 2), (2, 1), (1, 3), (0, 4)],
            removed: Vec::new(),
            next_file: 5,
            last_sequence: 9,
        },
        &crate::fs::NativeFs,
    )
    .unwrap();
    let db = Db::open(&dir.0, Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"new-a".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"x").unwrap(), None);
    assert_eq!(db.get(b"z").unwrap(), Some(b"deep-z".to_vec()));
    assert_eq!(db.get_at(b"a", 5).unwrap(), Some(b"middle-a".to_vec()));
    assert_eq!(db.get_at(b"a", 4).unwrap(), Some(b"deep-a".to_vec()));
    assert_eq!(db.get_at(b"b", 5).unwrap(), Some(b"deep-b".to_vec()));
    assert_eq!(db.get_at(b"x", 8).unwrap(), Some(b"deep-x".to_vec()));
    assert_eq!(
        db.scan_at(.., 5).unwrap(),
        vec![
            (b"a".to_vec(), b"middle-a".to_vec()),
            (b"b".to_vec(), b"deep-b".to_vec()),
            (b"x".to_vec(), b"deep-x".to_vec()),
            (b"z".to_vec(), b"deep-z".to_vec()),
        ]
    );
    assert_eq!(
        db.scan(..).unwrap(),
        vec![
            (b"a".to_vec(), b"new-a".to_vec()),
            (b"c".to_vec(), b"new-c".to_vec()),
            (b"z".to_vec(), b"deep-z".to_vec()),
        ]
    );
}

#[test]
fn overlapping_deeper_level_ranges_are_rejected() {
    let dir = TestDir::new();
    drop(Db::open(&dir.0, Options::default()).unwrap());
    write_table(
        &dir,
        1,
        vec![
            (key(b"a", 1, Kind::Put), b"a".to_vec()),
            (key(b"c", 2, Kind::Put), b"c".to_vec()),
        ],
    );
    write_table(&dir, 2, vec![(key(b"b", 3, Kind::Put), b"b".to_vec())]);
    manifest::append(
        &dir.0,
        &Edit {
            added: vec![(1, 1), (1, 2)],
            removed: Vec::new(),
            next_file: 3,
            last_sequence: 3,
        },
        &crate::fs::NativeFs,
    )
    .unwrap();
    assert!(matches!(
        Db::open(&dir.0, Options::default()),
        Err(Error::Corruption("overlapping table ranges"))
    ));
}

#[test]
fn obsolete_files_wait_for_reference_counted_versions() {
    use std::sync::Arc;
    let dir = TestDir::new();
    let options = Options {
        memtable_size: 1,
        l0_compaction_trigger: 2,
        l0_stop_writes_trigger: 3,
        ..Options::default()
    };
    let db = Db::open(&dir.0, options).unwrap();
    db.put(b"a", b"one").unwrap();
    db.put(b"b", b"two").unwrap();
    db.sync().unwrap();
    let pinned = Arc::clone(&db.state().unwrap().tables);
    let paths: Vec<_> = pinned
        .get(&0)
        .unwrap()
        .iter()
        .map(|table| table.path.clone())
        .collect();
    assert_eq!(paths.len(), 2);
    db.put(b"c", b"three").unwrap();
    db.sync().unwrap();
    assert!(paths.iter().all(|path| path.exists()));
    assert_eq!(db.get(b"a").unwrap(), Some(b"one".to_vec()));
    drop(pinned);
    assert!(paths.iter().all(|path| !path.exists()));
    assert!(
        db.stats()
            .bytes_written_per_level
            .get(&1)
            .copied()
            .unwrap_or(0)
            > 0
    );
    assert!(db.stats().write_amplification > 0.0);
}

#[test]
fn oversized_deeper_levels_are_compacted_and_split() {
    let dir = TestDir::new();
    let options = Options {
        memtable_size: 1,
        l0_compaction_trigger: 1,
        l0_stop_writes_trigger: 2,
        target_file_size: 80,
        ..Options::default()
    };
    let db = Db::open(&dir.0, options).unwrap();
    for i in 0..20 {
        db.put(format!("key-{i:02}").into_bytes(), vec![i as u8; 100])
            .unwrap();
    }
    db.sync().unwrap();
    {
        let state = db.state().unwrap();
        assert!(state
            .tables
            .iter()
            .any(|(&level, files)| level >= 2 && !files.is_empty()));
        assert!(state.tables.values().any(|files| files.len() > 1));
    }
    for i in 0..20 {
        assert_eq!(
            db.get(format!("key-{i:02}").as_bytes()).unwrap(),
            Some(vec![i as u8; 100])
        );
    }
}

#[test]
fn reads_progress_while_sync_waits_for_compaction() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    let dir = TestDir::new();
    let db = Arc::new(Db::open(&dir.0, Options::default()).unwrap());
    db.put(b"key", b"value").unwrap();
    let (release, waiting) = mpsc::channel::<()>();
    let (started, ready) = mpsc::channel::<()>();
    {
        let mut state = db.state().unwrap();
        let notify = state.notify.clone();
        let cv = Arc::clone(&state.maintenance_cv);
        let next_file = state.next_file;
        let handle = std::thread::spawn(move || {
            let _notify = crate::NotifyOnDrop(notify, cv);
            started.send(()).unwrap();
            waiting.recv().unwrap();
            Ok(crate::compaction::Output {
                level: 1,
                files: Vec::new(),
                removed: Vec::new(),
                next_file,
            })
        });
        state.pending = Some(crate::Background::Compaction(handle));
    }
    ready.recv().unwrap();
    let sync_db = Arc::clone(&db);
    let sync = std::thread::spawn(move || sync_db.sync());
    std::thread::sleep(Duration::from_millis(30));
    let (send, received) = mpsc::channel();
    let reader_db = Arc::clone(&db);
    let reader = std::thread::spawn(move || {
        send.send(reader_db.get(b"key")).unwrap();
    });
    let result = received.recv_timeout(Duration::from_secs(2));
    release.send(()).unwrap();
    assert_eq!(result.unwrap().unwrap(), Some(b"value".to_vec()));
    reader.join().unwrap();
    sync.join().unwrap().unwrap();
}
