use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::time::{Duration, Instant};

use lsmkv::{Db, Options, WriteBatch};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("concurrency-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:03}").into_bytes()
}

#[test]
fn db_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Db>();
}

#[test]
fn grouped_batches_remain_atomic_for_snapshots() {
    let dir = TestDir::new();
    let db = Arc::new(Db::open(&dir.0, Options::default()).unwrap());
    let barrier = Arc::new(Barrier::new(9));
    let mut writers = Vec::new();
    for owner in 0..8 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            for generation in 0..30 {
                let mut batch = WriteBatch::new();
                let value = format!("{generation}").into_bytes();
                batch.put(format!("{owner}-a").as_bytes(), &value);
                batch.put(format!("{owner}-b").as_bytes(), &value);
                db.write(batch).unwrap();
            }
        }));
    }
    barrier.wait();
    for _ in 0..200 {
        let snapshot = db.snapshot().unwrap();
        for owner in 0..8 {
            assert_eq!(snapshot.get(format!("{owner}-a").as_bytes()).unwrap(), snapshot.get(format!("{owner}-b").as_bytes()).unwrap());
        }
    }
    for writer in writers { writer.join().unwrap(); }
}

#[test]
fn sixteen_thread_group_commit_stress() {
    let dir = TestDir::new();
    let options = Options {
        memtable_size: 2048,
        l0_compaction_trigger: 1,
        l0_stop_writes_trigger: 3,
        target_file_size: 4096,
        ..Options::default()
    };
    let db = Arc::new(Db::open(&dir.0, options).unwrap());
    let model: Arc<Vec<Mutex<Option<Vec<u8>>>>> = Arc::new((0..128).map(|_| Mutex::new(None)).collect());
    let barrier = Arc::new(Barrier::new(17));
    let deadline = Arc::new(OnceLock::new());
    let mut threads = Vec::new();
    for owner in 0..8usize {
        let db = Arc::clone(&db);
        let model = Arc::clone(&model);
        let barrier = Arc::clone(&barrier);
        let deadline = Arc::clone(&deadline);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let deadline = *deadline.get_or_init(|| Instant::now() + Duration::from_secs(10));
            let mut writes = 0u64;
            while Instant::now() < deadline {
                let index = owner * 16 + (writes as usize % 16);
                let mut expected = model[index].lock().unwrap();
                let value = format!("writer-{owner}-version-{writes}-{}", "x".repeat(128)).into_bytes();
                db.put(key(index), &value).unwrap();
                *expected = Some(value);
                writes += 1;
            }
            writes
        }));
    }
    for reader in 0..8usize {
        let db = Arc::clone(&db);
        let model = Arc::clone(&model);
        let barrier = Arc::clone(&barrier);
        let deadline = Arc::clone(&deadline);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let deadline = *deadline.get_or_init(|| Instant::now() + Duration::from_secs(10));
            let mut reads = 0u64;
            while Instant::now() < deadline {
                let index = ((reads as usize).wrapping_mul(37) + reader * 13) % 128;
                let expected = model[index].lock().unwrap();
                assert_eq!(db.get(key(index)).unwrap(), *expected);
                reads += 1;
            }
            reads
        }));
    }
    barrier.wait();
    let counts: Vec<u64> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();
    assert!(counts.iter().all(|count| *count > 0));
    db.sync().unwrap();
    let stats = db.stats();
    let writes: u64 = counts[..8].iter().sum();
    assert_eq!(stats.committed_write_requests, writes);
    assert!(stats.wal_group_commits < writes, "writers never shared a WAL commit");
    assert!(stats.bytes_written_per_level.get(&0).copied().unwrap_or(0) > 0, "no flush occurred");
    assert!(stats.bytes_written_per_level.get(&1).copied().unwrap_or(0) > 0, "no compaction occurred");
    let expected: Vec<_> = model.iter().enumerate().filter_map(|(index, value)| {
        value.lock().unwrap().clone().map(|value| (key(index), value))
    }).collect();
    assert_eq!(db.scan(..).unwrap(), expected);
}
