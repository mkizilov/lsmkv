mod common;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use common::ModelDb;
use lsmkv::{Db, Options, WriteBatch};
use proptest::prelude::*;
use proptest::test_runner::{FileFailurePersistence, TestCaseResult};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("property-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}

#[derive(Clone, Debug)]
enum Mutation {
    Put(u8, Vec<u8>),
    Delete(u8),
}

#[derive(Clone, Debug)]
enum Op {
    Put(u8, Vec<u8>),
    Delete(u8),
    Get(u8),
    Scan(u8, u8),
    Batch(Vec<Mutation>),
    Snapshot,
    SnapshotGet(u8, u8),
    SnapshotScan(u8, u8, u8),
    Flush,
    Compact,
    Reopen,
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        3 => (0u8..8, prop::collection::vec(any::<u8>(), 0..32)).prop_map(|(key, value)| Mutation::Put(key, value)),
        2 => (0u8..8).prop_map(Mutation::Delete),
    ]
}

fn operation() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0u8..8, prop::collection::vec(any::<u8>(), 0..32)).prop_map(|(key, value)| Op::Put(key, value)),
        3 => (0u8..8).prop_map(Op::Delete),
        3 => (0u8..8).prop_map(Op::Get),
        2 => (0u8..9, 0u8..9).prop_map(|(start, end)| Op::Scan(start, end)),
        3 => prop::collection::vec(mutation(), 0..6).prop_map(Op::Batch),
        1 => Just(Op::Snapshot),
        2 => (any::<u8>(), 0u8..8).prop_map(|(snapshot, key)| Op::SnapshotGet(snapshot, key)),
        1 => (any::<u8>(), 0u8..9, 0u8..9).prop_map(|(snapshot, start, end)| Op::SnapshotScan(snapshot, start, end)),
        1 => Just(Op::Flush),
        1 => Just(Op::Compact),
        1 => Just(Op::Reopen),
    ]
}

fn key(value: u8) -> Vec<u8> { vec![value] }

fn bounds(start: u8, end: u8) -> std::ops::Range<Vec<u8>> {
    key(start.min(end))..key(start.max(end))
}

fn run_epoch(db: &Db, model: &mut ModelDb, operations: &[Op]) -> TestCaseResult {
    let mut snapshots = Vec::new();
    for operation in operations {
        match operation {
            Op::Put(k, value) => {
                db.put(key(*k), value).unwrap();
                model.put(key(*k), value).unwrap();
            }
            Op::Delete(k) => {
                db.delete(key(*k)).unwrap();
                model.delete(key(*k)).unwrap();
            }
            Op::Get(k) => prop_assert_eq!(db.get(key(*k)).unwrap(), model.get(key(*k)).unwrap(), "operation {:?}", operation),
            Op::Scan(start, end) => prop_assert_eq!(db.scan(bounds(*start, *end)).unwrap(), model.scan(bounds(*start, *end)).unwrap(), "operation {:?}", operation),
            Op::Batch(mutations) => {
                let mut batch = WriteBatch::new();
                for mutation in mutations {
                    match mutation {
                        Mutation::Put(k, value) => { batch.put(key(*k), value); }
                        Mutation::Delete(k) => { batch.delete(key(*k)); }
                    }
                }
                db.write(batch).unwrap();
                for mutation in mutations {
                    match mutation {
                        Mutation::Put(k, value) => model.put(key(*k), value).unwrap(),
                        Mutation::Delete(k) => model.delete(key(*k)).unwrap(),
                    }
                }
            }
            Op::Snapshot => {
                if snapshots.len() == 8 { snapshots.remove(0); }
                snapshots.push((db.snapshot().unwrap(), model.clone()));
            }
            Op::SnapshotGet(index, k) => {
                if !snapshots.is_empty() {
                    let (snapshot, reference): &(_, ModelDb) = &snapshots[*index as usize % snapshots.len()];
                    prop_assert_eq!(snapshot.get(key(*k)).unwrap(), reference.get(key(*k)).unwrap(), "operation {:?}", operation);
                }
            }
            Op::SnapshotScan(index, start, end) => {
                if !snapshots.is_empty() {
                    let (snapshot, reference): &(_, ModelDb) = &snapshots[*index as usize % snapshots.len()];
                    prop_assert_eq!(snapshot.scan(bounds(*start, *end)).unwrap(), reference.scan(bounds(*start, *end)).unwrap(), "operation {:?}", operation);
                }
            }
            Op::Flush => db.flush().unwrap(),
            Op::Compact => db.compact().unwrap(),
            Op::Reopen => unreachable!("reopen splits epochs"),
        }
        prop_assert_eq!(db.scan(..).unwrap(), model.scan(..).unwrap(), "operation {:?}", operation);
        for k in 0..8 { prop_assert_eq!(db.get(key(k)).unwrap(), model.get(key(k)).unwrap(), "key {} after {:?}", k, operation); }
        for (snapshot, reference) in &snapshots {
            prop_assert_eq!(snapshot.scan(..).unwrap(), reference.scan(..).unwrap(), "snapshot after {:?}", operation);
        }
    }
    Ok(())
}

fn run_sequence(operations: &[Op]) -> TestCaseResult {
    let dir = TestDir::new();
    let options = Options {
        memtable_size: 192,
        target_file_size: 256,
        l0_compaction_trigger: 1,
        l0_stop_writes_trigger: 3,
        ..Options::default()
    };
    let mut model = ModelDb::open(&dir.0, options.clone()).unwrap();
    for epoch in operations.split(|operation| matches!(operation, Op::Reopen)) {
        let db = Db::open(&dir.0, options.clone()).unwrap();
        prop_assert_eq!(db.scan(..).unwrap(), model.scan(..).unwrap());
        run_epoch(&db, &mut model, epoch)?;
        db.sync().unwrap();
    }
    let db = Db::open(&dir.0, options).unwrap();
    prop_assert_eq!(db.scan(..).unwrap(), model.scan(..).unwrap());
    Ok(())
}

#[test]
fn model_sequence_exercises_every_operation() {
    let operations = vec![
        Op::Put(1, b"old".to_vec()),
        Op::Put(2, b"value".to_vec()),
        Op::Snapshot,
        Op::Batch(vec![Mutation::Put(1, b"new".to_vec()), Mutation::Delete(2)]),
        Op::Get(1),
        Op::Scan(0, 8),
        Op::SnapshotGet(0, 1),
        Op::SnapshotScan(0, 0, 8),
        Op::Flush,
        Op::Compact,
        Op::Reopen,
        Op::Delete(1),
        Op::Put(2, Vec::new()),
        Op::Flush,
        Op::Compact,
    ];
    run_sequence(&operations).unwrap();
}

#[test]
fn explicit_controls_flush_and_compact_real_data() {
    let dir = TestDir::new();
    let options = Options { memtable_size: 4096, target_file_size: 256, ..Options::default() };
    let db = Db::open(&dir.0, options).unwrap();
    db.put(key(1), b"value").unwrap();
    db.flush().unwrap();
    assert!(db.stats().bytes_written_per_level.get(&0).copied().unwrap_or(0) > 0);
    db.compact().unwrap();
    assert!(db.stats().bytes_written_per_level.get(&1).copied().unwrap_or(0) > 0);
    assert_eq!(db.get(key(1)).unwrap(), Some(b"value".to_vec()));
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(env!("CARGO_MANIFEST_DIR"), "/proptest-regressions/property.txt")))),
        ..ProptestConfig::default()
    })]

    #[test]
    fn operations_match_reference_model(operations in prop::collection::vec(operation(), 1..81)) {
        run_sequence(&operations)?;
    }
}
