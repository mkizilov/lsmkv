//! Shared, project-local benchmark workloads.

use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::{Db, Options, WriteBatch};

pub const DEFAULT_KEYS: u64 = 1_000_000;
pub const VALUE: [u8; 100] = [b'x'; 100];
static NEXT: AtomicU64 = AtomicU64::new(0);

pub type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Workload {
    SequentialWrite,
    RandomWrite,
    ReadHit,
    ReadMiss,
    Scan100,
}

impl Workload {
    pub const ALL: [Self; 5] = [Self::SequentialWrite, Self::RandomWrite, Self::ReadHit, Self::ReadMiss, Self::Scan100];

    pub fn name(self) -> &'static str {
        match self {
            Self::SequentialWrite => "sequential-write",
            Self::RandomWrite => "random-write",
            Self::ReadHit => "read-hit",
            Self::ReadMiss => "read-miss",
            Self::Scan100 => "scan-100",
        }
    }
}

impl FromStr for Workload {
    type Err = io::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL.into_iter().find(|workload| workload.name() == value)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unknown benchmark workload"))
    }
}

struct OwnedDir(PathBuf);

impl OwnedDir {
    fn new() -> io::Result<Self> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/benchmarks");
        fs::create_dir_all(&root)?;
        for _ in 0..1000 {
            let path = root.join(format!("run-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "could not allocate benchmark directory"))
    }
}

impl Drop for OwnedDir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!("failed to remove benchmark directory {}: {error}", self.0.display());
        }
    }
}

/// The database drops before its owned directory is removed.
pub struct Dataset {
    db: Db,
    _dir: OwnedDir,
    keys: u64,
}

impl Dataset {
    pub fn new(keys: u64) -> BenchResult<Self> {
        if !(100..=u64::MAX / 2).contains(&keys) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "key count must be at least 100 and at most half of u64::MAX").into());
        }
        let dir = OwnedDir::new()?;
        let db = Db::open(&dir.0, Options::default())?;
        let mut first = 0;
        while first < keys {
            let end = first.saturating_add(1000).min(keys);
            let mut batch = WriteBatch::new();
            for number in first..end { batch.put((number * 2).to_be_bytes(), VALUE); }
            db.write(batch)?;
            first = end;
        }
        db.sync()?;
        Ok(Self { db, _dir: dir, keys })
    }

    pub fn db(&self) -> &Db { &self.db }
    pub fn keys(&self) -> u64 { self.keys }
}

fn random_index(sequence: u64, keys: u64) -> u64 {
    let mut value = sequence.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) % keys
}

/// Execute one operation and check the returned data.
pub fn step(db: &Db, keys: u64, workload: Workload, sequence: u64) -> BenchResult<()> {
    if keys < 100 || keys > u64::MAX / 2 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key count").into());
    }
    let random = random_index(sequence, keys);
    match workload {
        Workload::SequentialWrite => db.put(((sequence % keys) * 2).to_be_bytes(), VALUE)?,
        Workload::RandomWrite => db.put((random * 2).to_be_bytes(), VALUE)?,
        Workload::ReadHit => {
            if db.get((random * 2).to_be_bytes())?.as_deref() != Some(&VALUE[..]) {
                return Err(io::Error::other("benchmark hit returned an unexpected value").into());
            }
        }
        Workload::ReadMiss => {
            if db.get((random * 2 + 1).to_be_bytes())?.is_some() {
                return Err(io::Error::other("benchmark miss returned a value").into());
            }
        }
        Workload::Scan100 => {
            let start = random_index(sequence, keys - 99);
            let entries = db.scan((start * 2).to_be_bytes().to_vec()..((start + 100) * 2).to_be_bytes().to_vec())?;
            if entries.len() != 100 || entries.iter().enumerate().any(|(offset, (key, value))| {
                key.as_slice() != ((start + offset as u64) * 2).to_be_bytes().as_slice() || value.as_slice() != &VALUE[..]
            }) {
                return Err(io::Error::other("benchmark scan returned unexpected data").into());
            }
        }
    }
    Ok(())
}

/// Run one workload for a duration, including the final durability barrier.
pub fn run_for(dataset: &Dataset, workload: Workload, duration: Duration) -> BenchResult<(u64, Duration)> {
    if duration.is_zero() { return Err(io::Error::new(io::ErrorKind::InvalidInput, "duration must be positive").into()); }
    let started = Instant::now();
    let deadline = started.checked_add(duration).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "duration too large"))?;
    let mut operations = 0u64;
    while Instant::now() < deadline {
        step(dataset.db(), dataset.keys(), workload, operations)?;
        operations = operations.checked_add(1).ok_or_else(|| io::Error::other("operation count overflow"))?;
    }
    dataset.db().sync()?;
    Ok((operations, started.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::{random_index, step, Dataset, Workload};
    use std::str::FromStr;

    #[test]
    fn workload_names_and_random_indices_are_deterministic() {
        for workload in Workload::ALL {
            assert_eq!(Workload::from_str(workload.name()).unwrap(), workload);
        }
        assert!(Workload::from_str("invalid").is_err());
        for sequence in 0..10_000 {
            assert_eq!(random_index(sequence, 100), random_index(sequence, 100));
            assert!(random_index(sequence, 100) < 100);
        }
    }

    #[test]
    fn every_workload_runs_on_a_small_dataset() {
        let dataset = Dataset::new(120).unwrap();
        for workload in Workload::ALL {
            for sequence in 0..4 { step(dataset.db(), dataset.keys(), workload, sequence).unwrap(); }
        }
    }
}
