//! A WAL-backed, in-memory key-value database.

use std::fs::{self, File, OpenOptions};
use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use fs2::FileExt;
use memtable::{BTreeMemtable, InternalKey, Kind, Lookup, Memtable};
use sstable::{BlockCache, SstableReader};
use wal::{Operation, SyncPolicy, WalReader, WalWriter};

pub mod memtable;
pub mod encoding;
pub mod sstable;
pub mod wal;
mod manifest;
mod flush;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("database directory is already locked")]
    Locked,
    #[error("database mutex is poisoned")]
    Poisoned,
    #[error("a previous WAL write failed; reopen the database")]
    WriteFailed,
    #[error("database sequence numbers exhausted")]
    SequenceExhausted,
    #[error("invalid sequence order in WAL")]
    InvalidSequence,
    #[error("SSTable corruption: {0}")]
    Corruption(&'static str),
}

/// Database configuration. By default each write is synchronized to disk.
#[derive(Debug, Clone)]
pub struct Options {
    pub sync_policy: SyncPolicy,
    /// Shared SSTable block-cache capacity in bytes; zero disables caching.
    pub block_cache_capacity: usize,
    /// Bloom-filter bits per distinct user key (0 disables filters, maximum 30).
    pub bloom_bits_per_key: usize,
    /// Approximate mutable memtable size before scheduling a flush.
    pub memtable_size: usize,
    /// Counters shared by database and SSTable readers using these options.
    pub statistics: Arc<Statistics>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sync_policy: SyncPolicy::EveryWrite,
            block_cache_capacity: 8 * 1024 * 1024,
            bloom_bits_per_key: 10,
            memtable_size: 4 * 1024 * 1024,
            statistics: Arc::new(Statistics::default()),
        }
    }
}

/// Shared counters; clone `Options` to share them between a database and readers.
#[derive(Debug, Default)]
pub struct Statistics {
    bloom_filter_block_reads_avoided: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct Stats {
    pub bloom_filter_block_reads_avoided: u64,
}

pub type Entry = (Vec<u8>, Vec<u8>);

enum Mutation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// A group of mutations committed together as one WAL record.
#[derive(Default)]
pub struct WriteBatch {
    mutations: Vec<Mutation>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.mutations.push(Mutation::Put(key.as_ref().to_vec(), value.as_ref().to_vec()));
        self
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> &mut Self {
        self.mutations.push(Mutation::Delete(key.as_ref().to_vec()));
        self
    }
}

struct State {
    table: BTreeMemtable,
    wal: WalWriter,
    next_sequence: u64,
    write_failed: bool,
    dir: PathBuf,
    options: Options,
    cache: Arc<BlockCache>,
    tables: Vec<SstableReader>,
    frozen: Option<Arc<BTreeMemtable>>,
    pending: Option<JoinHandle<Result<SstableReader, Error>>>,
    wal_paths: Vec<PathBuf>,
    next_file: u64,
}

impl State {
    fn finish_flush(&mut self, wait: bool) -> Result<(), Error> {
        if !self.pending.as_ref().is_some_and(|handle| wait || handle.is_finished()) {
            return Ok(());
        }
        let Some(handle) = self.pending.take() else { return Ok(()); };
        let result = handle.join().map_err(|_| io::Error::other("flush thread panicked"))
            .and_then(|result| result.map_err(io::Error::other));
        match result {
            Ok(table) => {
                self.tables.push(table);
                self.frozen = None;
                Ok(())
            }
            Err(error) => {
                self.write_failed = true;
                Err(error.into())
            }
        }
    }

    fn rotate_and_flush(&mut self) -> Result<(), Error> {
        self.finish_flush(true)?;
        self.wal.sync()?;
        let wal_number = self.next_file;
        let table_number = wal_number.checked_add(1).ok_or(Error::SequenceExhausted)?;
        let next_file = table_number.checked_add(1).ok_or(Error::SequenceExhausted)?;
        let new_path = flush::wal_path(&self.dir, wal_number);
        let file = OpenOptions::new().write(true).create_new(true).open(&new_path)?;
        file.sync_all()?;
        manifest::sync_dir(&self.dir)?;
        let new_wal = WalWriter::open(&new_path, self.options.sync_policy)?;
        let old_wal = std::mem::replace(&mut self.wal, new_wal);
        old_wal.close()?;
        let old_paths = std::mem::replace(&mut self.wal_paths, vec![new_path]);
        let frozen = Arc::new(std::mem::take(&mut self.table));
        self.frozen = Some(Arc::clone(&frozen));
        self.next_file = next_file;
        let job = flush::FlushJob {
            dir: self.dir.clone(),
            table: frozen,
            wal_paths: old_paths,
            table_number,
            next_file,
            last_sequence: self.next_sequence - 1,
            options: self.options.clone(),
            cache: Arc::clone(&self.cache),
        };
        self.pending = Some(thread::Builder::new().name("lsmkv-flush".into()).spawn(move || flush::run(job))?);
        Ok(())
    }
}

/// A database handle. Dropping it closes the WAL before releasing the lock.
pub struct Db {
    state: Mutex<State>,
    _lock: File,
    statistics: Arc<Statistics>,
}

impl Db {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self, Error> {
        if options.bloom_bits_per_key > 30 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bloom bits per key must be at most 30").into());
        }
        let path = path.as_ref();
        fs::create_dir_all(path)?;
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(path.join("LOCK"))?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Err(Error::Locked),
            Err(error) => return Err(error.into()),
        }
        let version = manifest::open(path)?;
        let cache = Arc::new(BlockCache::new(&options));
        let mut tables = Vec::new();
        for &(level, number) in &version.files {
            if level != 0 { return Err(Error::Corruption("unsupported table level")); }
            tables.push(SstableReader::open(flush::table_path(path, number), Arc::clone(&cache))?);
        }
        let (table, wal_paths, wal, next_sequence, next_file) = recover(path, &version, options.sync_policy)?;
        Ok(Self {
            state: Mutex::new(State {
                table, wal, next_sequence, write_failed: false,
                dir: path.to_path_buf(), options: options.clone(), cache, tables,
                frozen: None, pending: None, wal_paths, next_file,
            }),
            _lock: lock,
            statistics: options.statistics,
        })
    }

    fn state(&self) -> Result<MutexGuard<'_, State>, Error> {
        self.state.lock().map_err(|_| Error::Poisoned)
    }

    /// Commit a batch atomically. Empty batches are a no-op.
    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        let mut state = self.state()?;
        state.finish_flush(false)?;
        if state.write_failed {
            return Err(Error::WriteFailed);
        }
        if batch.mutations.is_empty() {
            return Ok(());
        }
        let count = u64::try_from(batch.mutations.len()).map_err(|_| Error::SequenceExhausted)?;
        let next = state.next_sequence.checked_add(count).ok_or(Error::SequenceExhausted)?;
        let mut sequence = state.next_sequence;
        let mut operations = Vec::with_capacity(batch.mutations.len());
        for mutation in batch.mutations {
            operations.push(match mutation {
                Mutation::Put(key, value) => Operation::Put { sequence, key, value },
                Mutation::Delete(key) => Operation::Delete { sequence, key },
            });
            sequence += 1;
        }
        if let Err(error) = state.wal.append(&operations) {
            state.write_failed = true;
            return Err(error.into());
        }
        for operation in operations {
            apply(&mut state.table, operation);
        }
        state.next_sequence = next;
        if state.table.approximate_memory_usage() > state.options.memtable_size {
            if let Err(error) = state.rotate_and_flush() {
                state.write_failed = true;
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(batch)
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Error> {
        let key = key.as_ref();
        let mut state = self.state()?;
        state.finish_flush(false)?;
        match state.table.lookup(key, u64::MAX) {
            Lookup::Value(value) => return Ok(Some(value.to_vec())),
            Lookup::Deleted => return Ok(None),
            Lookup::Missing => {}
        }
        if let Some(table) = &state.frozen {
            match table.lookup(key, u64::MAX) {
                Lookup::Value(value) => return Ok(Some(value.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::Missing => {}
            }
        }
        for table in state.tables.iter().rev() {
            if let Some((kind, value)) = table.get_user(key)? {
                return Ok(if kind == Kind::Put { Some(value) } else { None });
            }
        }
        Ok(None)
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(batch)
    }

    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Vec<Entry>, Error> {
        let mut state = self.state()?;
        state.finish_flush(false)?;
        let mut values: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut add = |key: &InternalKey, value: &[u8]| {
            if range.contains(&key.user_key) {
                values.entry(key.user_key.clone()).or_insert_with(|| {
                    if key.kind == Kind::Put { Some(value.to_vec()) } else { None }
                });
            }
        };
        for (key, value) in state.table.entries() { add(key, value); }
        if let Some(table) = &state.frozen {
            for (key, value) in table.entries() { add(key, value); }
        }
        for table in state.tables.iter().rev() {
            for entry in table.iter() {
                let (key, value) = entry?;
                add(&key, &value);
            }
        }
        Ok(values.into_iter().filter_map(|(key, value)| value.map(|value| (key, value))).collect())
    }

    /// Wait for all writes to reach stable storage.
    pub fn sync(&self) -> Result<(), Error> {
        let mut state = self.state()?;
        state.wal.sync()?;
        state.finish_flush(true)
    }

    /// Return a snapshot of counters shared through the supplied options.
    pub fn stats(&self) -> Stats {
        Stats {
            bloom_filter_block_reads_avoided: self.statistics.bloom_filter_block_reads_avoided.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                eprintln!("database mutex poisoned while closing");
                error.into_inner()
            }
        };
        if let Err(error) = state.finish_flush(true) {
            eprintln!("failed to complete database flush: {error}");
        }
    }
}

fn numbered(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    let digits = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) { return None; }
    digits.parse().ok()
}

fn recover(
    dir: &Path,
    version: &manifest::Version,
    policy: SyncPolicy,
) -> Result<(BTreeMemtable, Vec<PathBuf>, WalWriter, u64, u64), Error> {
    let mut paths = Vec::new();
    let mut max_number = version.next_file - 1;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name == "WAL" {
            paths.push((0, entry.path()));
        } else if let Some(number) = numbered(name, "WAL-", "") {
            if number == 0 { return Err(Error::Corruption("invalid WAL number")); }
            max_number = max_number.max(number);
            paths.push((number, entry.path()));
        } else if let Some(number) = numbered(name, "SST-", ".sst")
            .or_else(|| numbered(name, "SST-", ".tmp")) {
            max_number = max_number.max(number);
        }
    }
    paths.sort_by_key(|(number, _)| *number);
    let mut next_file = max_number.checked_add(1).ok_or(Error::SequenceExhausted)?;
    if paths.is_empty() {
        let path = if version.last_sequence == 0 { dir.join("WAL") } else {
            let path = flush::wal_path(dir, next_file);
            next_file = next_file.checked_add(1).ok_or(Error::SequenceExhausted)?;
            path
        };
        let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        file.sync_all()?;
        manifest::sync_dir(dir)?;
        paths.push((0, path));
    }
    let mut table = BTreeMemtable::default();
    let mut next_sequence = version.last_sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
    for (_, path) in &paths {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut reader = WalReader::new(&file);
        while let Some(batch) = reader.next_batch()? {
            let mut previous = None;
            let mut older = None;
            for operation in batch {
                let sequence = match &operation {
                    Operation::Put { sequence, .. } | Operation::Delete { sequence, .. } => *sequence,
                };
                if sequence == 0 || previous.is_some_and(|last: u64| last.checked_add(1) != Some(sequence)) {
                    return Err(Error::InvalidSequence);
                }
                let flushed = sequence <= version.last_sequence;
                if older.is_some_and(|old| old != flushed) { return Err(Error::InvalidSequence); }
                older = Some(flushed);
                previous = Some(sequence);
                if !flushed {
                    if sequence != next_sequence { return Err(Error::InvalidSequence); }
                    next_sequence = next_sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
                    apply(&mut table, operation);
                }
            }
        }
        let valid = reader.valid_bytes();
        if valid != file.metadata()?.len() {
            file.set_len(valid)?;
            file.sync_all()?;
        }
    }
    let wal_paths: Vec<PathBuf> = paths.into_iter().map(|(_, path)| path).collect();
    let active = wal_paths.last().ok_or(Error::Corruption("missing WAL"))?;
    let wal = WalWriter::open(active, policy)?;
    Ok((table, wal_paths, wal, next_sequence, next_file))
}

fn apply(table: &mut BTreeMemtable, operation: Operation) {
    match operation {
        Operation::Put { sequence, key, value } => {
            table.insert(InternalKey::new(key, sequence, Kind::Put), value);
        }
        Operation::Delete { sequence, key } => {
            table.insert(InternalKey::new(key, sequence, Kind::Delete), Vec::new());
        }
    }
}
