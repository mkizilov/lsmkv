//! A WAL-backed, in-memory key-value database.

use std::fs::{self, File, OpenOptions};
use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::ops::Deref;
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
mod merge;
mod compaction;
#[cfg(test)]
mod read_tests;

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
    /// Compact when level 0 contains more than this many files.
    pub l0_compaction_trigger: usize,
    /// Stall writes at this many level-0 files until compaction completes.
    pub l0_stop_writes_trigger: usize,
    /// Target size of compaction output files and basis for level targets.
    pub target_file_size: u64,
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
            l0_compaction_trigger: 4,
            l0_stop_writes_trigger: 12,
            target_file_size: 2 * 1024 * 1024,
            statistics: Arc::new(Statistics::default()),
        }
    }
}

/// Shared counters; clone `Options` to share them between a database and readers.
#[derive(Debug, Default)]
pub struct Statistics {
    bloom_filter_block_reads_avoided: AtomicU64,
    logical_bytes_written: AtomicU64,
    bytes_written_per_level: Mutex<BTreeMap<u32, u64>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stats {
    pub bloom_filter_block_reads_avoided: u64,
    pub logical_bytes_written: u64,
    pub bytes_written_per_level: BTreeMap<u32, u64>,
    pub write_amplification: f64,
}

impl Statistics {
    fn record_level(&self, level: u32, bytes: u64) -> Result<(), Error> {
        let mut levels = self.bytes_written_per_level.lock().map_err(|_| Error::Poisoned)?;
        let total = levels.entry(level).or_default();
        *total = total.saturating_add(bytes);
        Ok(())
    }
}

struct Table {
    reader: SstableReader,
    number: u64,
    path: PathBuf,
    size: u64,
    obsolete: AtomicBool,
}

impl Deref for Table {
    type Target = SstableReader;
    fn deref(&self) -> &Self::Target { &self.reader }
}

impl Drop for Table {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            if let Err(error) = fs::remove_file(&self.path) {
                eprintln!("failed to remove obsolete SSTable {}: {error}", self.path.display());
            }
        }
    }
}

type TableVersion = BTreeMap<u32, Vec<Arc<Table>>>;

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
    tables: Arc<TableVersion>,
    frozen: Option<Arc<BTreeMemtable>>,
    pending: Option<Background>,
    wal_paths: Vec<PathBuf>,
    next_file: u64,
    flushed_sequence: u64,
    snapshots: BTreeMap<u64, usize>,
    notify: Sender<MaintenanceSignal>,
    closing: bool,
}

enum MaintenanceSignal { Finished, Stop }

struct NotifyOnDrop(Sender<MaintenanceSignal>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) { let _ = self.0.send(MaintenanceSignal::Finished); }
}

enum Background {
    Flush { handle: JoinHandle<Result<SstableReader, Error>>, number: u64, sequence: u64 },
    Compaction(JoinHandle<Result<compaction::Output, Error>>),
}

impl Background {
    fn is_finished(&self) -> bool {
        match self {
            Self::Flush { handle, .. } => handle.is_finished(),
            Self::Compaction(handle) => handle.is_finished(),
        }
    }
}

impl State {
    fn schedule_compaction(&mut self) -> Result<bool, Error> {
        if self.pending.is_some() || self.write_failed { return Ok(false); }
        let mut chosen = None;
        if self.tables.get(&0).map_or(0, Vec::len) > self.options.l0_compaction_trigger {
            chosen = Some(0);
        } else {
            for (&level, files) in self.tables.iter() {
                if level == 0 || files.is_empty() { continue; }
                let mut target = self.options.target_file_size;
                for _ in 0..level.min(20) { target = target.saturating_mul(10); }
                let size = files.iter().fold(0u64, |sum, file| sum.saturating_add(file.size));
                if size > target { chosen = Some(level); break; }
            }
        }
        let Some(level) = chosen else { return Ok(false); };
        let destination = level.checked_add(1).ok_or(Error::SequenceExhausted)?;
        let mut inputs = Vec::new();
        let mut removed = Vec::new();
        for input_level in [level, destination] {
            if let Some(files) = self.tables.get(&input_level) {
                for file in files {
                    inputs.push(Arc::clone(file));
                    removed.push((input_level, file.number));
                }
            }
        }
        let bottom = !self.tables.iter().any(|(&other, files)| other > destination && !files.is_empty());
        let job = compaction::Job {
            dir: self.dir.clone(), inputs, removed, level: destination,
            next_file: self.next_file, last_sequence: self.flushed_sequence,
            snapshots: self.snapshots.keys().copied().collect(), bottom,
            options: self.options.clone(), cache: Arc::clone(&self.cache),
        };
        let notify = self.notify.clone();
        self.pending = Some(Background::Compaction(
            thread::Builder::new().name("lsmkv-compaction".into()).spawn(move || {
                let _notification = NotifyOnDrop(notify);
                compaction::run(job)
            })?,
        ));
        Ok(true)
    }

    fn finish_flush(&mut self, wait: bool) -> Result<(), Error> {
        if !self.pending.as_ref().is_some_and(|job| wait || job.is_finished()) {
            return Ok(());
        }
        let Some(job) = self.pending.take() else { return Ok(()); };
        let result: Result<(), Error> = (|| match job {
            Background::Flush { handle, number, sequence } => {
                let reader = handle.join().map_err(|_| io::Error::other("flush thread panicked"))??;
                let path = flush::table_path(&self.dir, number);
                let size = fs::metadata(&path)?.len();
                Arc::make_mut(&mut self.tables).entry(0).or_default().push(Arc::new(Table {
                    reader, number, path, size, obsolete: AtomicBool::new(false),
                }));
                self.flushed_sequence = sequence;
                self.frozen = None;
                Ok(())
            }
            Background::Compaction(handle) => {
                let output = handle.join().map_err(|_| io::Error::other("compaction thread panicked"))??;
                let levels = Arc::make_mut(&mut self.tables);
                for (level, number) in &output.removed {
                    if let Some(files) = levels.get_mut(level) {
                        if let Some(index) = files.iter().position(|file| file.number == *number) {
                            files[index].obsolete.store(true, Ordering::Release);
                            files.remove(index);
                        }
                    }
                }
                levels.entry(output.level).or_default().extend(output.files);
                if let Some(files) = levels.get_mut(&output.level) {
                    files.sort_by(|a, b| a.user_key_bounds().map(|(first, _)| first).cmp(&b.user_key_bounds().map(|(first, _)| first)));
                }
                self.next_file = output.next_file;
                Ok(())
            }
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.write_failed = true;
                Err(error)
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
        let sequence = job.last_sequence;
        let notify = self.notify.clone();
        let handle = thread::Builder::new().name("lsmkv-flush".into()).spawn(move || {
            let _notification = NotifyOnDrop(notify);
            flush::run(job)
        })?;
        self.pending = Some(Background::Flush { handle, number: table_number, sequence });
        Ok(())
    }
}

/// A database handle. Dropping it closes the WAL before releasing the lock.
pub struct Db {
    state: Arc<Mutex<State>>,
    _lock: File,
    statistics: Arc<Statistics>,
    coordinator: Option<JoinHandle<()>>,
}

impl Db {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self, Error> {
        if options.bloom_bits_per_key > 30 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bloom bits per key must be at most 30").into());
        }
        if options.target_file_size == 0 || options.l0_stop_writes_trigger <= options.l0_compaction_trigger {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid compaction settings").into());
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
        let mut tables: TableVersion = BTreeMap::new();
        for &(level, number) in &version.files {
            let table_path = flush::table_path(path, number);
            let size = fs::metadata(&table_path)?.len();
            let reader = SstableReader::open(&table_path, Arc::clone(&cache))?;
            let table = Arc::new(Table { reader, number, path: table_path, size, obsolete: AtomicBool::new(false) });
            if level != 0 && table.user_key_bounds().is_none() {
                return Err(Error::Corruption("empty deeper-level table"));
            }
            tables.entry(level).or_default().push(table);
        }
        for (&level, files) in &mut tables {
            if level == 0 { continue; }
            files.sort_by(|a, b| a.user_key_bounds().map(|(first, _)| first).cmp(&b.user_key_bounds().map(|(first, _)| first)));
            for pair in files.windows(2) {
                let (_, previous_last) = pair[0].user_key_bounds().ok_or(Error::Corruption("empty deeper-level table"))?;
                let (next_first, _) = pair[1].user_key_bounds().ok_or(Error::Corruption("empty deeper-level table"))?;
                if previous_last >= next_first { return Err(Error::Corruption("overlapping table ranges")); }
            }
        }
        let (table, wal_paths, wal, next_sequence, next_file) = recover(path, &version, options.sync_policy)?;
        let (notify, receiver) = mpsc::channel();
        let state = Arc::new(Mutex::new(State {
                table, wal, next_sequence, write_failed: false,
                dir: path.to_path_buf(), options: options.clone(), cache, tables: Arc::new(tables),
                frozen: None, pending: None, wal_paths, next_file,
                flushed_sequence: version.last_sequence, snapshots: BTreeMap::new(),
                notify, closing: false,
            }));
        let weak = Arc::downgrade(&state);
        let coordinator = thread::Builder::new().name("lsmkv-maintenance".into()).spawn(move || {
            while let Ok(signal) = receiver.recv() {
                if matches!(signal, MaintenanceSignal::Stop) { break; }
                let Some(state) = weak.upgrade() else { break };
                let mut state = match state.lock() {
                    Ok(state) => state,
                    Err(error) => { eprintln!("maintenance mutex poisoned"); error.into_inner() }
                };
                if let Err(error) = state.finish_flush(true).and_then(|()| {
                    if !state.closing { state.schedule_compaction()?; }
                    Ok(())
                }) {
                    state.write_failed = true;
                    eprintln!("background maintenance failed: {error}");
                }
            }
        })?;
        let db = Self {
            state,
            _lock: lock,
            statistics: options.statistics,
            coordinator: Some(coordinator),
        };
        db.state()?.schedule_compaction()?;
        Ok(db)
    }

    fn state(&self) -> Result<MutexGuard<'_, State>, Error> {
        self.state.lock().map_err(|_| Error::Poisoned)
    }

    /// Commit a batch atomically. Empty batches are a no-op.
    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        let mut state = self.state()?;
        loop {
            state.finish_flush(false)?;
            if state.write_failed { return Err(Error::WriteFailed); }
            state.schedule_compaction()?;
            if state.tables.get(&0).map_or(0, Vec::len) < state.options.l0_stop_writes_trigger { break; }
            if state.pending.is_none() { return Err(Error::WriteFailed); }
            state.finish_flush(true)?;
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
        let logical_bytes = operations.iter().fold(0u64, |total, operation| {
            let size = match operation {
                Operation::Put { key, value, .. } => key.len().saturating_add(value.len()),
                Operation::Delete { key, .. } => key.len(),
            };
            total.saturating_add(size as u64)
        });
        if let Err(error) = state.wal.append(&operations) {
            state.write_failed = true;
            return Err(error.into());
        }
        let _ = self.statistics.logical_bytes_written.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| Some(value.saturating_add(logical_bytes)));
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
        self.get_at(key.as_ref(), u64::MAX)
    }

    fn get_at(&self, key: &[u8], sequence: u64) -> Result<Option<Vec<u8>>, Error> {
        let mut state = self.state()?;
        state.finish_flush(false)?;
        match state.table.lookup(key, sequence) {
            Lookup::Value(value) => return Ok(Some(value.to_vec())),
            Lookup::Deleted => return Ok(None),
            Lookup::Missing => {}
        }
        if let Some(table) = &state.frozen {
            match table.lookup(key, sequence) {
                Lookup::Value(value) => return Ok(Some(value.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::Missing => {}
            }
        }
        let tables = Arc::clone(&state.tables);
        drop(state);
        for (&level, files) in tables.as_ref() {
            if level == 0 {
                for table in files.iter().rev() {
                    if let Some((kind, value)) = table.get_user_at(key, sequence)? {
                        return Ok(if kind == Kind::Put { Some(value) } else { None });
                    }
                }
            } else {
                let position = files.partition_point(|table| {
                    table.user_key_bounds().is_some_and(|(_, last)| last < key)
                });
                if let Some(table) = files.get(position) {
                    if table.user_key_bounds().is_some_and(|(first, last)| first <= key && key <= last) {
                        if let Some((kind, value)) = table.get_user_at(key, sequence)? {
                            return Ok(if kind == Kind::Put { Some(value) } else { None });
                        }
                    }
                }
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
        self.scan_at(range, u64::MAX)
    }

    fn scan_at<R: RangeBounds<Vec<u8>>>(&self, range: R, sequence: u64) -> Result<Vec<Entry>, Error> {
        let mut state = self.state()?;
        state.finish_flush(false)?;
        let mut sources: Vec<merge::Source<'_>> = Vec::new();
        sources.push(Box::new(state.table.entries().map(|(key, value)| Ok((key.clone(), value.clone())))));
        if let Some(table) = &state.frozen {
            sources.push(Box::new(table.entries().map(|(key, value)| Ok((key.clone(), value.clone())))));
        }
        for files in state.tables.values() {
            for table in files {
                sources.push(Box::new(table.iter()));
            }
        }
        let mut result = Vec::new();
        for entry in merge::MergeIterator::new(sources, sequence)? {
            let (key, value) = entry?;
            if range.contains(&key) { result.push((key, value)); }
        }
        Ok(result)
    }

    /// Pin the current sequence for consistent reads while writes continue.
    pub fn snapshot(&self) -> Result<Snapshot<'_>, Error> {
        let mut state = self.state()?;
        let sequence = state.next_sequence - 1;
        *state.snapshots.entry(sequence).or_default() += 1;
        Ok(Snapshot { db: self, sequence })
    }

    /// Wait for all writes to reach stable storage.
    pub fn sync(&self) -> Result<(), Error> {
        let mut state = self.state()?;
        if state.write_failed { return Err(Error::WriteFailed); }
        state.wal.sync()?;
        loop {
            state.finish_flush(true)?;
            if !state.schedule_compaction()? { break; }
        }
        Ok(())
    }

    /// Return a snapshot of counters shared through the supplied options.
    pub fn stats(&self) -> Stats {
        let levels = match self.statistics.bytes_written_per_level.lock() {
            Ok(levels) => levels.clone(),
            Err(error) => { eprintln!("statistics mutex poisoned"); error.into_inner().clone() }
        };
        let logical = self.statistics.logical_bytes_written.load(Ordering::Relaxed);
        let written = levels.values().fold(0u64, |total, bytes| total.saturating_add(*bytes));
        Stats {
            bloom_filter_block_reads_avoided: self.statistics.bloom_filter_block_reads_avoided.load(Ordering::Relaxed),
            logical_bytes_written: logical,
            bytes_written_per_level: levels,
            write_amplification: if logical == 0 { 0.0 } else { written as f64 / logical as f64 },
        }
    }
}

/// A sequence-pinned read view. Versions are retained while snapshots exist.
pub struct Snapshot<'a> {
    db: &'a Db,
    sequence: u64,
}

impl Snapshot<'_> {
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Error> {
        self.db.get_at(key.as_ref(), self.sequence)
    }

    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Vec<Entry>, Error> {
        self.db.scan_at(range, self.sequence)
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        let mut state = match self.db.state.lock() {
            Ok(state) => state,
            Err(error) => { eprintln!("database mutex poisoned while releasing snapshot"); error.into_inner() }
        };
        if let Some(count) = state.snapshots.get_mut(&self.sequence) {
            *count -= 1;
            if *count == 0 { state.snapshots.remove(&self.sequence); }
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
        state.closing = true;
        if let Err(error) = state.finish_flush(true) {
            eprintln!("failed to complete database flush: {error}");
        }
        let _ = state.notify.send(MaintenanceSignal::Stop);
        drop(state);
        if let Some(handle) = self.coordinator.take() {
            if handle.join().is_err() { eprintln!("maintenance thread panicked"); }
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
