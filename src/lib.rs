//! A WAL-backed, in-memory key-value database.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use fs2::FileExt;
use memtable::{BTreeMemtable, InternalKey, Kind, Memtable};
use wal::{Operation, SyncPolicy, WalReader, WalWriter};

pub mod memtable;
pub mod wal;

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
}

/// Database configuration. By default each write is synchronized to disk.
#[derive(Debug, Clone)]
pub struct Options {
    pub sync_policy: SyncPolicy,
}

impl Default for Options {
    fn default() -> Self {
        Self { sync_policy: SyncPolicy::EveryWrite }
    }
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
}

/// A database handle. Dropping it closes the WAL before releasing the lock.
pub struct Db {
    state: Mutex<State>,
    _lock: File,
}

impl Db {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self, Error> {
        let path = path.as_ref();
        fs::create_dir_all(path)?;
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(path.join("LOCK"))?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Err(Error::Locked),
            Err(error) => return Err(error.into()),
        }
        let wal_path = path.join("WAL");
        let wal_file = OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(&wal_path)?;
        let mut reader = WalReader::new(&wal_file);
        let mut table = BTreeMemtable::default();
        let mut next_sequence = 1u64;
        while let Some(batch) = reader.next_batch()? {
            for operation in batch {
                let sequence = match &operation {
                    Operation::Put { sequence, .. } | Operation::Delete { sequence, .. } => *sequence,
                };
                if sequence != next_sequence {
                    return Err(Error::InvalidSequence);
                }
                next_sequence = next_sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
                apply(&mut table, operation);
            }
        }
        let valid_bytes = reader.valid_bytes();
        if valid_bytes != wal_file.metadata()?.len() {
            wal_file.set_len(valid_bytes)?;
            wal_file.sync_all()?;
        }
        let wal = WalWriter::open(wal_path, options.sync_policy)?;
        Ok(Self {
            state: Mutex::new(State { table, wal, next_sequence, write_failed: false }),
            _lock: lock,
        })
    }

    fn state(&self) -> Result<MutexGuard<'_, State>, Error> {
        self.state.lock().map_err(|_| Error::Poisoned)
    }

    /// Commit a batch atomically. Empty batches are a no-op.
    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        let mut state = self.state()?;
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
        Ok(())
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(batch)
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.state()?.table.get(key.as_ref(), u64::MAX).map(<[u8]>::to_vec))
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(batch)
    }

    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Vec<Entry>, Error> {
        Ok(self.state()?.table.scan(range))
    }

    /// Wait for all writes to reach stable storage.
    pub fn sync(&self) -> Result<(), Error> {
        self.state()?.wal.sync().map_err(Error::from)
    }
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
