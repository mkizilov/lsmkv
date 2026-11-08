//! Ordered, versioned in-memory storage for database mutations.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::mem::size_of;

/// The operation represented by an internal key.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Kind {
    Put,
    Delete,
}

/// Sorts by user key ascending, sequence descending, then kind ascending.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub sequence: Reverse<u64>,
    pub kind: Kind,
}

impl InternalKey {
    pub fn new(user_key: impl Into<Vec<u8>>, sequence: u64, kind: Kind) -> Self {
        Self {
            user_key: user_key.into(),
            sequence: Reverse(sequence),
            kind,
        }
    }
}

/// Distinguishes a tombstone from an absent version when merging storage layers.
#[derive(Debug, Eq, PartialEq)]
pub enum Lookup<'a> {
    Value(&'a [u8]),
    Deleted,
    Missing,
}

/// Minimal interface for an interchangeable ordered memtable implementation.
pub trait Memtable {
    fn insert(&mut self, key: InternalKey, value: Vec<u8>);
    fn lookup(&self, user_key: &[u8], snapshot_seq: u64) -> Lookup<'_>;
    fn approximate_memory_usage(&self) -> usize;

    fn get(&self, user_key: &[u8], snapshot_seq: u64) -> Option<&[u8]> {
        match self.lookup(user_key, snapshot_seq) {
            Lookup::Value(value) => Some(value),
            Lookup::Deleted | Lookup::Missing => None,
        }
    }
}

/// A memtable backed by a B-tree.
#[derive(Debug, Default)]
pub struct BTreeMemtable {
    entries: BTreeMap<InternalKey, Vec<u8>>,
    memory_usage: usize,
}

impl Memtable for BTreeMemtable {
    fn insert(&mut self, key: InternalKey, value: Vec<u8>) {
        let entry_size = size_of::<InternalKey>() + key.user_key.len() + size_of::<Vec<u8>>();
        self.memory_usage += entry_size + value.len();
        if let Some(old) = self.entries.insert(key, value) {
            self.memory_usage -= entry_size + old.len();
        }
    }

    fn lookup(&self, user_key: &[u8], snapshot_seq: u64) -> Lookup<'_> {
        let start = InternalKey::new(user_key.to_vec(), snapshot_seq, Kind::Put);
        match self.entries.range(start..).next() {
            Some((key, value)) if key.user_key == user_key => match key.kind {
                Kind::Put => Lookup::Value(value),
                Kind::Delete => Lookup::Deleted,
            },
            _ => Lookup::Missing,
        }
    }

    fn approximate_memory_usage(&self) -> usize {
        self.memory_usage
    }
}

#[cfg(test)]
mod tests {
    use super::{BTreeMemtable, InternalKey, Kind, Lookup, Memtable};

    #[test]
    fn versions_and_snapshots_respect_deletes() {
        let mut table = BTreeMemtable::default();
        table.insert(
            InternalKey::new(b"key".to_vec(), 2, Kind::Put),
            b"old".to_vec(),
        );
        table.insert(
            InternalKey::new(b"key".to_vec(), 5, Kind::Put),
            b"new".to_vec(),
        );
        table.insert(
            InternalKey::new(b"key".to_vec(), 7, Kind::Delete),
            Vec::new(),
        );
        table.insert(
            InternalKey::new(b"key".to_vec(), 9, Kind::Put),
            b"again".to_vec(),
        );
        table.insert(
            InternalKey::new(b"other".to_vec(), 1, Kind::Put),
            b"x".to_vec(),
        );
        assert_eq!(table.get(b"key", 1), None);
        assert_eq!(table.get(b"key", 2), Some(&b"old"[..]));
        assert_eq!(table.get(b"key", 6), Some(&b"new"[..]));
        assert_eq!(table.get(b"key", 7), None);
        assert_eq!(table.lookup(b"key", 8), Lookup::Deleted);
        assert_eq!(table.get(b"key", u64::MAX), Some(&b"again"[..]));
        assert_eq!(table.lookup(b"ke", u64::MAX), Lookup::Missing);
        assert_eq!(table.lookup(b"missing", u64::MAX), Lookup::Missing);
    }

    #[test]
    fn replacing_internal_key_updates_memory_estimate() {
        let mut table = BTreeMemtable::default();
        let key = InternalKey::new(b"a".to_vec(), 1, Kind::Put);
        assert_eq!(table.approximate_memory_usage(), 0);
        table.insert(key.clone(), b"long".to_vec());
        let initial = table.approximate_memory_usage();
        table.insert(key, b"x".to_vec());
        assert_eq!(table.approximate_memory_usage(), initial - 3);
        assert_eq!(table.get(b"a", 1), Some(&b"x"[..]));
    }
}
