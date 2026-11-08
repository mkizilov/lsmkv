//! Heap merge of sorted internal-key sources with snapshot visibility.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::memtable::{InternalKey, Kind};
use crate::{Entry, Error};

pub type Source<'a> = Box<dyn Iterator<Item = Result<(InternalKey, Vec<u8>), Error>> + 'a>;

pub struct MergeIterator<'a> {
    sources: Vec<Source<'a>>,
    heap: BinaryHeap<Reverse<(InternalKey, usize, Vec<u8>)>>,
    sequence: u64,
    previous_user_key: Option<Vec<u8>>,
    failed: bool,
}

impl<'a> MergeIterator<'a> {
    pub fn new(sources: Vec<Source<'a>>, sequence: u64) -> Result<Self, Error> {
        let mut merged = Self {
            sources,
            heap: BinaryHeap::new(),
            sequence,
            previous_user_key: None,
            failed: false,
        };
        for source in 0..merged.sources.len() {
            merged.advance(source)?;
        }
        Ok(merged)
    }

    fn advance(&mut self, source: usize) -> Result<(), Error> {
        if let Some(entry) = self.sources[source].next() {
            let (key, value) = entry?;
            self.heap.push(Reverse((key, source, value)));
        }
        Ok(())
    }
}

impl Iterator for MergeIterator<'_> {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        while let Some(Reverse((key, source, value))) = self.heap.pop() {
            if let Err(error) = self.advance(source) {
                self.failed = true;
                return Some(Err(error));
            }
            if key.sequence.0 > self.sequence
                || self.previous_user_key.as_ref() == Some(&key.user_key)
            {
                continue;
            }
            self.previous_user_key = Some(key.user_key.clone());
            if key.kind == Kind::Put {
                return Some(Ok((key.user_key, value)));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{MergeIterator, Source};
    use crate::memtable::{InternalKey, Kind};
    use crate::Error;

    #[test]
    fn merges_versions_and_suppresses_tombstones() {
        let first = vec![
            (
                InternalKey::new(b"a".to_vec(), 5, Kind::Put),
                b"new".to_vec(),
            ),
            (InternalKey::new(b"b".to_vec(), 4, Kind::Delete), Vec::new()),
        ];
        let second = vec![
            (
                InternalKey::new(b"a".to_vec(), 2, Kind::Put),
                b"old".to_vec(),
            ),
            (
                InternalKey::new(b"b".to_vec(), 1, Kind::Put),
                b"hidden".to_vec(),
            ),
            (
                InternalKey::new(b"c".to_vec(), 3, Kind::Put),
                b"visible".to_vec(),
            ),
        ];
        let sources: Vec<Source<'_>> = vec![
            Box::new(first.into_iter().map(Ok)),
            Box::new(second.into_iter().map(Ok)),
        ];
        let result: Result<Vec<_>, _> = MergeIterator::new(sources, 4).unwrap().collect();
        assert_eq!(
            result.unwrap(),
            vec![
                (b"a".to_vec(), b"old".to_vec()),
                (b"c".to_vec(), b"visible".to_vec())
            ]
        );
    }

    #[test]
    fn source_error_is_returned_once() {
        let entries = vec![
            Ok((
                InternalKey::new(b"a".to_vec(), 1, Kind::Put),
                b"value".to_vec(),
            )),
            Err(Error::Corruption("test source")),
        ];
        let sources: Vec<Source<'_>> = vec![Box::new(entries.into_iter())];
        let mut merged = MergeIterator::new(sources, u64::MAX).unwrap();
        assert!(matches!(
            merged.next(),
            Some(Err(Error::Corruption("test source")))
        ));
        assert!(merged.next().is_none());
    }
}
