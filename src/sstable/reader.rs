use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::encoding::{get_u32, get_u64, get_varint};
use crate::fs::{FileOptions, Fs, FsFile};
use crate::memtable::{InternalKey, Kind};
use crate::wal::crc32c;
use crate::{Error, Options, Statistics};

use super::FOOTER_MAGIC;
use super::bloom::Bloom;

static NEXT_TABLE: AtomicU64 = AtomicU64::new(1);
type CacheKey = (u64, u64);

struct CacheState {
    blocks: HashMap<CacheKey, Arc<Block>>,
    order: VecDeque<CacheKey>,
    bytes: usize,
}

/// A byte-bounded LRU cache that can be shared by multiple table readers.
pub struct BlockCache {
    capacity: usize,
    state: Mutex<CacheState>,
    statistics: Arc<Statistics>,
    fs: Arc<dyn Fs>,
}

impl BlockCache {
    pub fn new(options: &Options) -> Self {
        Self {
            capacity: options.block_cache_capacity,
            state: Mutex::new(CacheState { blocks: HashMap::new(), order: VecDeque::new(), bytes: 0 }),
            statistics: Arc::clone(&options.statistics),
            fs: Arc::clone(&options.fs),
        }
    }

    fn contains(&self, key: CacheKey) -> Result<bool, Error> {
        Ok(self.state.lock().map_err(|_| Error::Poisoned)?.blocks.contains_key(&key))
    }

    fn get(&self, key: CacheKey) -> Result<Option<Arc<Block>>, Error> {
        let mut state = self.state.lock().map_err(|_| Error::Poisoned)?;
        let block = state.blocks.get(&key).cloned();
        if block.is_some() {
            self.statistics.cache_hits.fetch_add(1, Ordering::Relaxed);
            state.order.retain(|item| *item != key);
            state.order.push_back(key);
        } else {
            self.statistics.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
        Ok(block)
    }

    fn insert(&self, key: CacheKey, block: Arc<Block>) -> Result<(), Error> {
        let size = block.memory_usage();
        if size > self.capacity || self.capacity == 0 { return Ok(()); }
        let mut state = self.state.lock().map_err(|_| Error::Poisoned)?;
        if state.blocks.contains_key(&key) {
            state.order.retain(|item| *item != key);
            state.order.push_back(key);
            return Ok(());
        }
        while state.bytes > self.capacity - size {
            let Some(oldest) = state.order.pop_front() else { break };
            if let Some(removed) = state.blocks.remove(&oldest) {
                state.bytes -= removed.memory_usage();
            }
        }
        state.bytes += size;
        state.order.push_back(key);
        state.blocks.insert(key, block);
        Ok(())
    }
}

struct IndexEntry {
    last: InternalKey,
    offset: u64,
    size: u64,
}

struct Block {
    data: Vec<u8>,
    entries_end: usize,
    restarts: Vec<usize>,
}

impl Block {
    fn memory_usage(&self) -> usize {
        self.data.capacity().saturating_add(
            self.restarts.capacity().saturating_mul(std::mem::size_of::<usize>()),
        )
    }

    fn entry(&self, offset: usize, previous: &[u8]) -> Result<(usize, Vec<u8>, InternalKey, Vec<u8>), Error> {
        let mut input = self.data.get(offset..self.entries_end).ok_or(Error::Corruption("entry offset"))?;
        let shared = length(&mut input)?;
        let suffix = length(&mut input)?;
        let value_len = length(&mut input)?;
        if shared > previous.len() { return Err(Error::Corruption("shared key prefix")); }
        let suffix_bytes = take(&mut input, suffix)?;
        let value = take(&mut input, value_len)?.to_vec();
        let mut key_bytes = previous[..shared].to_vec();
        key_bytes.extend_from_slice(suffix_bytes);
        let key = decode_key(&key_bytes)?;
        if key.kind == Kind::Delete && !value.is_empty() {
            return Err(Error::Corruption("delete value"));
        }
        Ok((self.entries_end - input.len(), key_bytes, key, value))
    }

    // Locate the greatest restart key not greater than the target.
    fn restart_for(&self, target: &InternalKey) -> Result<usize, Error> {
        let mut lo = 0;
        let mut hi = self.restarts.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (_, _, key, _) = self.entry(self.restarts[mid], &[])?;
            if key <= *target { lo = mid + 1; } else { hi = mid; }
        }
        Ok(self.restarts[lo.saturating_sub(1)])
    }
}

/// An immutable SSTable; its underlying file must not be modified after opening.
pub struct SstableReader {
    file: Mutex<Box<dyn FsFile>>,
    index: Vec<IndexEntry>,
    cache: Arc<BlockCache>,
    id: u64,
    filter: Option<Bloom>,
    first_user_key: Option<Vec<u8>>,
    last_user_key: Option<Vec<u8>>,
}

impl SstableReader {
    /// Validate the complete file, then load its index. Share `cache` among tables.
    pub fn open(path: impl AsRef<Path>, cache: Arc<BlockCache>) -> Result<Self, Error> {
        let mut file = cache.fs.open(path.as_ref(), FileOptions::new().read(true))?;
        let file_len = file.len()?;
        if file_len < 48 { return Err(Error::Corruption("short footer")); }
        let footer_start = file_len - 40;
        file.seek(SeekFrom::Start(footer_start))?;
        let mut footer = [0; 40];
        read_exact(file.as_mut(), &mut footer)?;
        let Footer { filter_offset, filter_size, index_offset, index_size } = decode_footer(&footer, file_len)?;
        let filter_bytes = read_block(file.as_mut(), filter_offset, filter_size, footer_start)?;
        let filter = Bloom::parse(&filter_bytes)?;
        let index_bytes = read_block(file.as_mut(), index_offset, index_size, footer_start)?;
        let mut input = index_bytes.as_slice();
        let count = get_u32(&mut input).ok_or(Error::Corruption("index count"))? as usize;
        if count > input.len() / 26 { return Err(Error::Corruption("index count")); }
        let mut index = Vec::new();
        let mut offset = 0u64;
        let mut previous: Option<InternalKey> = None;
        let mut first_user_key = None;
        for _ in 0..count {
            let key_len = length(&mut input)?;
            let last = decode_key(take(&mut input, key_len)?)?;
            let block_offset = get_u64(&mut input).ok_or(Error::Corruption("index offset"))?;
            let size = get_u64(&mut input).ok_or(Error::Corruption("index size"))?;
            if block_offset != offset || size < 12 { return Err(Error::Corruption("index layout")); }
            offset = block_offset.checked_add(size).ok_or(Error::Corruption("index overflow"))?;
            if offset > filter_offset { return Err(Error::Corruption("data block range")); }
            let raw = read_block(file.as_mut(), block_offset, size, filter_offset)?;
            let (block, first, actual_last) = parse_block(raw)?;
            if first_user_key.is_none() { first_user_key = Some(first.user_key.clone()); }
            if actual_last != last || previous.as_ref().is_some_and(|key| *key >= first) {
                return Err(Error::Corruption("index key order"));
            }
            if let Some(filter) = &filter {
                let mut position = 0;
                let mut previous_bytes = Vec::new();
                while position < block.entries_end {
                    let (next, bytes, key, _) = block.entry(position, &previous_bytes)?;
                    if !filter.may_contain(&key.user_key) {
                        return Err(Error::Corruption("Bloom filter missing key"));
                    }
                    position = next;
                    previous_bytes = bytes;
                }
            }
            previous = Some(last.clone());
            index.push(IndexEntry { last, offset: block_offset, size });
        }
        if !input.is_empty() || offset != filter_offset { return Err(Error::Corruption("index trailing data")); }
        let id = NEXT_TABLE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::Corruption("table identifier exhausted"))?;
        let last_user_key = previous.map(|key| key.user_key);
        Ok(Self { file: Mutex::new(file), index, cache, id, filter, first_user_key, last_user_key })
    }

    fn block(&self, number: usize) -> Result<Arc<Block>, Error> {
        let entry = self.index.get(number).ok_or(Error::Corruption("block index"))?;
        let key = (self.id, entry.offset);
        if let Some(block) = self.cache.get(key)? { return Ok(block); }
        let mut file = self.file.lock().map_err(|_| Error::Poisoned)?;
        let raw = read_block(file.as_mut(), entry.offset, entry.size, entry.offset.checked_add(entry.size).ok_or(Error::Corruption("block range"))?)?;
        let (block, _, last) = parse_block(raw)?;
        if last != entry.last { return Err(Error::Corruption("index key mismatch")); }
        let block = Arc::new(block);
        self.cache.insert(key, Arc::clone(&block))?;
        Ok(block)
    }

    /// Find an exact internal key using the index and restart-point binary searches.
    pub fn get(&self, target: &InternalKey) -> Result<Option<Vec<u8>>, Error> {
        if !self.may_read(target)? { return Ok(None); }
        let mut iterator = self.iter();
        iterator.seek(target)?;
        match iterator.next() {
            Some(Ok((key, value))) if key == *target => Ok(Some(value)),
            Some(Err(error)) => Err(error),
            _ => Ok(None),
        }
    }

    /// Find the newest internal version for a user key, including tombstones.
    pub fn get_user(&self, user_key: &[u8]) -> Result<Option<(Kind, Vec<u8>)>, Error> {
        self.get_user_at(user_key, u64::MAX)
    }

    /// Find the newest version visible at a sequence, including tombstones.
    pub fn get_user_at(&self, user_key: &[u8], sequence: u64) -> Result<Option<(Kind, Vec<u8>)>, Error> {
        let target = InternalKey::new(user_key.to_vec(), sequence, Kind::Put);
        if !self.may_read(&target)? { return Ok(None); }
        let mut iterator = self.iter();
        iterator.seek(&target)?;
        match iterator.next() {
            Some(Ok((key, value))) if key.user_key == user_key => Ok(Some((key.kind, value))),
            Some(Err(error)) => Err(error),
            _ => Ok(None),
        }
    }

    /// Inclusive user-key bounds; empty tables have no bounds.
    pub fn user_key_bounds(&self) -> Option<(&[u8], &[u8])> {
        Some((self.first_user_key.as_deref()?, self.last_user_key.as_deref()?))
    }

    fn may_read(&self, target: &InternalKey) -> Result<bool, Error> {
        let number = self.index.partition_point(|entry| entry.last < *target);
        let Some(entry) = self.index.get(number) else { return Ok(false); };
        if self.filter.as_ref().is_some_and(|filter| !filter.may_contain(&target.user_key)) {
            if !self.cache.contains((self.id, entry.offset))? {
                self.cache.statistics.bloom_filter_block_reads_avoided.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(false);
        }
        Ok(true)
    }

    pub fn iter(&self) -> SstableIterator<'_> {
        SstableIterator { table: self, number: 0, block: None, offset: 0, previous: Vec::new(), failed: false }
    }
}

struct Footer {
    filter_offset: u64,
    filter_size: u64,
    index_offset: u64,
    index_size: u64,
}

fn decode_footer(bytes: &[u8], file_len: u64) -> Result<Footer, Error> {
    if bytes.len() != 40 || file_len < 48 { return Err(Error::Corruption("short footer")); }
    let mut input = bytes;
    let filter_offset = get_u64(&mut input).ok_or(Error::Corruption("footer"))?;
    let filter_size = get_u64(&mut input).ok_or(Error::Corruption("footer"))?;
    let index_offset = get_u64(&mut input).ok_or(Error::Corruption("footer"))?;
    let index_size = get_u64(&mut input).ok_or(Error::Corruption("footer"))?;
    if get_u64(&mut input) != Some(FOOTER_MAGIC) || filter_size < 4
        || filter_offset.checked_add(filter_size) != Some(index_offset)
        || index_offset.checked_add(index_size) != Some(file_len - 40) {
        return Err(Error::Corruption("footer layout"));
    }
    Ok(Footer { filter_offset, filter_size, index_offset, index_size })
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_footer(bytes: &[u8]) -> Result<(), Error> {
    let mut input = bytes;
    let file_len = get_u64(&mut input).ok_or(Error::Corruption("footer length"))?;
    decode_footer(input, file_len).map(|_| ())
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_block(bytes: &[u8]) -> Result<(), Error> { parse_block(bytes.to_vec()).map(|_| ()) }

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_filter(bytes: &[u8]) -> Result<(), Error> { Bloom::parse(bytes).map(|_| ()) }

/// Forward iterator over internal keys in ascending internal-key order.
pub struct SstableIterator<'a> {
    table: &'a SstableReader,
    number: usize,
    block: Option<Arc<Block>>,
    offset: usize,
    previous: Vec<u8>,
    failed: bool,
}

impl SstableIterator<'_> {
    /// Position at the first internal key greater than or equal to `target`.
    pub fn seek(&mut self, target: &InternalKey) -> Result<(), Error> {
        self.failed = false;
        self.number = self.table.index.partition_point(|entry| entry.last < *target);
        self.block = None;
        self.offset = 0;
        self.previous.clear();
        if self.number == self.table.index.len() { return Ok(()); }
        let block = self.table.block(self.number)?;
        let mut offset = block.restart_for(target)?;
        let mut previous = Vec::new();
        while offset < block.entries_end {
            let (next, bytes, key, _) = block.entry(offset, &previous)?;
            if key >= *target { break; }
            offset = next;
            previous = bytes;
        }
        self.block = Some(block);
        self.offset = offset;
        self.previous = previous;
        Ok(())
    }
}

impl Iterator for SstableIterator<'_> {
    type Item = Result<(InternalKey, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed { return None; }
        loop {
            if self.number >= self.table.index.len() { return None; }
            if self.block.is_none() {
                match self.table.block(self.number) {
                    Ok(block) => self.block = Some(block),
                    Err(error) => { self.failed = true; return Some(Err(error)); }
                }
            }
            let Some(block) = &self.block else { return None };
            if self.offset == block.entries_end {
                self.number += 1;
                self.block = None;
                self.offset = 0;
                self.previous.clear();
                continue;
            }
            match block.entry(self.offset, &self.previous) {
                Ok((next, bytes, key, value)) => {
                    self.offset = next;
                    self.previous = bytes;
                    return Some(Ok((key, value)));
                }
                Err(error) => { self.failed = true; return Some(Err(error)); }
            }
        }
    }
}

fn decode_key(bytes: &[u8]) -> Result<InternalKey, Error> {
    let split = bytes.len().checked_sub(9).ok_or(Error::Corruption("short internal key"))?;
    let mut tail = &bytes[split..];
    let sequence = get_u64(&mut tail).ok_or(Error::Corruption("internal sequence"))?;
    let kind = match tail {
        [1] => Kind::Put,
        [2] => Kind::Delete,
        _ => return Err(Error::Corruption("internal kind")),
    };
    Ok(InternalKey::new(bytes[..split].to_vec(), sequence, kind))
}

fn length(input: &mut &[u8]) -> Result<usize, Error> {
    let value = get_varint(input).ok_or(Error::Corruption("invalid varint"))?;
    usize::try_from(value).map_err(|_| Error::Corruption("length overflow"))
}

fn take<'a>(input: &mut &'a [u8], size: usize) -> Result<&'a [u8], Error> {
    let result = input.get(..size).ok_or(Error::Corruption("truncated entry"))?;
    *input = input.get(size..).ok_or(Error::Corruption("truncated entry"))?;
    Ok(result)
}

fn read_exact(file: &mut dyn FsFile, bytes: &mut [u8]) -> Result<(), Error> {
    file.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof { Error::Corruption("truncated file") }
        else { Error::Io(error) }
    })
}

fn read_block(file: &mut dyn FsFile, offset: u64, size: u64, limit: u64) -> Result<Vec<u8>, Error> {
    if size < 4 || offset.checked_add(size).is_none_or(|end| end > limit) {
        return Err(Error::Corruption("block bounds"));
    }
    let size = usize::try_from(size).map_err(|_| Error::Corruption("block size"))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(|_| Error::Io(io::Error::other("block allocation failed")))?;
    bytes.resize(size, 0);
    file.seek(SeekFrom::Start(offset))?;
    read_exact(file, &mut bytes)?;
    let payload_len = bytes.len() - 4;
    let mut checksum_bytes = &bytes[payload_len..];
    let checksum = get_u32(&mut checksum_bytes).ok_or(Error::Corruption("block checksum"))?;
    if crc32c(&bytes[..payload_len]) != checksum { return Err(Error::Corruption("block checksum")); }
    bytes.truncate(payload_len);
    Ok(bytes)
}

fn parse_block(data: Vec<u8>) -> Result<(Block, InternalKey, InternalKey), Error> {
    let tail = data.len().checked_sub(4).ok_or(Error::Corruption("restart count"))?;
    let mut count_bytes = &data[tail..];
    let count = get_u32(&mut count_bytes).ok_or(Error::Corruption("restart count"))? as usize;
    let bytes = count.checked_mul(4).ok_or(Error::Corruption("restart count"))?;
    let entries_end = tail.checked_sub(bytes).ok_or(Error::Corruption("restart array"))?;
    if count == 0 || entries_end == 0 { return Err(Error::Corruption("empty data block")); }
    let mut restart_bytes = &data[entries_end..tail];
    let mut restarts = Vec::new();
    for _ in 0..count {
        restarts.push(get_u32(&mut restart_bytes).ok_or(Error::Corruption("restart array"))? as usize);
    }
    let block = Block { data, entries_end, restarts };
    let mut position = 0;
    let mut previous_bytes = Vec::new();
    let mut previous_key: Option<InternalKey> = None;
    let mut first = None;
    let mut entries = 0usize;
    while position < entries_end {
        if entries % 16 == 0 {
            if block.restarts.get(entries / 16) != Some(&position) {
                return Err(Error::Corruption("restart position"));
            }
            let mut input = &block.data[position..entries_end];
            if length(&mut input)? != 0 { return Err(Error::Corruption("restart prefix")); }
        }
        let (next, bytes, key, _) = block.entry(position, &previous_bytes)?;
        if next <= position || previous_key.as_ref().is_some_and(|last| *last >= key) {
            return Err(Error::Corruption("data key order"));
        }
        if first.is_none() { first = Some(key.clone()); }
        previous_key = Some(key);
        previous_bytes = bytes;
        position = next;
        entries += 1;
    }
    if entries.div_ceil(16) != count { return Err(Error::Corruption("restart count")); }
    Ok((block, first.ok_or(Error::Corruption("empty block"))?, previous_key.ok_or(Error::Corruption("empty block"))?))
}

#[cfg(test)]
mod tests {
    use super::{decode_footer, parse_block, Block, BlockCache, FOOTER_MAGIC};
    use crate::encoding::put_u64;
    use crate::Options;
    use std::sync::Arc;

    #[test]
    fn cache_evicts_lru_and_separates_table_ids() {
        let cache = BlockCache::new(&Options { block_cache_capacity: 8, ..Options::default() });
        let block = || Arc::new(Block { data: vec![0; 4], entries_end: 0, restarts: Vec::new() });
        cache.insert((1, 0), block()).unwrap();
        cache.insert((2, 0), block()).unwrap();
        assert!(cache.get((1, 0)).unwrap().is_some());
        cache.insert((1, 4), block()).unwrap();
        assert!(cache.get((2, 0)).unwrap().is_none());
        assert!(cache.get((1, 0)).unwrap().is_some());
        assert!(cache.get((1, 4)).unwrap().is_some());
        assert_eq!(cache.state.lock().unwrap().bytes, 8);
    }

    #[test]
    fn disabled_and_oversized_blocks_are_not_cached() {
        let block = Arc::new(Block { data: vec![0; 4], entries_end: 0, restarts: Vec::new() });
        for capacity in [0, 3] {
            let cache = BlockCache::new(&Options { block_cache_capacity: capacity, ..Options::default() });
            cache.insert((1, 0), Arc::clone(&block)).unwrap();
            assert!(cache.get((1, 0)).unwrap().is_none());
        }
    }

    #[test]
    fn malformed_footer_and_restart_counts_are_rejected() {
        for length in 0..40 {
            assert!(decode_footer(&vec![0; length], 48).is_err());
        }
        let mut footer = Vec::new();
        for value in [0, 4, 4, 8, FOOTER_MAGIC] { put_u64(&mut footer, value); }
        assert!(decode_footer(&footer, 52).is_ok());
        for index in 0..footer.len() {
            let mut damaged = footer.clone();
            damaged[index] ^= 1;
            assert!(decode_footer(&damaged, 52).is_err());
        }
        assert!(parse_block(u32::MAX.to_le_bytes().to_vec()).is_err());
        assert!(parse_block(vec![0x80, 0x80, 0x80, 0xff, 0xff, 0xff, 0xff]).is_err());
    }
}
