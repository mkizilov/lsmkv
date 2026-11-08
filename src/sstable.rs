//! Sorted table writer. Publishing and syncing files are the caller's responsibility.

use std::io::{self, Write};

use crate::encoding::{put_u32, put_u64, put_varint};
use crate::memtable::{InternalKey, Kind};
use crate::wal::crc32c;

mod reader;
mod bloom;
pub use reader::{BlockCache, SstableIterator, SstableReader};
#[cfg(feature = "fuzzing")]
pub(crate) use reader::{fuzz_block, fuzz_filter, fuzz_footer};

const TARGET_BLOCK_SIZE: usize = 4096;
const RESTART_INTERVAL: usize = 16;
pub const FOOTER_MAGIC: u64 = 0x3156_4b4d_534c_5353;

struct BlockIndex {
    last_key: Vec<u8>,
    offset: u64,
    size: u64,
}

/// Writes strictly increasing internal keys to an SSTable stream.
pub struct SstableWriter<W: Write> {
    output: W,
    offset: u64,
    data: Vec<u8>,
    restarts: Vec<u32>,
    block_entries: usize,
    previous_bytes: Vec<u8>,
    last_key: Option<InternalKey>,
    index: Vec<BlockIndex>,
    failed: bool,
    bloom_bits_per_key: usize,
    bloom_hashes: Vec<u64>,
}

impl<W: Write> SstableWriter<W> {
    pub fn new(output: W) -> Self {
        Self {
            output,
            offset: 0,
            data: Vec::new(),
            restarts: Vec::new(),
            block_entries: 0,
            previous_bytes: Vec::new(),
            last_key: None,
            index: Vec::new(),
            failed: false,
            bloom_bits_per_key: 10,
            bloom_hashes: Vec::new(),
        }
    }

    /// Configure the filter using database options; zero disables the filter.
    pub fn with_options(output: W, options: &crate::Options) -> io::Result<Self> {
        if options.bloom_bits_per_key > 30 {
            return Err(invalid("bloom bits per key must be at most 30"));
        }
        let mut writer = Self::new(output);
        writer.bloom_bits_per_key = options.bloom_bits_per_key;
        Ok(writer)
    }

    /// Append one entry; duplicate or out-of-order internal keys are rejected.
    pub fn add(&mut self, key: &InternalKey, value: &[u8]) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "table writer failed"));
        }
        if self.last_key.as_ref().is_some_and(|last| key <= last) {
            return Err(invalid("internal keys must be strictly increasing"));
        }
        if key.kind == Kind::Delete && !value.is_empty() {
            return Err(invalid("delete entries must have empty values"));
        }
        let key_len = key.user_key.len().checked_add(9).ok_or_else(|| invalid("key too large"))?;
        if key_len.checked_add(value.len()).and_then(|n| n.checked_add(34))
            .and_then(|n| u32::try_from(n).ok()).is_none() {
            return Err(invalid("entry too large"));
        }
        let encoded = encode_key(key);
        let restart = self.block_entries % RESTART_INTERVAL == 0;
        let shared = if restart { 0 } else {
            self.previous_bytes.iter().zip(&encoded).take_while(|(a, b)| a == b).count()
        };
        let mut entry = Vec::new();
        put_varint(&mut entry, shared as u64);
        put_varint(&mut entry, (encoded.len() - shared) as u64);
        put_varint(&mut entry, value.len() as u64);
        entry.extend_from_slice(&encoded[shared..]);
        entry.extend_from_slice(value);
        let predicted = self.restarts.len().checked_add(restart as usize)
            .and_then(|n| n.checked_mul(4))
            .and_then(|n| n.checked_add(8))
            .and_then(|n| n.checked_add(self.data.len()))
            .and_then(|n| n.checked_add(entry.len()));
        if self.block_entries > 0 && predicted.is_none_or(|size| size > TARGET_BLOCK_SIZE) {
            self.flush_data()?;
            return self.add(key, value);
        }
        if restart {
            self.restarts.push(u32::try_from(self.data.len()).map_err(|_| invalid("block too large"))?);
        }
        self.data.extend_from_slice(&entry);
        self.block_entries += 1;
        self.previous_bytes = encoded;
        if self.bloom_bits_per_key != 0 && self.last_key.as_ref().is_none_or(|last| last.user_key != key.user_key) {
            self.bloom_hashes.push(bloom::hash(&key.user_key));
        }
        self.last_key = Some(key.clone());
        Ok(())
    }

    /// Finish the table and return the underlying writer; this does not fsync it.
    pub fn finish(mut self) -> io::Result<W> {
        if self.failed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "table writer failed"));
        }
        self.flush_data()?;
        let filter = bloom::build(&self.bloom_hashes, self.bloom_bits_per_key)?;
        let (filter_offset, filter_size) = self.write_block(&filter)?;
        let mut index = Vec::new();
        put_u32(&mut index, u32::try_from(self.index.len()).map_err(|_| invalid("too many blocks"))?);
        for entry in &self.index {
            put_varint(&mut index, entry.last_key.len() as u64);
            index.extend_from_slice(&entry.last_key);
            put_u64(&mut index, entry.offset);
            put_u64(&mut index, entry.size);
        }
        let (index_offset, index_size) = self.write_block(&index)?;
        let mut footer = Vec::with_capacity(40);
        put_u64(&mut footer, filter_offset);
        put_u64(&mut footer, filter_size);
        put_u64(&mut footer, index_offset);
        put_u64(&mut footer, index_size);
        put_u64(&mut footer, FOOTER_MAGIC);
        self.output.write_all(&footer)?;
        self.output.flush()?;
        Ok(self.output)
    }

    fn flush_data(&mut self) -> io::Result<()> {
        if self.block_entries == 0 {
            return Ok(());
        }
        let mut block = std::mem::take(&mut self.data);
        for &restart in &self.restarts {
            put_u32(&mut block, restart);
        }
        put_u32(&mut block, u32::try_from(self.restarts.len()).map_err(|_| invalid("too many restarts"))?);
        let (offset, size) = self.write_block(&block)?;
        self.index.push(BlockIndex { last_key: self.previous_bytes.clone(), offset, size });
        self.restarts.clear();
        self.previous_bytes.clear();
        self.block_entries = 0;
        Ok(())
    }

    fn write_block(&mut self, block: &[u8]) -> io::Result<(u64, u64)> {
        let size = u64::try_from(block.len()).map_err(|_| invalid("block too large"))?
            .checked_add(4).ok_or_else(|| invalid("block too large"))?;
        let offset = self.offset;
        let next = offset.checked_add(size).ok_or_else(|| invalid("table too large"))?;
        let result = self.output.write_all(block).and_then(|()| self.output.write_all(&crc32c(block).to_le_bytes()));
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }
        self.offset = next;
        Ok((offset, size))
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn encode_key(key: &InternalKey) -> Vec<u8> {
    let mut encoded = key.user_key.clone();
    put_u64(&mut encoded, key.sequence.0);
    encoded.push(match key.kind { Kind::Put => 1, Kind::Delete => 2 });
    encoded
}

#[cfg(test)]
mod tests {
    use super::{bloom::Bloom, encode_key, SstableWriter, FOOTER_MAGIC};
    use crate::encoding::{get_u32, get_u64, get_varint};
    use crate::memtable::{InternalKey, Kind};
    use crate::wal::crc32c;
    use std::io::{self, Write};

    fn block(bytes: &[u8], offset: u64, size: u64) -> &[u8] {
        let bytes = &bytes[offset as usize..(offset + size) as usize];
        let (data, checksum) = bytes.split_at(bytes.len() - 4);
        assert_eq!(get_u32(&mut &checksum[..]), Some(crc32c(data)));
        data
    }

    #[test]
    fn writes_indexed_checksummed_prefix_compressed_blocks() {
        let mut writer = SstableWriter::new(Vec::new());
        let mut expected = Vec::new();
        for i in 0..500u64 {
            let key = InternalKey::new(format!("shared-prefix-{i:04}").into_bytes(), 3, Kind::Put);
            let value = vec![(i % 256) as u8; 24];
            writer.add(&key, &value).unwrap();
            expected.push((encode_key(&key), value));
        }
        let large_key = InternalKey::new(b"zz-large".to_vec(), 2, Kind::Put);
        let large_value = vec![7; 5000];
        writer.add(&large_key, &large_value).unwrap();
        expected.push((encode_key(&large_key), large_value));
        let tombstone = InternalKey::new(b"zz-tombstone".to_vec(), 1, Kind::Delete);
        writer.add(&tombstone, &[]).unwrap();
        expected.push((encode_key(&tombstone), Vec::new()));
        let bytes = writer.finish().unwrap();
        let mut footer = &bytes[bytes.len() - 40..];
        let filter_offset = get_u64(&mut footer).unwrap();
        let filter_size = get_u64(&mut footer).unwrap();
        let index_offset = get_u64(&mut footer).unwrap();
        let index_size = get_u64(&mut footer).unwrap();
        assert_eq!(get_u64(&mut footer), Some(FOOTER_MAGIC));
        assert_eq!(index_offset + index_size, (bytes.len() - 40) as u64);
        assert_eq!(filter_offset + filter_size, index_offset);
        assert!(Bloom::parse(block(&bytes, filter_offset, filter_size)).unwrap().is_some());
        let mut index = block(&bytes, index_offset, index_size);
        let count = get_u32(&mut index).unwrap();
        assert!(count > 1);
        let mut actual = Vec::new();
        let mut end = 0;
        for _ in 0..count {
            let key_len = get_varint(&mut index).unwrap() as usize;
            let last_key = index[..key_len].to_vec();
            index = &index[key_len..];
            let offset = get_u64(&mut index).unwrap();
            let size = get_u64(&mut index).unwrap();
            assert_eq!(offset, end);
            end = offset + size;
            let data = block(&bytes, offset, size);
            let restart_count = get_u32(&mut &data[data.len() - 4..]).unwrap() as usize;
            let entries_end = data.len() - 4 - restart_count * 4;
            let mut restart_data = &data[entries_end..data.len() - 4];
            let mut restarts = Vec::new();
            for _ in 0..restart_count { restarts.push(get_u32(&mut restart_data).unwrap() as usize); }
            let mut input = &data[..entries_end];
            let mut previous = Vec::new();
            let mut entry_number = 0;
            while !input.is_empty() {
                let position = entries_end - input.len();
                let shared = get_varint(&mut input).unwrap() as usize;
                let suffix = get_varint(&mut input).unwrap() as usize;
                let value_len = get_varint(&mut input).unwrap() as usize;
                if entry_number % 16 == 0 {
                    assert_eq!(shared, 0);
                    assert_eq!(restarts[entry_number / 16], position);
                }
                let mut key = previous[..shared].to_vec();
                key.extend_from_slice(&input[..suffix]);
                let value = input[suffix..suffix + value_len].to_vec();
                input = &input[suffix + value_len..];
                previous = key.clone();
                actual.push((key, value));
                entry_number += 1;
            }
            assert_eq!(restarts.len(), (entry_number + 15) / 16);
            assert_eq!(previous, last_key);
        }
        assert!(index.is_empty());
        assert_eq!(end, filter_offset);
        assert_eq!(actual, expected);
    }

    #[test]
    fn empty_table_and_order_validation() {
        let bytes = SstableWriter::new(Vec::new()).finish().unwrap();
        let mut footer = &bytes[bytes.len() - 40..];
        let filter_offset = get_u64(&mut footer).unwrap();
        let filter_size = get_u64(&mut footer).unwrap();
        let index_offset = get_u64(&mut footer).unwrap();
        let index_size = get_u64(&mut footer).unwrap();
        assert!(Bloom::parse(block(&bytes, filter_offset, filter_size)).unwrap().is_some());
        assert_eq!(get_u32(&mut block(&bytes, index_offset, index_size)), Some(0));
        let mut writer = SstableWriter::new(Vec::new());
        let key = InternalKey::new(b"a".to_vec(), 2, Kind::Put);
        writer.add(&key, b"value").unwrap();
        assert!(writer.add(&key, b"duplicate").is_err());
        let newer = InternalKey::new(b"a".to_vec(), 3, Kind::Put);
        assert!(writer.add(&newer, b"out of order").is_err());
        let delete = InternalKey::new(b"a".to_vec(), 1, Kind::Delete);
        assert!(writer.add(&delete, b"invalid").is_err());
        writer.add(&delete, b"").unwrap();
        assert!(writer.finish().is_ok());
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("injected failure"))
        }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    #[test]
    fn io_failure_prevents_reuse() {
        let mut writer = SstableWriter::new(FailingWriter);
        writer.add(&InternalKey::new(b"a".to_vec(), 1, Kind::Put), &vec![0; 5000]).unwrap();
        assert!(writer.add(&InternalKey::new(b"b".to_vec(), 2, Kind::Put), b"x").is_err());
        assert!(writer.add(&InternalKey::new(b"c".to_vec(), 3, Kind::Put), b"x").is_err());
        assert!(writer.finish().is_err());
    }
}
