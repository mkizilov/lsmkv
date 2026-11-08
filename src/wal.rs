//! Framed, checksummed write-ahead log batches.

use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use crate::fs::{FileOptions, Fs, FsFile, NativeFs};

pub(crate) const MAX_PAYLOAD: usize = 64 * 1024 * 1024;
const BATCH_TYPE: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operation {
    Put { sequence: u64, key: Vec<u8>, value: Vec<u8> },
    Delete { sequence: u64, key: Vec<u8> },
}

#[derive(Clone, Copy, Debug)]
pub enum SyncPolicy {
    EveryWrite,
    Interval(Duration),
}

enum Command {
    Append(Vec<u8>, Sender<io::Result<()>>),
    Sync(Sender<io::Result<()>>),
    Close(Sender<io::Result<()>>),
}

/// A single-writer WAL. Close explicitly to observe final synchronization errors.
pub struct WalWriter {
    sender: Sender<Command>,
    worker: Option<JoinHandle<()>>,
}

impl WalWriter {
    /// Opens an empty or fully valid WAL for appending; damaged tails must be
    /// truncated to the reader's valid byte count before reopening.
    pub fn open(path: impl AsRef<Path>, policy: SyncPolicy) -> io::Result<Self> {
        Self::open_with_fs(path, policy, Arc::new(NativeFs))
    }

    pub fn open_with_fs(path: impl AsRef<Path>, policy: SyncPolicy, fs: Arc<dyn Fs>) -> io::Result<Self> {
        if matches!(policy, SyncPolicy::Interval(interval) if interval.is_zero() || Instant::now().checked_add(interval).is_none()) {
            return Err(invalid_input("invalid sync interval"));
        }
        let path = path.as_ref();
        let mut file = fs.open(path, FileOptions::new().read(true).append(true).create(true))?;
        let length = file.len()?;
        let mut reader = WalReader::new(&mut file);
        while reader.next_batch()?.is_some() {}
        if reader.valid_bytes() != length {
            return Err(invalid_input("WAL has a corrupt or torn tail"));
        }
        let (sender, receiver) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("lsmkv-wal-sync".into())
            .spawn(move || run_writer(file, policy, receiver))?;
        Ok(Self { sender, worker: Some(worker) })
    }

    pub fn append(&self, batch: &[Operation]) -> io::Result<()> {
        self.request(|reply| Ok(Command::Append(encode(batch)?, reply)))?
    }

    /// Synchronize all preceding writes and report any prior worker error.
    pub fn sync(&self) -> io::Result<()> {
        self.request(|reply| Ok(Command::Sync(reply)))?
    }

    pub fn close(mut self) -> io::Result<()> {
        self.finish()
    }

    fn request<F>(&self, command: F) -> io::Result<io::Result<()>>
    where
        F: FnOnce(Sender<io::Result<()>>) -> io::Result<Command>,
    {
        let (sender, receiver) = mpsc::channel();
        self.sender.send(command(sender)?).map_err(|_| broken_worker())?;
        receiver.recv().map_err(|_| broken_worker())
    }

    fn finish(&mut self) -> io::Result<()> {
        if let Some(worker) = self.worker.take() {
            let result = self.request(|reply| Ok(Command::Close(reply))).and_then(|r| r);
            worker.join().map_err(|_| broken_worker())?;
            result
        } else {
            Ok(())
        }
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            eprintln!("failed to close WAL: {error}");
        }
    }
}

fn broken_worker() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "WAL worker stopped")
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn run_writer(mut file: Box<dyn FsFile>, policy: SyncPolicy, receiver: Receiver<Command>) {
    let mut dirty = false;
    let mut deadline: Option<Instant> = None;
    let mut failure: Option<(io::ErrorKind, String)> = None;
    loop {
        if deadline.is_some_and(|at| Instant::now() >= at) {
            if let Err(error) = file.sync_all() {
                failure = Some((error.kind(), error.to_string()));
            }
            dirty = false;
            deadline = None;
        }
        let command = if let Some(at) = deadline {
            match receiver.recv_timeout(at.saturating_duration_since(Instant::now())) {
                Ok(command) => Some(command),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match receiver.recv() {
                Ok(command) => Some(command),
                Err(_) => break,
            }
        };
        if command.is_none() {
            if let Err(error) = file.sync_all() {
                failure = Some((error.kind(), error.to_string()));
            }
            dirty = false;
            deadline = None;
            continue;
        }
        let Some(command) = command else { continue };
        let (result, reply, close) = match command {
            Command::Append(bytes, reply) => {
                let result = if let Some((kind, message)) = &failure {
                    Err(io::Error::new(*kind, message.clone()))
                } else {
                    file.write_all(&bytes).and_then(|()| {
                        dirty = true;
                        match policy {
                            SyncPolicy::EveryWrite => file.sync_all().map(|()| dirty = false),
                            SyncPolicy::Interval(interval) => {
                                if deadline.is_none() {
                                    deadline = Instant::now().checked_add(interval);
                                }
                                Ok(())
                            }
                        }
                    })
                };
                (result, reply, false)
            }
            Command::Sync(reply) => {
                let result = if let Some((kind, message)) = &failure {
                    Err(io::Error::new(*kind, message.clone()))
                } else if dirty {
                    file.sync_all().map(|()| { dirty = false; deadline = None; })
                } else { Ok(()) };
                (result, reply, false)
            }
            Command::Close(reply) => {
                let result = if let Some((kind, message)) = &failure {
                    Err(io::Error::new(*kind, message.clone()))
                } else if dirty {
                    file.sync_all()
                } else { Ok(()) };
                (result, reply, true)
            }
        };
        if let Err(error) = &result {
            failure = Some((error.kind(), error.to_string()));
        }
        let _ = reply.send(result);
        if close { break; }
    }
}

fn encode(batch: &[Operation]) -> io::Result<Vec<u8>> {
    if batch.is_empty() {
        return Err(invalid_input("batch must not be empty"));
    }
    let count = u32::try_from(batch.len()).map_err(|_| invalid_input("batch too large"))?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&count.to_le_bytes());
    for operation in batch {
        let (kind, sequence, key, value): (u8, u64, &[u8], &[u8]) = match operation {
            Operation::Put { sequence, key, value } => (1, *sequence, key, value),
            Operation::Delete { sequence, key } => (2, *sequence, key, &[]),
        };
        let key_len = u32::try_from(key.len()).map_err(|_| invalid_input("key too large"))?;
        let value_len = u32::try_from(value.len()).map_err(|_| invalid_input("value too large"))?;
        let length = payload.len().checked_add(17).and_then(|n| n.checked_add(key.len()))
            .and_then(|n| n.checked_add(value.len())).ok_or_else(|| invalid_input("batch too large"))?;
        if length > MAX_PAYLOAD {
            return Err(invalid_input("batch too large"));
        }
        payload.push(kind);
        payload.extend_from_slice(&sequence.to_le_bytes());
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&value_len.to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(value);
    }
    let mut record = Vec::with_capacity(payload.len() + 9);
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.push(BATCH_TYPE);
    record.extend_from_slice(&payload);
    let checksum = crc32c(&record);
    let mut framed = Vec::with_capacity(record.len() + 4);
    framed.extend_from_slice(&checksum.to_le_bytes());
    framed.extend_from_slice(&record);
    Ok(framed)
}

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    !crc32c_extend(!0u32, bytes)
}

fn crc32c_extend(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    crc
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> Option<&'a [u8]> {
    let value = input.get(..length)?;
    *input = input.get(length..)?;
    Some(value)
}

fn decode(mut payload: &[u8]) -> Option<Vec<Operation>> {
    let count = u32::from_le_bytes(take(&mut payload, 4)?.try_into().ok()?) as usize;
    if count == 0 || count > payload.len() / 17 {
        return None;
    }
    let mut operations = Vec::new();
    for _ in 0..count {
        let kind = *take(&mut payload, 1)?.first()?;
        let sequence = u64::from_le_bytes(take(&mut payload, 8)?.try_into().ok()?);
        let key_len = u32::from_le_bytes(take(&mut payload, 4)?.try_into().ok()?) as usize;
        let value_len = u32::from_le_bytes(take(&mut payload, 4)?.try_into().ok()?) as usize;
        let key = take(&mut payload, key_len)?.to_vec();
        let value = take(&mut payload, value_len)?.to_vec();
        operations.push(match kind {
            1 => Operation::Put { sequence, key, value },
            2 if value.is_empty() => Operation::Delete { sequence, key },
            _ => return None,
        });
    }
    if payload.is_empty() { Some(operations) } else { None }
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_payload(bytes: &[u8]) -> Result<(), crate::Error> {
    decode(bytes).map(|_| ()).ok_or(crate::Error::Corruption("invalid WAL payload"))
}

/// Sequential reader that stops at the first invalid or incomplete record.
pub struct WalReader<R> {
    input: R,
    valid_bytes: u64,
    stopped: bool,
}

impl<R: Read> WalReader<R> {
    pub fn new(input: R) -> Self {
        Self { input, valid_bytes: 0, stopped: false }
    }

    pub fn valid_bytes(&self) -> u64 {
        self.valid_bytes
    }

    pub fn next_batch(&mut self) -> io::Result<Option<Vec<Operation>>> {
        if self.stopped { return Ok(None); }
        self.stopped = true;
        let mut header = [0u8; 9];
        if !read_complete(&mut self.input, &mut header)? {
            self.stopped = true;
            return Ok(None);
        }
        let checksum = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        if length > MAX_PAYLOAD || header[8] != BATCH_TYPE {
            self.stopped = true;
            return Ok(None);
        }
        let Some(payload) = read_payload(&mut self.input, length)? else {
            self.stopped = true;
            return Ok(None);
        };
        let checksum_actual = !crc32c_extend(crc32c_extend(!0u32, &header[4..]), &payload);
        let batch = if checksum_actual == checksum { decode(&payload) } else { None };
        if batch.is_none() {
            self.stopped = true;
            return Ok(None);
        }
        self.valid_bytes += 9 + length as u64;
        self.stopped = false;
        Ok(batch)
    }
}

fn read_payload(reader: &mut impl Read, length: usize) -> io::Result<Option<Vec<u8>>> {
    let mut payload = Vec::new();
    let mut buffer = [0u8; 8192];
    while payload.len() < length {
        let requested = (length - payload.len()).min(buffer.len());
        let count = match reader.read(&mut buffer[..requested]) {
            Ok(0) => return Ok(None),
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        payload.try_reserve(count).map_err(|_| io::Error::other("WAL payload allocation failed"))?;
        payload.extend_from_slice(&buffer[..count]);
    }
    Ok(Some(payload))
}

fn read_complete(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<bool> {
    match reader.read_exact(buffer) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::{crc32c, crc32c_extend, decode, Operation, SyncPolicy, WalReader, WalWriter};
    use std::fs::{self, File};
    use std::io::{self, Cursor, Read, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct TestFile(PathBuf);

    impl TestFile {
        fn new() -> Self {
            let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
            fs::create_dir_all(&dir).unwrap();
            Self(dir.join(format!("wal-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))))
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            if self.0.exists() { fs::remove_file(&self.0).unwrap(); }
        }
    }

    fn batch(sequence: u64) -> Vec<Operation> {
        vec![Operation::Put { sequence, key: sequence.to_le_bytes().to_vec(), value: vec![sequence as u8; (sequence % 31) as usize] }]
    }

    #[test]
    fn crc_known_vector_and_empty_file() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
        assert_eq!(!crc32c_extend(crc32c_extend(!0, b"1234"), b"56789"), 0xe3069283);
        let mut reader = WalReader::new(Cursor::new(Vec::<u8>::new()));
        assert_eq!(reader.next_batch().unwrap(), None);
        assert_eq!(reader.valid_bytes(), 0);
        assert_eq!(reader.next_batch().unwrap(), None);
    }

    #[test]
    fn torn_large_payload_is_read_in_bounded_chunks() {
        struct Limited(Cursor<Vec<u8>>);
        impl Read for Limited {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                if bytes.len() > 8192 { return Err(io::Error::other("unbounded read request")); }
                self.0.read(bytes)
            }
        }
        let mut header = vec![0; 4];
        header.extend_from_slice(&(64u32 * 1024 * 1024).to_le_bytes());
        header.push(1);
        let mut reader = WalReader::new(Limited(Cursor::new(header)));
        assert_eq!(reader.next_batch().unwrap(), None);
        assert_eq!(reader.valid_bytes(), 0);
    }

    #[test]
    fn impossible_wal_operation_count_is_rejected() {
        let bytes = u32::MAX.to_le_bytes();
        assert_eq!(decode(&bytes), None);
    }

    #[test]
    fn random_batches_round_trip() {
        let path = TestFile::new();
        let writer = WalWriter::open(&path.0, SyncPolicy::Interval(Duration::from_millis(5))).unwrap();
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut expected = Vec::new();
        for _ in 0..3000 {
            seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
            let mut operations = Vec::new();
            for n in 0..(seed % 5 + 1) {
                let sequence = seed.wrapping_add(n);
                let key = sequence.to_le_bytes().to_vec();
                if sequence & 1 == 0 {
                    operations.push(Operation::Put { sequence, key, value: vec![sequence as u8; (sequence % 40) as usize] });
                } else {
                    operations.push(Operation::Delete { sequence, key });
                }
            }
            writer.append(&operations).unwrap();
            expected.push(operations);
        }
        writer.sync().unwrap();
        writer.close().unwrap();
        let file = File::open(&path.0).unwrap();
        let length = file.metadata().unwrap().len();
        let mut reader = WalReader::new(file);
        for operations in expected { assert_eq!(reader.next_batch().unwrap(), Some(operations)); }
        assert_eq!(reader.next_batch().unwrap(), None);
        assert_eq!(reader.valid_bytes(), length);
    }

    #[test]
    fn truncated_and_corrupt_records_stop_at_valid_prefix() {
        let path = TestFile::new();
        let writer = WalWriter::open(&path.0, SyncPolicy::EveryWrite).unwrap();
        writer.append(&batch(1)).unwrap();
        let first_length = fs::metadata(&path.0).unwrap().len();
        writer.append(&batch(2)).unwrap();
        let second_length = fs::metadata(&path.0).unwrap().len();
        writer.append(&batch(3)).unwrap();
        writer.close().unwrap();
        let original = fs::read(&path.0).unwrap();
        for cut in 0..first_length as usize {
            let mut reader = WalReader::new(Cursor::new(&original[..cut]));
            assert_eq!(reader.next_batch().unwrap(), None);
            assert_eq!(reader.valid_bytes(), 0);
        }
        for cut in first_length as usize..original.len() {
            let mut reader = WalReader::new(Cursor::new(&original[..cut]));
            assert_eq!(reader.next_batch().unwrap(), Some(batch(1)));
            let expected = if cut as u64 >= second_length {
                assert_eq!(reader.next_batch().unwrap(), Some(batch(2)));
                second_length
            } else {
                first_length
            };
            assert_eq!(reader.next_batch().unwrap(), None);
            assert_eq!(reader.valid_bytes(), expected);
        }
        let mut damaged = original;
        damaged[first_length as usize + 10] ^= 0x40;
        let mut reader = WalReader::new(Cursor::new(&damaged));
        assert_eq!(reader.next_batch().unwrap(), Some(batch(1)));
        assert_eq!(reader.next_batch().unwrap(), None);
        assert_eq!(reader.valid_bytes(), first_length);
        assert_eq!(reader.next_batch().unwrap(), None);
        File::create(&path.0).unwrap().write_all(&damaged).unwrap();
        assert!(WalWriter::open(&path.0, SyncPolicy::EveryWrite).is_err());
    }

    #[test]
    fn invalid_input_and_reopen() {
        let path = TestFile::new();
        assert!(WalWriter::open(&path.0, SyncPolicy::Interval(Duration::ZERO)).is_err());
        let writer = WalWriter::open(&path.0, SyncPolicy::EveryWrite).unwrap();
        assert!(writer.append(&[]).is_err());
        writer.append(&batch(1)).unwrap();
        writer.close().unwrap();
        let writer = WalWriter::open(&path.0, SyncPolicy::EveryWrite).unwrap();
        writer.append(&batch(2)).unwrap();
        writer.close().unwrap();
        let mut reader = WalReader::new(File::open(&path.0).unwrap());
        assert_eq!(reader.next_batch().unwrap(), Some(batch(1)));
        assert_eq!(reader.next_batch().unwrap(), Some(batch(2)));
        assert_eq!(reader.next_batch().unwrap(), None);
    }
}
