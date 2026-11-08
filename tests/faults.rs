use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lsmkv::fs::{FileOptions, Fs, FsFile, NativeFs};
use lsmkv::{Db, Options};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "fault-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    Write,
    TornWrite,
    Sync,
}

#[derive(Debug, Default)]
struct FaultFs {
    native: NativeFs,
    fault: Arc<Mutex<Option<(String, Fault)>>>,
    opened: Mutex<Vec<PathBuf>>,
}

impl FaultFs {
    fn arm(&self, fault: Fault) {
        self.arm_on("WAL", fault);
    }
    fn arm_on(&self, prefix: &str, fault: Fault) {
        *self.fault.lock().unwrap() = Some((prefix.to_owned(), fault));
    }

    fn truncate(&self, path: &Path, length: u64) -> io::Result<()> {
        self.native
            .open(path, FileOptions::new().write(true))?
            .set_len(length)
    }
}

struct FaultFile {
    inner: Box<dyn FsFile>,
    path: PathBuf,
    fault: Arc<Mutex<Option<(String, Fault)>>>,
}

impl FaultFile {
    fn take_fault(&self, allowed: &[Fault]) -> Option<Fault> {
        let mut fault = self.fault.lock().unwrap();
        let selected = fault.as_ref().is_some_and(|(prefix, value)| {
            allowed.contains(value)
                && self
                    .path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(prefix.as_str()))
        });
        if selected {
            fault.take().map(|(_, value)| value)
        } else {
            None
        }
    }
}

impl Read for FaultFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}
impl Write for FaultFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.take_fault(&[Fault::Write, Fault::TornWrite]) {
            Some(Fault::Write) => Err(io::Error::other("injected write failure")),
            Some(Fault::TornWrite) => {
                self.inner.write_all(&buf[..buf.len().min(5)])?;
                Err(io::Error::other("injected torn write"))
            }
            _ => self.inner.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl Seek for FaultFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.inner.seek(position)
    }
}
impl FsFile for FaultFile {
    fn sync_all(&self) -> io::Result<()> {
        if self.take_fault(&[Fault::Sync]).is_some() {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.inner.sync_all()
    }
    fn set_len(&self, length: u64) -> io::Result<()> {
        self.inner.set_len(length)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn try_lock_exclusive(&self) -> io::Result<()> {
        self.inner.try_lock_exclusive()
    }
}

impl Fs for FaultFs {
    fn open(&self, path: &Path, options: FileOptions) -> io::Result<Box<dyn FsFile>> {
        self.opened.lock().unwrap().push(path.to_path_buf());
        Ok(Box::new(FaultFile {
            inner: self.native.open(path, options)?,
            path: path.to_path_buf(),
            fault: Arc::clone(&self.fault),
        }))
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.native.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.native.read_dir(path)
    }
    fn len(&self, path: &Path) -> io::Result<u64> {
        self.native.len(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.native.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.native.remove_file(path)
    }
}

#[test]
fn wal_write_torn_write_and_fsync_failures_recover() {
    for fault in [Fault::Write, Fault::TornWrite, Fault::Sync] {
        let dir = TestDir::new();
        let fs = Arc::new(FaultFs::default());
        let options = Options {
            fs: fs.clone(),
            ..Options::default()
        };
        {
            let db = Db::open(&dir.0, options).unwrap();
            db.put(b"before", b"durable").unwrap();
            fs.arm(fault);
            assert!(db.put(b"failed", b"value").is_err());
        }
        let db = Db::open(&dir.0, Options::default()).unwrap();
        assert_eq!(db.get(b"before").unwrap(), Some(b"durable".to_vec()));
        let failed = db.get(b"failed").unwrap();
        if fault == Fault::Sync {
            assert!(failed.is_none() || failed == Some(b"value".to_vec()));
        } else {
            assert_eq!(failed, None);
        }
        db.put(b"after", b"recoverable").unwrap();
        drop(db);
        let db = Db::open(&dir.0, Options::default()).unwrap();
        assert_eq!(db.get(b"after").unwrap(), Some(b"recoverable".to_vec()));
    }
}

#[test]
fn explicit_truncation_recovers_valid_wal_prefix() {
    let dir = TestDir::new();
    let fs = Arc::new(FaultFs::default());
    {
        let db = Db::open(
            &dir.0,
            Options {
                fs: fs.clone(),
                ..Options::default()
            },
        )
        .unwrap();
        db.put(b"first", b"durable").unwrap();
        db.put(b"second", b"torn").unwrap();
    }
    let wal = dir.0.join("WAL");
    let length = fs.len(&wal).unwrap();
    fs.truncate(&wal, length - 1).unwrap();
    let db = Db::open(&dir.0, Options::default()).unwrap();
    assert_eq!(db.get(b"first").unwrap(), Some(b"durable".to_vec()));
    assert_eq!(db.get(b"second").unwrap(), None);
}

#[test]
fn injected_filesystem_observes_wal_manifest_and_sstable_operations() {
    let dir = TestDir::new();
    let fs = Arc::new(FaultFs::default());
    let options = Options {
        fs: fs.clone(),
        memtable_size: 1024,
        ..Options::default()
    };
    {
        let db = Db::open(&dir.0, options.clone()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
    }
    let db = Db::open(&dir.0, options).unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    let opened = fs.opened.lock().unwrap();
    for name in ["LOCK", "CURRENT", "MANIFEST-000001", "WAL", "SST-"] {
        assert!(
            opened.iter().any(|path| path
                .file_name()
                .is_some_and(|value| value.to_string_lossy().starts_with(name))),
            "missing {name}"
        );
    }
}

#[test]
fn sstable_and_manifest_failures_leave_wal_recoverable() {
    for (prefix, fault) in [
        ("SST-", Fault::Write),
        ("MANIFEST-", Fault::TornWrite),
        ("MANIFEST-", Fault::Sync),
    ] {
        let dir = TestDir::new();
        let fs = Arc::new(FaultFs::default());
        {
            let db = Db::open(
                &dir.0,
                Options {
                    fs: fs.clone(),
                    ..Options::default()
                },
            )
            .unwrap();
            db.put(b"key", b"durable").unwrap();
            fs.arm_on(prefix, fault);
            assert!(db.flush().is_err());
        }
        let db = Db::open(&dir.0, Options::default()).unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"durable".to_vec()));
        db.put(b"after", b"recovered").unwrap();
        drop(db);
        let db = Db::open(&dir.0, Options::default()).unwrap();
        assert_eq!(db.get(b"after").unwrap(), Some(b"recovered".to_vec()));
    }
}
