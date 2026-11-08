//! Injectable filesystem boundary used by database storage.

use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;

#[derive(Clone, Copy, Debug, Default)]
pub struct FileOptions {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
}

impl FileOptions {
    pub fn new() -> Self { Self::default() }
    pub fn read(mut self, value: bool) -> Self { self.read = value; self }
    pub fn write(mut self, value: bool) -> Self { self.write = value; self }
    pub fn append(mut self, value: bool) -> Self { self.append = value; self }
    pub fn create(mut self, value: bool) -> Self { self.create = value; self }
    pub fn create_new(mut self, value: bool) -> Self { self.create_new = value; self }
    pub fn truncate(mut self, value: bool) -> Self { self.truncate = value; self }
}

pub trait FsFile: Read + Write + Seek + Send + Sync {
    fn sync_all(&self) -> io::Result<()>;
    fn set_len(&self, length: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> { Ok(self.len()? == 0) }
    fn try_lock_exclusive(&self) -> io::Result<()>;
}

pub trait Fs: Debug + Send + Sync {
    fn open(&self, path: &Path, options: FileOptions) -> io::Result<Box<dyn FsFile>>;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
    fn len(&self, path: &Path) -> io::Result<u64>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    fn exists(&self, path: &Path) -> io::Result<bool> {
        match self.len(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut file = self.open(path, FileOptions::new().read(true))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        self.open(path, FileOptions::new().read(true))?.sync_all()
    }
}

#[derive(Debug, Default)]
pub struct NativeFs;

impl Fs for NativeFs {
    fn open(&self, path: &Path, options: FileOptions) -> io::Result<Box<dyn FsFile>> {
        let file = OpenOptions::new().read(options.read).write(options.write)
            .append(options.append).create(options.create).create_new(options.create_new)
            .truncate(options.truncate).open(path)?;
        Ok(Box::new(NativeFile(file)))
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> { fs::create_dir_all(path) }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(path)?.map(|entry| entry.map(|entry| entry.path())).collect()
    }
    fn len(&self, path: &Path) -> io::Result<u64> { Ok(fs::metadata(path)?.len()) }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> { fs::rename(from, to) }
    fn remove_file(&self, path: &Path) -> io::Result<()> { fs::remove_file(path) }
}

struct NativeFile(File);

impl Read for NativeFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.0.read(buf) }
}
impl Write for NativeFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { self.0.write(buf) }
    fn flush(&mut self) -> io::Result<()> { self.0.flush() }
}
impl Seek for NativeFile {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> { self.0.seek(position) }
}
impl FsFile for NativeFile {
    fn sync_all(&self) -> io::Result<()> { self.0.sync_all() }
    fn set_len(&self, length: u64) -> io::Result<()> { self.0.set_len(length) }
    fn len(&self) -> io::Result<u64> { Ok(self.0.metadata()?.len()) }
    fn try_lock_exclusive(&self) -> io::Result<()> { self.0.try_lock_exclusive() }
}
