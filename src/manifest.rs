//! Durable append-only version edits and the CURRENT pointer.

use std::io::{self, Write};
use std::path::Path;

use crate::encoding::{get_u32, get_u64, put_u32, put_u64};
use crate::fs::{FileOptions, Fs};
use crate::wal::crc32c;
use crate::Error;

const NAME: &str = "MANIFEST-000001";
const CURRENT: &str = "MANIFEST-000001\n";

#[derive(Default)]
pub struct Version {
    pub files: Vec<(u32, u64)>,
    pub next_file: u64,
    pub last_sequence: u64,
}

pub struct Edit {
    pub added: Vec<(u32, u64)>,
    pub removed: Vec<(u32, u64)>,
    pub next_file: u64,
    pub last_sequence: u64,
}

pub fn sync_dir(path: &Path, fs: &dyn Fs) -> io::Result<()> {
    fs.sync_dir(path)
}

pub fn open(dir: &Path, fs: &dyn Fs) -> Result<Version, Error> {
    let current = dir.join("CURRENT");
    let current_exists = fs.exists(&current)?;
    if current_exists {
        if fs.read(&current)? != CURRENT.as_bytes() { return Err(Error::Corruption("CURRENT")); }
    } else {
        let manifest = dir.join(NAME);
        if !fs.exists(&manifest)? || fs.len(&manifest)? == 0 {
            let temporary = dir.join("MANIFEST-000001.tmp");
            let mut file = fs.open(&temporary, FileOptions::new().write(true).create(true).truncate(true))?;
            let initial = Edit { added: Vec::new(), removed: Vec::new(), next_file: 1, last_sequence: 0 };
            file.write_all(&encode(&initial)?)?;
            file.sync_all()?;
            fs.rename(&temporary, &manifest)?;
            sync_dir(dir, fs)?;
        }
    }
    let path = dir.join(NAME);
    let bytes = fs.read(&path)?;
    let (version, position) = parse_records(&bytes)?;
    if position != bytes.len() {
        let file = fs.open(&path, FileOptions::new().write(true))?;
        file.set_len(position as u64)?;
        file.sync_all()?;
    }
    if !current_exists {
        let temporary = dir.join("CURRENT.tmp");
        let mut file = fs.open(&temporary, FileOptions::new().write(true).create(true).truncate(true))?;
        file.write_all(CURRENT.as_bytes())?;
        file.sync_all()?;
        fs.rename(&temporary, &current)?;
        sync_dir(dir, fs)?;
    }
    Ok(version)
}

fn parse_records(bytes: &[u8]) -> Result<(Version, usize), Error> {
    let mut position = 0usize;
    let mut version = Version::default();
    while position < bytes.len() {
        let Some(header_end) = position.checked_add(12) else { return Err(Error::Corruption("manifest length")); };
        if header_end > bytes.len() { break; }
        let mut header = &bytes[position..header_end];
        let size = get_u32(&mut header).ok_or(Error::Corruption("manifest header"))? as usize;
        let header_checksum = get_u32(&mut header).ok_or(Error::Corruption("manifest header"))?;
        if crc32c(&(size as u32).to_le_bytes()) != header_checksum {
            return Err(Error::Corruption("manifest header checksum"));
        }
        let checksum = get_u32(&mut header).ok_or(Error::Corruption("manifest header"))?;
        let end = header_end.checked_add(size).ok_or(Error::Corruption("manifest length"))?;
        if end > bytes.len() { break; }
        let payload = &bytes[header_end..end];
        if crc32c(payload) != checksum { return Err(Error::Corruption("manifest checksum")); }
        apply(&mut version, decode(payload)?)?;
        position = end;
    }
    if version.next_file == 0 { return Err(Error::Corruption("empty manifest")); }
    Ok((version, position))
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_edit(bytes: &[u8]) -> Result<(), Error> { decode(bytes).map(|_| ()) }

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_log(bytes: &[u8]) -> Result<(), Error> { parse_records(bytes).map(|_| ()) }

fn decode(mut bytes: &[u8]) -> Result<Edit, Error> {
    let version = get_u32(&mut bytes).ok_or(Error::Corruption("manifest edit"))?;
    if version != 1 { return Err(Error::Corruption("manifest version")); }
    let next_file = get_u64(&mut bytes).ok_or(Error::Corruption("manifest edit"))?;
    let last_sequence = get_u64(&mut bytes).ok_or(Error::Corruption("manifest edit"))?;
    let added_count = get_u32(&mut bytes).ok_or(Error::Corruption("manifest edit"))? as usize;
    let removed_count = get_u32(&mut bytes).ok_or(Error::Corruption("manifest edit"))? as usize;
    let total = added_count.checked_add(removed_count).ok_or(Error::Corruption("manifest count"))?;
    if total.checked_mul(12) != Some(bytes.len()) { return Err(Error::Corruption("manifest count")); }
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for i in 0..total {
        let level = get_u32(&mut bytes).ok_or(Error::Corruption("manifest file"))?;
        let number = get_u64(&mut bytes).ok_or(Error::Corruption("manifest file"))?;
        if i < added_count { added.push((level, number)); } else { removed.push((level, number)); }
    }
    Ok(Edit { added, removed, next_file, last_sequence })
}

fn apply(version: &mut Version, edit: Edit) -> Result<(), Error> {
    if edit.next_file < version.next_file || edit.last_sequence < version.last_sequence || edit.next_file == 0 {
        return Err(Error::Corruption("manifest ordering"));
    }
    for file in edit.removed {
        let Some(index) = version.files.iter().position(|item| *item == file) else {
            return Err(Error::Corruption("manifest remove"));
        };
        version.files.remove(index);
    }
    for file in edit.added {
        if file.1 >= edit.next_file || version.files.contains(&file) {
            return Err(Error::Corruption("manifest add"));
        }
        version.files.push(file);
    }
    version.next_file = edit.next_file;
    version.last_sequence = edit.last_sequence;
    Ok(())
}

fn encode(edit: &Edit) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    put_u32(&mut payload, 1);
    put_u64(&mut payload, edit.next_file);
    put_u64(&mut payload, edit.last_sequence);
    put_u32(&mut payload, u32::try_from(edit.added.len()).map_err(|_| io::Error::other("manifest edit too large"))?);
    put_u32(&mut payload, u32::try_from(edit.removed.len()).map_err(|_| io::Error::other("manifest edit too large"))?);
    for &(level, number) in edit.added.iter().chain(&edit.removed) {
        put_u32(&mut payload, level);
        put_u64(&mut payload, number);
    }
    let mut record = Vec::new();
    let size = u32::try_from(payload.len()).map_err(|_| io::Error::other("manifest edit too large"))?;
    put_u32(&mut record, size);
    put_u32(&mut record, crc32c(&size.to_le_bytes()));
    put_u32(&mut record, crc32c(&payload));
    record.extend_from_slice(&payload);
    Ok(record)
}

pub fn append(dir: &Path, edit: &Edit, fs: &dyn Fs) -> io::Result<()> {
    let record = encode(edit)?;
    let mut file = fs.open(&dir.join(NAME), FileOptions::new().append(true))?;
    file.write_all(&record)?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::{append, decode, open, parse_records, Edit, NAME};
    use crate::fs::NativeFs;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".agent-local/tmp");
            fs::create_dir_all(&root).unwrap();
            let path = root.join(format!("manifest-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
    }

    #[test]
    fn edits_and_torn_tail_recover() {
        let dir = TestDir::new();
        let version = open(&dir.0, &NativeFs).unwrap();
        assert_eq!(version.next_file, 1);
        append(&dir.0, &Edit { added: vec![(0, 2)], removed: Vec::new(), next_file: 3, last_sequence: 7 }, &NativeFs).unwrap();
        append(&dir.0, &Edit { added: vec![(1, 3)], removed: vec![(0, 2)], next_file: 4, last_sequence: 7 }, &NativeFs).unwrap();
        let path = dir.0.join(NAME);
        let valid = fs::metadata(&path).unwrap().len();
        OpenOptions::new().append(true).open(&path).unwrap().write_all(&[1, 2, 3]).unwrap();
        let version = open(&dir.0, &NativeFs).unwrap();
        assert_eq!(version.files, vec![(1, 3)]);
        assert_eq!(version.next_file, 4);
        assert_eq!(version.last_sequence, 7);
        assert_eq!(fs::metadata(&path).unwrap().len(), valid);
        let mut bytes = fs::read(&path).unwrap();
        bytes[9] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(open(&dir.0, &NativeFs).is_err());
    }

    #[test]
    fn bootstrap_does_not_publish_current_for_an_invalid_manifest() {
        let dir = TestDir::new();
        let path = dir.0.join(NAME);
        fs::write(&path, [1, 2, 3]).unwrap();
        assert!(open(&dir.0, &NativeFs).is_err());
        assert!(!dir.0.join("CURRENT").exists());
        assert_eq!(fs::read(&path).unwrap(), [1, 2, 3]);
    }

    #[test]
    fn bootstrap_recovers_from_temporary_files_and_republishes_current() {
        let dir = TestDir::new();
        fs::write(dir.0.join("MANIFEST-000001.tmp"), b"partial").unwrap();
        fs::write(dir.0.join("CURRENT.tmp"), b"partial").unwrap();
        assert_eq!(open(&dir.0, &NativeFs).unwrap().next_file, 1);
        assert!(!dir.0.join("MANIFEST-000001.tmp").exists());
        assert!(!dir.0.join("CURRENT.tmp").exists());
        fs::remove_file(dir.0.join("CURRENT")).unwrap();
        assert_eq!(open(&dir.0, &NativeFs).unwrap().next_file, 1);
        assert_eq!(fs::read(dir.0.join("CURRENT")).unwrap(), b"MANIFEST-000001\n");
    }

    #[test]
    fn impossible_manifest_counts_and_torn_headers_are_rejected() {
        let mut edit = Vec::new();
        edit.extend_from_slice(&1u32.to_le_bytes());
        edit.extend_from_slice(&1u64.to_le_bytes());
        edit.extend_from_slice(&0u64.to_le_bytes());
        edit.extend_from_slice(&u32::MAX.to_le_bytes());
        edit.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&edit).is_err());
        for length in 0..12 {
            assert!(parse_records(&vec![0; length]).is_err());
        }
    }
}
