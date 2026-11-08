//! Whole-level compaction, preserving versions visible to pinned snapshots.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::flush::table_path;
use crate::fs::{FileOptions, FsFile};
use crate::manifest::{self, Edit};
use crate::memtable::{InternalKey, Kind};
use crate::merge::Source;
use crate::sstable::{BlockCache, SstableReader, SstableWriter};
use crate::{Error, Options, Table};

pub struct Job {
    pub dir: PathBuf,
    pub inputs: Vec<Arc<Table>>,
    pub removed: Vec<(u32, u64)>,
    pub level: u32,
    pub next_file: u64,
    pub last_sequence: u64,
    pub snapshots: Vec<u64>,
    pub bottom: bool,
    pub options: Options,
    pub cache: Arc<BlockCache>,
}

pub struct Output {
    pub level: u32,
    pub files: Vec<Arc<Table>>,
    pub removed: Vec<(u32, u64)>,
    pub next_file: u64,
}

pub fn run(job: Job) -> Result<Output, Error> {
    let mut sources: Vec<Source<'_>> = job.inputs.iter().map(|table| Box::new(table.iter()) as Source<'_>).collect();
    let mut heap = BinaryHeap::new();
    for (index, source) in sources.iter_mut().enumerate() {
        if let Some(entry) = source.next() {
            let (key, value) = entry?;
            heap.push(Reverse((key, index, value)));
        }
    }
    let mut group: Vec<(InternalKey, Vec<u8>)> = Vec::new();
    let mut chunk = Vec::new();
    let mut estimate = 0u64;
    let mut files = Vec::new();
    let mut next_file = job.next_file;
    while let Some(Reverse((key, source, value))) = heap.pop() {
        if let Some(entry) = sources[source].next() {
            let (next, value) = entry?;
            heap.push(Reverse((next, source, value)));
        }
        if group.first().is_some_and(|(first, _)| first.user_key != key.user_key) {
            emit_group(&job, &mut group, &mut chunk, &mut estimate, &mut files, &mut next_file)?;
        }
        if let Some((last, last_value)) = group.last() {
            if *last == key {
                if *last_value != value { return Err(Error::Corruption("duplicate internal key")); }
                continue;
            }
        }
        group.push((key, value));
    }
    emit_group(&job, &mut group, &mut chunk, &mut estimate, &mut files, &mut next_file)?;
    if !chunk.is_empty() { write_chunk(&job, &mut chunk, &mut files, &mut next_file)?; }
    manifest::sync_dir(&job.dir, job.options.fs.as_ref())?;
    manifest::append(&job.dir, &Edit {
        added: files.iter().map(|file| (job.level, file.number)).collect(),
        removed: job.removed.clone(),
        next_file,
        last_sequence: job.last_sequence,
    }, job.options.fs.as_ref())?;
    Ok(Output { level: job.level, files, removed: job.removed, next_file })
}

fn retain_versions(group: &[(InternalKey, Vec<u8>)], snapshots: &[u64], bottom: bool) -> Vec<(InternalKey, Vec<u8>)> {
    if group.is_empty() { return Vec::new(); }
    let mut keep = vec![false; group.len()];
    keep[0] = true;
    for &snapshot in snapshots {
        if let Some(index) = group.iter().position(|(key, _)| key.sequence.0 <= snapshot) {
            keep[index] = true;
        }
    }
    if bottom {
        let mut older_put = false;
        for index in (0..group.len()).rev() {
            if !keep[index] { continue; }
            if group[index].0.kind == Kind::Delete && !older_put { keep[index] = false; }
            if group[index].0.kind == Kind::Put { older_put = true; }
        }
    }
    group.iter().zip(keep).filter_map(|(entry, keep)| keep.then(|| entry.clone())).collect()
}

fn emit_group(
    job: &Job,
    group: &mut Vec<(InternalKey, Vec<u8>)>,
    chunk: &mut Vec<(InternalKey, Vec<u8>)>,
    estimate: &mut u64,
    files: &mut Vec<Arc<Table>>,
    next_file: &mut u64,
) -> Result<(), Error> {
    let retained = retain_versions(group, &job.snapshots, job.bottom);
    group.clear();
    let size = retained.iter().fold(0u64, |total, (key, value)| {
        total.saturating_add(key.user_key.len() as u64).saturating_add(value.len() as u64).saturating_add(32)
    });
    if !chunk.is_empty() && estimate.saturating_add(size) > job.options.target_file_size {
        write_chunk(job, chunk, files, next_file)?;
        *estimate = 0;
    }
    chunk.extend(retained);
    *estimate = estimate.saturating_add(size);
    Ok(())
}

fn write_chunk(job: &Job, chunk: &mut Vec<(InternalKey, Vec<u8>)>, files: &mut Vec<Arc<Table>>, next_file: &mut u64) -> Result<(), Error> {
    let number = *next_file;
    *next_file = next_file.checked_add(1).ok_or(Error::SequenceExhausted)?;
    let temporary = job.dir.join(format!("SST-{number:020}.tmp"));
    let path = table_path(&job.dir, number);
    let file = job.options.fs.open(&temporary, FileOptions::new().write(true).create_new(true))?;
    let result = write_output(job, file, &temporary, &path, number, chunk);
    if result.is_err() {
        if let Err(error) = job.options.fs.remove_file(&temporary) {
            if error.kind() != std::io::ErrorKind::NotFound { eprintln!("failed to remove incomplete compaction output: {error}"); }
        }
    }
    files.push(result?);
    chunk.clear();
    Ok(())
}

fn write_output(job: &Job, file: Box<dyn FsFile>, temporary: &Path, path: &Path, number: u64, chunk: &[(InternalKey, Vec<u8>)]) -> Result<Arc<Table>, Error> {
    let mut writer = SstableWriter::with_options(file, &job.options)?;
    for (key, value) in chunk { writer.add(key, value)?; }
    let file = writer.finish()?;
    file.sync_all()?;
    let size = file.len()?;
    job.options.statistics.record_level(job.level, size)?;
    job.options.fs.rename(temporary, path)?;
    let reader = SstableReader::open(path, Arc::clone(&job.cache))?;
    Ok(Arc::new(Table { reader, number, path: path.to_path_buf(), size, obsolete: AtomicBool::new(false), fs: Arc::clone(&job.options.fs) }))
}

#[cfg(test)]
mod tests {
    use super::retain_versions;
    use crate::memtable::{InternalKey, Kind};

    fn entry(sequence: u64, kind: Kind) -> (InternalKey, Vec<u8>) {
        (InternalKey::new(b"key".to_vec(), sequence, kind), if kind == Kind::Put { vec![sequence as u8] } else { Vec::new() })
    }

    #[test]
    fn retains_only_versions_needed_by_snapshots() {
        let group = vec![entry(10, Kind::Put), entry(8, Kind::Put), entry(6, Kind::Delete), entry(4, Kind::Put), entry(2, Kind::Put)];
        let retained = retain_versions(&group, &[3, 7], false);
        assert_eq!(retained, vec![entry(10, Kind::Put), entry(6, Kind::Delete), entry(2, Kind::Put)]);
        assert_eq!(retain_versions(&group, &[], false), vec![entry(10, Kind::Put)]);
    }

    #[test]
    fn bottom_tombstone_is_kept_only_when_it_hides_retained_older_values() {
        let group = vec![entry(10, Kind::Delete), entry(5, Kind::Put)];
        assert!(retain_versions(&group, &[], true).is_empty());
        assert_eq!(retain_versions(&group, &[], false), vec![entry(10, Kind::Delete)]);
        assert_eq!(retain_versions(&group, &[5], true), group);
    }
}
