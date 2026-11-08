use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::manifest::{self, Edit};
use crate::memtable::BTreeMemtable;
use crate::sstable::{BlockCache, SstableReader, SstableWriter};
use crate::{Error, Options};

pub fn table_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("SST-{number:020}.sst"))
}

pub fn wal_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("WAL-{number:020}"))
}

pub struct FlushJob {
    pub dir: PathBuf,
    pub table: Arc<BTreeMemtable>,
    pub wal_paths: Vec<PathBuf>,
    pub table_number: u64,
    pub next_file: u64,
    pub last_sequence: u64,
    pub options: Options,
    pub cache: Arc<BlockCache>,
}

pub struct Output {
    pub reader: SstableReader,
    pub size: u64,
}

pub fn run(job: FlushJob) -> Result<Output, Error> {
    let final_path = table_path(&job.dir, job.table_number);
    let temporary = job.dir.join(format!("SST-{:020}.tmp", job.table_number));
    let file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
    let result = write_and_publish(job, file, &temporary, &final_path);
    if result.is_err() && temporary.exists() {
        if let Err(error) = fs::remove_file(&temporary) {
            eprintln!("failed to remove incomplete SSTable: {error}");
        }
    }
    result
}

fn write_and_publish(job: FlushJob, file: std::fs::File, temporary: &Path, final_path: &Path) -> Result<Output, Error> {
    let mut writer = SstableWriter::with_options(file, &job.options)?;
    for (key, value) in job.table.entries() {
        writer.add(key, value)?;
    }
    let file = writer.finish()?;
    file.sync_all()?;
    let size = file.metadata()?.len();
    job.options.statistics.record_level(0, size)?;
    fs::rename(temporary, final_path)?;
    manifest::sync_dir(&job.dir)?;
    let reader = SstableReader::open(final_path, job.cache)?;
    manifest::append(&job.dir, &Edit {
        added: vec![(0, job.table_number)],
        removed: Vec::new(),
        next_file: job.next_file,
        last_sequence: job.last_sequence,
    })?;
    for path in job.wal_paths { fs::remove_file(path)?; }
    manifest::sync_dir(&job.dir)?;
    Ok(Output { reader, size })
}
