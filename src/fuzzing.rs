//! Bounded decoder entry points for the separate cargo-fuzz workspace.

use crate::Error;

const MAX_INPUT: usize = 65_536;

fn bounded(bytes: &[u8]) -> Result<(), Error> {
    if bytes.len() > MAX_INPUT {
        return Err(Error::Corruption("fuzz input exceeds limit"));
    }
    Ok(())
}

pub fn wal(bytes: &[u8]) -> Result<(), Error> {
    bounded(bytes)?;
    let mut reader = crate::wal::WalReader::new(bytes);
    while reader.next_batch()?.is_some() {}
    Ok(())
}

pub fn wal_payload(bytes: &[u8]) -> Result<(), Error> {
    bounded(bytes)?;
    crate::wal::fuzz_payload(bytes)
}

pub fn sstable_footer(bytes: &[u8]) -> Result<(), Error> {
    bounded(bytes)?;
    crate::sstable::fuzz_footer(bytes)
}

pub fn sstable_block(bytes: &[u8]) -> Result<(), Error> {
    bounded(bytes)?;
    crate::sstable::fuzz_block(bytes)
}

pub fn sstable_filter(bytes: &[u8]) -> Result<(), Error> {
    bounded(bytes)?;
    crate::sstable::fuzz_filter(bytes)
}

pub fn manifest_edit(bytes: &[u8]) -> Result<(), Error> {
    bounded(bytes)?;
    crate::manifest::fuzz_edit(bytes)
}

pub fn manifest_log(bytes: &[u8]) -> Result<(), Error> {
    bounded(bytes)?;
    crate::manifest::fuzz_log(bytes)
}
