use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::path::Path;

use lsmkv::{Entry, Error, Options};

/// Simple current-state reference for database integration tests.
#[derive(Clone, Default)]
pub struct ModelDb {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[allow(clippy::unnecessary_wraps)] // Match the fallible database API.
impl ModelDb {
    pub fn open(_path: impl AsRef<Path>, _options: Options) -> Result<Self, Error> {
        Ok(Self::default())
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), Error> {
        self.entries
            .insert(key.as_ref().to_vec(), value.as_ref().to_vec());
        Ok(())
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.entries.get(key.as_ref()).cloned())
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<(), Error> {
        self.entries.remove(key.as_ref());
        Ok(())
    }

    pub fn scan<R>(&self, range: R) -> Result<Vec<Entry>, Error>
    where
        R: RangeBounds<Vec<u8>>,
    {
        Ok(self
            .entries
            .range(range)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}
