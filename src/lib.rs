//! Public interface for the planned LSM-tree database.

use std::ops::RangeBounds;
use std::path::Path;

/// Errors returned by the database.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The storage engine has not been implemented yet.
    #[error("database operation is not implemented")]
    NotImplemented,
}

/// Configuration for opening a database.
#[derive(Debug, Default, Clone)]
pub struct Options {}

/// A key and its value, returned by a range scan.
pub type Entry = (Vec<u8>, Vec<u8>);

/// A database handle.
#[derive(Debug)]
pub struct Db {
    _private: (),
}

impl Db {
    /// Open a database at `path`.
    pub fn open(_path: impl AsRef<Path>, _options: Options) -> Result<Self, Error> {
        Err(Error::NotImplemented)
    }

    /// Store a value for a key.
    pub fn put(&self, _key: impl AsRef<[u8]>, _value: impl AsRef<[u8]>) -> Result<(), Error> {
        Err(Error::NotImplemented)
    }

    /// Retrieve the current value for a key, if present.
    pub fn get(&self, _key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, Error> {
        Err(Error::NotImplemented)
    }

    /// Delete a key.
    pub fn delete(&self, _key: impl AsRef<[u8]>) -> Result<(), Error> {
        Err(Error::NotImplemented)
    }

    /// Retrieve key-value pairs in a range of byte keys.
    pub fn scan<R>(&self, _range: R) -> Result<Vec<Entry>, Error>
    where
        R: RangeBounds<Vec<u8>>,
    {
        Err(Error::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::{Db, Error, Options};

    #[test]
    fn open_reports_not_implemented() {
        assert!(matches!(
            Db::open("unused", Options::default()),
            Err(Error::NotImplemented)
        ));
    }

    #[test]
    fn operations_report_not_implemented() {
        let db = Db { _private: () };
        assert!(matches!(
            db.put(b"key", b"value"),
            Err(Error::NotImplemented)
        ));
        assert!(matches!(db.get(b"key"), Err(Error::NotImplemented)));
        assert!(matches!(db.delete(b"key"), Err(Error::NotImplemented)));
        assert!(matches!(db.scan(..), Err(Error::NotImplemented)));
    }
}
