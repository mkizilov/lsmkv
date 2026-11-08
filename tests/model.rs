mod common;

use common::ModelDb;
use lsmkv::memtable::{BTreeMemtable, InternalKey, Kind, Memtable};
use lsmkv::{Error, Options};

#[test]
fn model_matches_latest_memtable_values() -> Result<(), Error> {
    let mut model = ModelDb::open("unused", Options::default())?;
    let mut table = BTreeMemtable::default();
    for (seq, key, value) in [
        (1, b"a", b"one"),
        (2, b"b", b"two"),
        (3, b"a", b"new"),
    ] {
        model.put(key, value)?;
        table.insert(
            InternalKey::new(key.to_vec(), seq, Kind::Put),
            value.to_vec(),
        );
    }
    model.delete(b"b")?;
    table.insert(
        InternalKey::new(b"b".to_vec(), 4, Kind::Delete),
        Vec::new(),
    );
    for key in [b"a", b"b", b"c"] {
        assert_eq!(
            model.get(key)?,
            table.get(key, u64::MAX).map(<[u8]>::to_vec)
        );
    }
    assert_eq!(model.scan(..)?, vec![(b"a".to_vec(), b"new".to_vec())]);
    assert!(model.scan(b"b".to_vec()..)?.is_empty());
    Ok(())
}
