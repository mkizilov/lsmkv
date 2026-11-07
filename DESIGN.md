# lsmkv design

This document describes the intended engine, not functionality provided by the
initial API stubs. Keys and values are arbitrary bytes, ordered lexicographically.
The initial API returns a not-implemented error and does not create a database.

## Internal keys and storage

Each mutation has an internal key `(user key, sequence number, kind)`, where
kind is `Put` or `Delete`. Internal keys sort by user key ascending and then
sequence number descending; kind breaks any remaining ties deterministically.
Sequence numbers are monotonically assigned to writes. Deletes are tombstones,
not immediate removal, so older values cannot reappear from lower levels.

The write-ahead log (WAL) records mutations before they enter the mutable
memtable, an in-memory ordered map of internal keys. A full memtable becomes an
immutable memtable and a fresh mutable memtable takes its place. Immutable
memtables remain readable until flushed to sorted string tables (SSTables).
On recovery, live SSTables are recovered from the manifest and unflushed WAL
records are replayed into memtables. WAL retirement must happen only after the
corresponding flush is durably recorded.

Each SSTable contains sorted data blocks, a block index mapping key ranges to
block locations, and bloom filters that can rule out a user key without reading
its data block. The manifest records the live SSTable files and their levels;
flush and compaction changes must be made durable before obsolete files are
removed. File publication and recovery must avoid treating incomplete output as
live.

## Paths

Writes assign a sequence number, append a Put or Delete to the WAL, honor the
configured durability policy, and insert the mutation into the mutable
memtable. Flushes write immutable memtables to level 0 SSTables and publish the
new files in the manifest before retiring their WALs.

Reads search the mutable and immutable memtables and candidate SSTables, using
the index and bloom filters where applicable. They choose the newest visible
sequence for the user key; a Delete means the key is absent. Range scans merge
ordered sources, select the newest visible version per user key, and suppress
tombstones.

Leveled compaction merges overlapping SSTables into the next level. Level 0 may
contain overlapping key ranges; deeper levels have non-overlapping ranges
within each level. Compaction retains versions and tombstones needed for reads
and must not discard a tombstone while an older value could remain in a lower
level. It atomically publishes its live-file changes through the manifest before
deleting inputs.
