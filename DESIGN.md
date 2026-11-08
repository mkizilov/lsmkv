# lsmkv design

Keys and values are arbitrary bytes, ordered lexicographically. The current
`Db` uses a WAL and one B-tree memtable; SSTables, immutable memtables, the
manifest, and compaction below describe the intended future engine.

`Db::open` holds an exclusive OS advisory lock on the persistent `LOCK` file for
the handle's lifetime. It replays complete `WAL` batches into a fresh memtable,
requiring contiguous sequences starting at 1, truncates and syncs an invalid or
torn tail to the last valid record, then opens the WAL for appending. The lock
file is never unlinked, so a second process cannot accidentally lock a new
inode. A mutex serializes reads and writes; each nonempty `WriteBatch` receives
one sequence per mutation, is appended as a single WAL record, and is applied
in full to the memtable only after append succeeds. An empty batch is a no-op.
After a WAL append error, further writes require reopening: a failed write may
still have reached disk and can appear after recovery. Scans return current live
keys in sorted order. There is no concurrent snapshot API yet.

## Internal keys and storage

Each mutation has an internal key `(user key, sequence number, kind)`, where
kind is `Put` or `Delete`. Internal keys sort by user key ascending and then
sequence number descending; kind breaks any remaining ties deterministically.
Sequence numbers are monotonically assigned to writes. Deletes are tombstones,
not immediate removal, so older values cannot reappear from lower levels.
The memtable exposes a snapshot lookup that distinguishes a tombstone from a
missing key for future cross-layer reads; its convenience `get` maps both to
not found. Its approximate memory accounting includes key and value lengths
and fixed entry sizes, but excludes B-tree allocation overhead and spare capacity.
Identical internal keys replace their previous values; callers must assign unique
sequence numbers to distinct mutations, including differing kinds for one key.

The write-ahead log (WAL) records mutations before they enter the mutable
memtable, an in-memory ordered map of internal keys. A full memtable becomes an
immutable memtable and a fresh mutable memtable takes its place. Immutable
memtables remain readable until flushed to sorted string tables (SSTables).
On recovery, live SSTables are recovered from the manifest and unflushed WAL
records are replayed into memtables. WAL retirement must happen only after the
corresponding flush is durably recorded.

### WAL format and synchronization

All integers are little-endian.
Each record has a four-byte CRC32C (Castagnoli), a four-byte payload length, a
one-byte record type (1 for batch), and the payload. The CRC covers the length,
type, and payload, not the CRC field. Payloads are limited to 64 MiB and contain
a nonzero u32 operation count followed by operations: one-byte kind (1 Put,
2 Delete), u64 sequence, u32 key length, u32 value length, key bytes, and value
bytes. A Delete must have zero value length. Empty keys and Put values are valid.

The reader returns complete validated batches in order, stopping permanently at
the first incomplete, invalid, or corrupt record; `valid_bytes` identifies the
safe prefix to retain. I/O errors are returned separately. A writer refuses to
append to a damaged tail: recovery must truncate it to the valid prefix first.
The writer syncs each append or uses a worker to sync within the configured
interval after the first unsynced append, even during inactivity. Interval-mode
acknowledgement is not a durability guarantee; explicit `sync` or `close` waits
for durability and reports worker errors. Close the writer explicitly to observe
final errors; implicit drop reports errors to stderr. The database does not yet
sync directory entries, so creation of a new directory or WAL is not guaranteed
to survive a power failure; WAL rotation remains future engine work.

Each SSTable contains sorted data blocks, a block index mapping key ranges to
block locations, and bloom filters that can rule out a user key without reading
its data block. The manifest records the live SSTable files and their levels;
flush and compaction changes must be made durable before obsolete files are
removed. File publication and recovery must avoid treating incomplete output as
live.

### SSTable writer format, version 1

The standalone writer emits data blocks, an empty filter block, an index block,
and a 40-byte footer, in that order. The writer accepts strictly increasing
internal keys and requires empty values for deletes. Empty tables are valid.
All fixed-width integers are little-endian. Varints are canonical unsigned LEB128
(seven data bits per byte, high bit indicates continuation). Each block is its
payload followed by a four-byte CRC32C of the payload. All stored block sizes
include the CRC. The footer is not a block and has no CRC.

An encoded internal key is `user_key || sequence:u64 || kind:u8`, with kind 1
for Put and 2 for Delete. The user key is all bytes before the final nine bytes.
These encoded bytes are for storage and prefix compression, not lexicographic
comparison: keys must be compared using the internal-key ordering above.

Each data block's payload contains entries followed by a restart array and a
restart count. Each entry is `shared:varint, suffix_len:varint, value_len:varint,
key_suffix, value`. Its key is the first `shared` bytes of the previous encoded
key followed by `key_suffix`. The first entry and every sixteenth subsequent
entry are restart entries with `shared = 0`. The restart array contains the
u32 byte offset of each restart entry from the beginning of the block payload,
followed by the u32 number of restarts. A block targets 4096 bytes including
restart metadata and CRC; a single oversized entry occupies its own block.

The filter block has an empty payload for now (and therefore just its CRC). The
index payload begins with a u32 block count followed by one entry per data
block: `key_len:varint, last_encoded_key, offset:u64, size:u64`. Offsets are
absolute from the start of the SSTable and the index follows data-block order.
The footer has five u64 fields: filter offset, filter size, index offset, index
size, and magic `0x31564b4d534c5353`. SSTables are not yet integrated into `Db`;
the caller must flush/sync, atomically publish, and manage incomplete output.

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
