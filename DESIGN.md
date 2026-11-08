# lsmkv design

Keys and values are arbitrary bytes, ordered lexicographically. The current
`Db` uses a WAL, B-tree memtables, and manifest-tracked SSTables. It flushes to
level 0 and compacts into successively deeper non-overlapping levels.

`Db::open` holds an exclusive OS advisory lock on the persistent `LOCK` file for
the handle's lifetime. It loads the manifest and its SSTables, then replays WAL
batches newer than the manifest's last flushed sequence into a fresh memtable,
requiring contiguous sequences and truncating a torn WAL tail to the last valid
record. It resumes appending to the latest WAL. The lock
file is never unlinked, so a second process cannot accidentally lock a new
inode. Each nonempty `WriteBatch` receives one sequence per mutation and is
applied in full to the memtable only after its WAL append succeeds. An empty
batch is a no-op.
After a WAL append error, further writes require reopening: a failed write may
still have reached disk and can appear after recovery. Scans return current live
keys in sorted order. `snapshot()` pins the current sequence; its `get` and
`scan` filter out later writes even across subsequent flushes. Snapshots borrow
the database and cannot outlive it. Active snapshot sequences are registered
until their handles are dropped; compaction preserves the newest version and the
version visible to each snapshot for every user key.

### Concurrency and group commit

`Db` is `Send + Sync` and is intended to be shared through `Arc`. Writers enqueue
individual atomic batches in FIFO order. One caller becomes leader, waits up to
1 ms for companions, and combines up to 256 requests and the WAL's 64 MiB limit
into one record. With `EveryWrite`, that record uses one WAL write and fsync;
interval synchronization retains its existing delayed-durability semantics. Each
request remains atomic on replay: a torn shared record loses the whole group,
while a valid record contains every batch. Completed requests receive individual
responses, then leadership is handed to a pending writer. A failed WAL append
poisons further writes and may still have reached disk.

The queue mutex is held only for admission and leader handoff. The state mutex
protects sequence assignment, memtable publication, snapshot pins and version
installation; the leader releases it during WAL writes and directory/WAL
rotation I/O. Readers capture an immutable table version and release the mutex
before SSTable I/O. Scans pin a sequence and a copy-on-write active memtable,
then release the mutex before merging. The maintenance coordinator joins completed jobs only;
writers and `sync` wait on a condition variable that releases the state mutex,
so compaction does not hold it while doing I/O. Compaction outputs are installed
under the mutex; obsolete file deletion occurs after it is released. `stats()`
reports `wal_group_commits` and `committed_write_requests` for process-lifetime
group-commit observation.

`flush()` and `compact()` are FIFO barriers in the same writer queue. `flush()`
publishes the current nonempty memtable; `compact()` first flushes, then forces
one eligible level into its successor and waits for resulting maintenance.
Either is a no-op when there is no corresponding work.

When a batch takes the mutable memtable over `Options::memtable_size` (default
4 MiB), the database syncs its old WAL, creates and syncs a new numbered WAL,
then freezes the old memtable and flushes it on a background thread. At most one
flush is in flight; a subsequent rotation waits for it. Reads see the mutable
memtable, the frozen memtable, live level-0 tables in newest-first order, and
then deeper levels in ascending level order. Each deeper level has non-overlapping
user-key ranges, validated at open and binary-searched for a single candidate.
Scans heap-merge all ordered sources by internal key, discard versions newer
than the read sequence, and return the first visible version per user key,
suppressing tombstones. The current `scan` API materializes the result as a Vec.
`sync` and database drop wait for pending flushes. A failed flush stops further
writes and retains the frozen table for recovery.

The worker writes a new SSTable to a temporary file, syncs it, renames it to
its final name and syncs the directory. It then appends and syncs a manifest edit
that adds the level-0 file and advances the flushed sequence, and only afterward
deletes the WALs covered by that edit. Recovery ignores orphan SSTables that
were not added to the manifest, replays their still-live WALs, and skips stale
WAL records already covered by the manifest. File numbers are advanced past all
existing numbered WALs and SSTables, including orphan and temporary SSTables;
orphan cleanup is not yet implemented.

### Manifest

`CURRENT` contains `MANIFEST-000001\n` and is published by syncing a temporary
file, renaming it, and syncing the database directory. Initial manifest creation
also uses a synced temporary file and rename; when `CURRENT` is missing, the
manifest is validated before its pointer is published. The manifest is an
append-only sequence of little-endian records: u32 payload length, u32 CRC32C
of the encoded length, u32 CRC32C of the payload, then payload. Each version-1 payload contains u32 version, u64
next file number, u64 last flushed sequence, u32 added count, u32 removed count,
then added and removed entries respectively, each `(level:u32, file:u64)`.
Complete edits with bad checksums are rejected; a truncated final edit is
discarded and truncated. File additions/removals are applied in order. This
implementation writes level-0 flushes and compaction outputs into deeper levels;
manifest-listed deeper levels must have non-overlapping user-key ranges.

### Compaction and versions

One background maintenance job runs at a time, serialized with flushes. Level 0
is compacted when it has more than `l0_compaction_trigger` files (default 4).
Level N greater than zero is compacted when its total file size exceeds
`target_file_size * 10^N`, using saturating arithmetic. This implementation
merges all files in the chosen level and its successor, so all destination
overlaps are included. The output is split approximately at `target_file_size`
(default 2 MiB), only between user keys; a single user key may exceed the target.
The coordinator continues scheduling eligible levels even without new requests.
Writes stall at `l0_stop_writes_trigger` (default 12) until level 0 is reduced.

For each user key the merge keeps its newest version and the newest version at
or below each live snapshot sequence. A tombstone is dropped only when no deeper
level exists and no retained older Put requires that tombstone to hide it. New
snapshots acquired while a job is running have sequences at least as new as the
job's inputs. The job syncs output files and their directory before one manifest
edit adds outputs and removes inputs. Each reader holds a reference-counted table
version; removed files are marked obsolete after the manifest edit and unlinked
only when all references to their table objects have gone away. A failed job
stops further writes and leaves inputs intact; unpublished output may remain as
an orphan for future cleanup.

`Db::stats()` reports process-lifetime SSTable bytes written per level, logical
key/value bytes accepted by WAL writes, and their ratio as write amplification.
The ratio excludes WAL and manifest bytes, and is zero if no logical bytes have
been written. Statistics are shared via `Options` and are not persisted.

### Filesystem and crash testing

`Options::fs` selects an `Arc<dyn Fs>`; the default `NativeFs` delegates to the
operating system. Database directory operations, lock acquisition, WAL and
manifest reads/writes, SSTable I/O, sync, rename, truncation and deletion all go
through this boundary. `FsFile` provides read/write/seek, metadata length,
truncate, sync, and exclusive lock operations. The standalone WAL writer defaults
to `NativeFs` and also offers `open_with_fs`; standalone SSTable readers use the
filesystem supplied to their shared block cache.

`tests/crash.rs` launches a child that syncs each deterministic write and sends
acknowledgements over stdout. Its parent uses reproducible pseudo-random kill
points, sends SIGKILL, drains complete pipe messages, and checks the recovered
prefix for 200 iterations. `tests/faults.rs` supplies an injectable filesystem
that can fail or tear writes, truncate a file, or fail fsync; it checks WAL,
SSTable, and manifest failure recovery. Run `cargo test --test crash` and
`cargo test --test faults` in addition to `scripts/check.sh`. These checks have
not been run in this environment because Cargo is unavailable.

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
final errors; implicit drop reports errors to stderr. The database syncs directory
entries when publishing a new WAL, SSTable, or CURRENT file; durability of the
database directory's own creation in its parent is not yet guaranteed.

Each SSTable contains sorted data blocks, a block index mapping key ranges to
block locations, and bloom filters that can rule out a user key without reading
its data block. The manifest records the live SSTable files and their levels;
flush and compaction changes must be made durable before obsolete files are
removed. File publication and recovery must avoid treating incomplete output as
live.

### SSTable writer format, version 1

The standalone writer emits data blocks, a filter block, an index block,
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

The filter block payload is `version:u8, probes:u8, bit_count:u64, bits[]`:
version is 1; bit count is a multiple of eight and at least 64. Bits use the
least-significant-bit-first convention within each byte. An empty payload is a
legacy/disabled filter and must be treated as "may contain" for every key. The
writer builds the filter over distinct user keys, including tombstones; default
bits per key is 10, configurable from 0 (disabled) to 30. For n keys and b bits
per key, the size is max(64, n*b) rounded up to a multiple of eight. The probe
count is clamp(round(b*0.69), 1, 30).

Hashing starts with 64-bit FNV-1a (offset 0xcbf29ce484222325, prime
0x100000001b3) over the user key, then applies `mix`: xor with a 30-bit right
shift, multiply by 0xbf58476d1ce4e5b9, xor with a 27-bit right shift, multiply
by 0x94d049bb133111eb, then xor with a 31-bit right shift. Arithmetic wraps at
64 bits. If the result is h, the odd step is mix(h xor 0x9e3779b97f4a7c15) OR
1; probe i uses wrapping(h + i*step) modulo bit count.

The index payload begins with a u32 block count followed by one entry per data
block: `key_len:varint, last_encoded_key, offset:u64, size:u64`. Offsets are
absolute from the start of the SSTable and the index follows data-block order.
The footer has five u64 fields: filter offset, filter size, index offset, index
size, and magic `0x31564b4d534c5353`. The standalone writer's caller must
flush/sync, atomically publish, and manage incomplete output.

The standalone reader validates the complete file at open, including each block
CRC, all restart entries, index key ordering, contiguous block layout, filter
format and membership of stored keys, footer offsets, and magic. These constraints detect a single-byte change
anywhere, including the footer, although the footer has no checksum and the
format does not promise detection of coordinated multi-byte footer changes.
Point lookups binary-search the index and restart keys, then scan within a
restart interval; a seekable forward iterator uses the same ordering. Both APIs
return exact internal keys and values, including tombstones, rather than applying
snapshot visibility. SSTable files must remain immutable after opening.

`BlockCache::new(&options)` uses `Options::block_cache_capacity` bytes (default
8 MiB; zero disables caching); callers share it across readers with `Arc`.
Cached block payload and decoded restart offsets count against that limit, not
allocator overhead or blocks still referenced by active iterators after eviction.
The cache is keyed by a unique reader identity and block offset and evicts least
recently used blocks. Point lookups consult the filter before accessing the data
block; iterator seeks do not use the filter. `Db::stats()` exposes the shared
`bloom_filter_block_reads_avoided` counter: it increments when a negative filter
result eliminates an uncached candidate block read, excluding validation reads
at open. Cloning the same `Options` into `Db` and the shared `BlockCache` shares
this counter; it is an estimate under concurrent cache changes. `Db` uses one
shared cache and the per-table filters for its level-0 point reads.

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

## Bugs found by property tests

The model-based property suite has not yet been run in this environment: the
project-local Rust toolchain could not be installed because the download host did
not resolve. No property-test-discovered bugs or failing seeds have been claimed.
`tests/property.rs` stores future failing seeds in
`proptest-regressions/property.txt`; check them in and keep them when fixing a
failure. Run it with `cargo test --test property` in addition to `scripts/check.sh`.
