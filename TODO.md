# Senior review and follow-ups

This is a source review, not a successful compiler, test, crash-test, fuzz, or
benchmark run. Cargo and rust-analyzer were unavailable in the review sandbox.
Third-party dependencies and their transitive native code were not audited.

## Unsafe and panic inventory

- No `unsafe` blocks or other uses of the `unsafe` keyword were found in the
  repository's Rust source, tests, benchmarks, or fuzz targets. This does not
  cover dependencies.
- No `.unwrap()`, `.expect()`, `panic!`, or `unreachable!` occurs in non-test
  library or CLI code. Test modules and integration tests use them intentionally.
- The three `.expect()` calls outside tests are in `benches/workloads.rs`: dataset
  preparation, measured operations, and the final sync. They intentionally fail
  the Criterion run on an invalid benchmark; consider structured error reporting
  if Criterion gains a convenient fallible benchmark interface.
- Safe Rust indexing still has implicit panic paths if invariants are violated;
  decoder fuzzing and the complete test suite must be run before relying on them.

## I/O and error visibility

- `WalReader` intentionally stops at the first invalid record. Recovery now
  rejects complete corrupt records without truncating them, but truncates torn
  tails; offline `verify` rejects both. This is a behavior change worth validating
  with the crash and fault suites.
- `manifest::open` treats a truncated final manifest edit as an interrupted
  append and truncates it. A storage corruption that truncates a previously
  durable edit is not distinguishable from a crash here; if covered WALs were
  already removed, this may risk data loss. Add a durable commit marker or a
  recovery protocol that can distinguish the cases before production use.
- `WalWriter::drop` and its disconnected-worker fallback can only log final sync
  failures. `Db::drop` logs background failure and a failed coordinator join,
  but callers cannot receive these errors. Add an explicit error-returning
  `Db::close` and make callers use it when shutdown durability matters.
- `Table::drop` logs obsolete-file unlink failures; `benchmark::OwnedDir::drop`
  logs temporary-directory cleanup failures. Flush and compaction cleanup ignore
  `NotFound` and log other removal failures after the original error. These
  destructors cannot propagate cleanup errors; orphan files may remain.
- `fs::Fs::exists` deliberately maps `NotFound` to false; other metadata errors
  propagate. `manifest::open` reads the entire manifest into memory, so a very
  large file can exhaust resources; stream and bound manifest recovery.
- `src/lib.rs` ignores failed maintenance and write-reply channel sends when the
  receiver has gone away. `src/wal.rs` likewise ignores a failed reply send;
  disconnection can hide an operation's final I/O result from the departed
  caller. A disconnected WAL worker now attempts a final sync and logs failure.
  The `fetch_update` result in statistics is ignored because its closure always
  returns `Some`; it is not an I/O operation.
- The fuzz targets intentionally ignore decoder errors because malformed inputs
  are expected; the WAL target can also discard an allocation/read error. The
  crash-test `ChildGuard` ignores kill/wait errors during cleanup and uses
  `try_wait().ok()`; improve cleanup reporting without masking the original test
  failure. CLI `eprintln!` on a broken stderr can itself panic.
- The SSTable footer has structural checks but no checksum. `verify` covers all
  recognized final SSTables, numbered WALs and the current manifest, but not
  temporary/orphan manifest files or unknown files; it cannot authenticate data
  against deliberate manipulation.

## API and operational rough edges

- `Db::scan` returns an eagerly allocated `Vec` and scans all sources rather than
  offering a fallible streaming range iterator; large scans and benchmarks can be
  expensive. Snapshots borrow `Db`, cannot outlive it, and have no explicit
  persistent identity.
- `Db::stats()` cannot return an error on a poisoned lock, logs and recovers the
  guard, and shares process-lifetime counters through cloned `Options`. A new CLI
  process normally reports zero historical hit rate and write amplification.
- `Options` exposes a filesystem object, statistics state, and many mutable
  public fields. Consider a validated builder and a narrower public surface;
  standalone SSTable verification currently requires constructing a block cache.
- CLI input is UTF-8 only, `get` appends a newline to raw bytes, and `scan` emits
  hex. A binary-safe input/output mode and structured output would make scripting
  less awkward. `verify_path` requires an existing, closed database and lock.
- Whole-level compaction can be expensive, orphan files are not automatically
  collected, and the database directory's entry in its parent is not synced.
  Benchmark/profiling and before/after optimization remain outstanding.
- The maintenance coordinator and write-queue leader do not have a fully
  supervised shutdown protocol if their own thread panics. In particular,
  `Db::drop` can wait indefinitely for a pending job if the coordinator has
  exited, and queued writers can be stranded if a leader panics before handoff.
  Add supervision and failure propagation, then exercise those cases.
- Opening now rejects SSTables when both `CURRENT` and the manifest are missing,
  rather than silently initializing an empty database. There is not yet a tool
  to reconstruct a lost manifest from otherwise recoverable files.
- Install the pinned toolchain locally when DNS is available; then run
  `scripts/check.sh`, property tests, the 200-iteration crash harness, 10-second
  concurrency tests in debug/release, all fuzz targets, and benchmarks. Fix and
  retain any discovered regressions rather than assuming this review proves
  correctness.
