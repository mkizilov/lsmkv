# Roadmap and repository handoff

This roadmap is ordered by risk and unfinished acceptance criteria, not by
feature appeal. Estimates are engineering hours for one Rust engineer familiar
with storage systems, with project-local dependency access restored. They include
implementation, review, and ordinary verification, but exclude waiting for
network access, CI capacity, privileged profiling permission, or hardware. Steps
may overlap only where their dependencies permit it. The original full plan was
**260–528 hours**. Steps 1–2 are now complete; subtracting their original
estimates leaves an indicative **236–472 hours** for the remaining steps, before
adjusting for partial progress. This is not a delivery commitment.

## Current implementation and evidence

| Area | Status at handoff |
| --- | --- |
| Stable crate, CLI, WAL, memtable, SSTables, manifest, snapshots, compaction, cache, filters, filesystem abstraction | **Implemented and checked:** stable all-target `cargo check` and release build, strict Clippy, and the full test suite passed on 2026-11-08; tests do not prove production durability. |
| Property, fault-injection, 200-iteration crash, and 8-writer/8-reader concurrency suites | **Verified:** property and fault tests, 200 crash iterations, and debug/release concurrency stress passed. |
| Three bounded fuzz targets | **Partially verified:** all three targets built, but a five-second WAL smoke run hung until the 180-second cap; full-duration targets remain unverified and no fuzz crash fixtures are claimed. |
| Million-key Criterion and CLI benchmarks | **Partially implemented:** workloads and machine specifications are documented, but no performance results, slowest workload, profile, or measured optimization exist. |
| CLI checksum verification, statistics, source review | **Implemented and tested:** CLI, integrity, statistics, and recovery integration tests passed; `TODO.md` records remaining review risks. |
| Shell and repository inspection | **Verified here:** shell syntax and Git whitespace checks passed; targeted scans of tracked files found no matches for four common credential signatures or common cache/scratch paths. These scans are not a complete security audit. |

DNS access was restored on 2026-11-08; pinned stable and fuzz toolchains and
cargo-fuzz were installed locally. `scripts/check.sh` passed with 77 tests, and
both debug and release concurrency stress passed. Root and fuzz lockfiles were
generated for reproducibility. No hardware or Docker
integration is required by the current implementation; no such check has been
performed, and host services and hardware remain outside the repository boundary.

## 1. Restore the project-local Rust toolchain

**Status: complete on 2026-11-08.** Stable 1.85.1 with rustfmt and Clippy,
nightly-2025-03-18, and cargo-fuzz 0.12.0 are installed project-locally.

- **Scope:** Install the pinned stable toolchain and components inside the
  repository; install the pinned nightly and cargo-fuzz locally when needed.
- **Rationale:** Every Rust acceptance criterion is blocked by missing tools and
  failed DNS, not by a passing check.
- **Dependencies:** None; requires permitted project-local package access.
- **Acceptance:** `rustc`, Cargo, rustfmt, and Clippy report the pinned stable
  version without writing outside the repository; nightly remains fuzz-only.
- **Effort:** 8–16 hours.

## 2. Establish a passing build and verification baseline

**Status: complete on 2026-11-08.** `cargo check --all-targets`, an all-target
release build, strict Clippy, and `RUST_TEST_THREADS=2 scripts/check.sh` passed
using the existing macOS SDK read-only; the latter reported 77 passing tests.

- **Scope:** Resolve dependencies locally, run format, Clippy, unit/integration
  tests, and fix every actual compile, formatting, lint, or test failure.
- **Rationale:** A passing baseline is the prerequisite for trustworthy
  downstream work and must be preserved as changes are made.
- **Dependencies:** Step 1.
- **Acceptance:** `RUST_TEST_THREADS=2 scripts/check.sh` passes from a clean
  checkout; any corrections have regression tests and no weakened assertions.
- **Effort:** 16–40 hours.

## 3. Validate and harden concurrent group commit

- **Scope:** Run the existing 10-second eight-writer/eight-reader stress suite in
  debug and release, exercise atomic batches, leader handoff, stalls, and
  coordinator/leader failure and shutdown paths.
- **Rationale:** The debug and release stress suites passed; coordinator or
  leader panic can still strand waiters or hang shutdown.
- **Dependencies:** Steps 1–2.
- **Acceptance:** Both specified stress runs complete without mismatches,
  deadlocks, or panics; deterministic failure-injection tests prove waiters are
  released and background failures reach callers.
- **Effort:** 24–48 hours.

## 4. Execute crash and filesystem-fault recovery tests

- **Scope:** Run all 200 SIGKILL iterations and injected write, torn-write,
  truncate, fsync, manifest, and SSTable failures; investigate every failure.
- **Rationale:** The full harness and fault suite passed once; broader crash
  boundaries and repeated platform coverage remain worthwhile.
- **Dependencies:** Steps 1–2.
- **Acceptance:** The full harness passes with actual iteration counts recorded;
  acknowledged data survives, unacknowledged data remains a valid prefix, and
  all fault cases recover or report corruption without silent loss.
- **Effort:** 24–48 hours.

## 5. Make manifest recovery distinguish truncation from durable corruption

- **Scope:** Design a durable commit marker or equivalent protocol, including
  migration and crash-safe publication, so truncated durable edits cannot be
  mistaken for an uncommitted tail.
- **Rationale:** Current manifest recovery truncates an incomplete last edit;
  if its WALs were already removed, prior durable state could be lost.
- **Dependencies:** Steps 2 and 4.
- **Acceptance:** Faults at every publication/deletion boundary preserve prior
  committed state or return a clear corruption error; old-format handling is
  explicit and covered by regression tests.
- **Effort:** 24–48 hours.

## 6. Complete integrity and security verification

- **Scope:** Exercise offline verification against corrupted manifests, WALs,
  indexes, data/filter blocks, footers, missing live tables, and orphan files;
  assess footer checksum/versioning and dependency risks.
- **Rationale:** CLI integrity tests passed; the footer lacks a checksum and
  recognized-file coverage and dependency review remain limited.
- **Dependencies:** Steps 2, 4, and 5.
- **Acceptance:** Corruptions are detected without altering evidence; coverage
  and excluded file types are explicit; dependency and unsafe-code review results
  are documented with regression tests for fixes.
- **Effort:** 12–24 hours.

## 7. Prove snapshot and compaction correctness

- **Scope:** Run model-based sequences and targeted overlapping-level,
  tombstone, retained-version, file-lifetime, and concurrent snapshot tests.
- **Rationale:** The compaction implementation drops versions and sometimes
  tombstones; an error can resurrect deleted data or violate snapshots.
- **Dependencies:** Steps 2–4.
- **Acceptance:** Property tests and deterministic edge cases pass across
  flush, compaction, close/reopen, and live snapshots; failing seeds are retained.
- **Effort:** 20–40 hours.

## 8. Run and extend decoder fuzzing

- **Scope:** Run each existing WAL, SSTable, and manifest target for at least
  600 seconds, expand coverage where necessary, and minimize all crashes.
- **Rationale:** All targets built, but the bounded WAL smoke run hung before
  producing libFuzzer output; the required runs have not completed.
- **Dependencies:** Steps 1–2 and integrity fixes from steps 5–6.
- **Acceptance:** Actual durations and outcomes are recorded; each failure is
  fixed and its minimized input is retained in a normal regression test.
- **Effort:** 16–32 hours.

## 9. Measure, profile, and optimize one real workload

- **Scope:** Run all million-key, 100-byte Criterion workloads, identify the
  measured slowest workload, profile it, and test one targeted optimization.
- **Rationale:** The requested benchmark numbers, hotspot, and before/after
  comparison do not exist; no speculative optimization was made.
- **Dependencies:** Steps 1–3 and correctness fixes from steps 5–7.
- **Acceptance:** `BENCHMARKS.md` records actual machine/method/results and a
  reproducible profile; a statistically measurable improvement is retained, or
  the optimization is reverted. Report privilege blockers without escalation.
- **Effort:** 24–48 hours.

## 10. Provide explicit close and error propagation

- **Scope:** Add an error-returning close/shutdown path and supervised worker and
  channel lifetimes; make cleanup and final synchronization failures observable.
- **Rationale:** Several destructor paths can only log errors, and disconnected
  callers can lose the final result of an operation.
- **Dependencies:** Steps 2–3.
- **Acceptance:** Tests inject close, worker, reply-channel, and cleanup failures
  and verify bounded shutdown and actionable errors without data loss.
- **Effort:** 12–24 hours.

## 11. Bound recovery and decoding resource use

- **Scope:** Stream or bound manifest recovery, audit allocations and malformed
  length handling, and bound scan/compaction resource consumption.
- **Rationale:** The manifest is currently read wholly into memory; malformed or
  very large inputs and whole-level jobs can exhaust resources.
- **Dependencies:** Steps 2 and 5.
- **Acceptance:** Large/corrupt inputs produce documented errors within tested
  memory and time budgets; legitimate large databases remain readable.
- **Effort:** 16–32 hours.

## 12. Recover and clean obsolete or orphaned files safely

- **Scope:** Add manifest reconstruction or explicit repair and a conservative
  orphan/temporary-file collector with durable directory handling.
- **Rationale:** Unpublished outputs and failed cleanup can leave files behind;
  missing manifests require manual investigation.
- **Dependencies:** Steps 4–5.
- **Acceptance:** Recovery never silently adopts uncommitted files or deletes a
  live reader's data; crash-boundary tests verify repair and cleanup behavior.
- **Effort:** 12–24 hours.

## 13. Improve the public and CLI interfaces

- **Scope:** Design a validated options builder, fallible streaming range scans,
  clearer snapshot lifetime and statistics APIs, and binary-safe CLI I/O.
- **Rationale:** Scans allocate results, options expose internals, counters are
  process-local, and the CLI accepts UTF-8 input only.
- **Dependencies:** Steps 2, 6, and 10.
- **Acceptance:** Documented migration examples and compatibility tests cover
  binary keys/values, streaming errors, snapshots, and statistics semantics.
- **Effort:** 12–24 hours.

## 14. Make builds and automation reproducible

- **Scope:** Generate and retain appropriate lockfiles, pin local tool installs,
  and add project-contained CI for formatting, Clippy, tests, and fuzz smoke runs.
- **Rationale:** Lockfiles and a local Rust baseline now exist; automated clean
  checkout and CI verification are still needed.
- **Dependencies:** Steps 1–2.
- **Acceptance:** A clean checkout builds with documented pinned tools and
  lockfiles; CI runs the same checks without host credentials or global installs.
- **Effort:** 8–16 hours.

## 15. Reduce read and compaction amplification

- **Scope:** Use measured profiles to implement bounded range-aware merging and
  overlap-selective compaction rather than scanning or rewriting whole levels.
- **Rationale:** The current range scan and whole-level compaction can do far
  more I/O and CPU work than the requested range requires.
- **Dependencies:** Step 9 and correctness guarantees from step 7.
- **Acceptance:** Model/crash/snapshot tests still pass, and identical benchmark
  comparisons show a reproducible improvement without increased data loss risk.
- **Effort:** 24–48 hours.

## 16. Prepare a documented release decision

- **Scope:** Consolidate test evidence, residual risk, portability, support
  policy, storage-format migration, and a reproducible release checklist.
- **Rationale:** The repository is experimental and no production durability or
  performance claim is justified by the current evidence.
- **Dependencies:** Steps 2–15.
- **Acceptance:** A reviewer can reproduce the checks and trace every remaining
  limitation; release is explicitly accepted or deferred without invented data.
- **Effort:** 8–16 hours.
