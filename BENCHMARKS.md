# Benchmarks

## Machine and environment

Observed on 2026-11-07 in this repository's sandbox:

- CPU: Apple M4, 10 physical and 10 logical cores (`sysctl`).
- Memory: 17,179,869,184 bytes (16 GiB).
- Architecture: arm64; macOS 27.2, build 26B123 (`sw_vers`).
- Workspace filesystem: `/dev/disk3s5`, 228 GiB total and 166 GiB available at
  inspection time (`df -h .`).
- Rust: project pins stable 1.85.1, but neither `rustc` nor Cargo was available;
  no compiler version was observed. The toolchain download failed because
  `static.rust-lang.org` could not be resolved.

## Workloads and method

`benches/workloads.rs` uses Criterion 0.5.1 and a fresh, project-local database
containing 1,000,000 keys with 100-byte values. Keys are eight-byte big-endian
even integers; misses use the intervening odd integers, so most misses are
within the populated key range. Preloading is outside Criterion's measurements,
in batches of 1,000. Sequential and deterministic pseudo-random writes overwrite
keys within the populated keyspace; read hits, read misses, and 100-key range
scans validate their results. The database uses default options, including WAL
fsync on each single-threaded write. Each Criterion workload reports operation
latency; scan throughput counts keys while other workloads count operations.
Warm-up is 3 seconds, measurement is 10 seconds, and sample size is 10. The
explicit sync between workloads is outside the measured loop.

```sh
export CARGO_BUILD_JOBS=2
cargo bench --bench workloads
cargo run --release --bin lsmkv-cli -- bench --seconds 60 --keys 1000000
```

The CLI runs each selected workload for the requested duration and includes a
final sync in its reported elapsed time. Use `--workload sequential-write`,
`random-write`, `read-hit`, `read-miss`, or `scan-100` to isolate a workload.
For a functional smoke test only, `--keys 120 --seconds 1 --workload read-hit`
uses a smaller dataset; it is not a substitute for the million-key measurements.
Each run creates a unique directory under `.agent-local/benchmarks/` and removes
it after the database closes on normal exit. A forcibly terminated process can
leave its own `run-<pid>-<counter>` directory there; inspect and remove only that
run's directory after confirming the process has exited. Input generation uses
only one 1,000-key batch at a time rather than holding a million-key vector;
database indexes, caches, active scans, and compaction state also consume memory.
Compactions can temporarily increase disk usage.

## Results and profile

No workload timings or throughput were measured in this environment. Attempts
to run Criterion, the CLI benchmark, and the profiler could not start because
Cargo is absent. Consequently the slowest workload and its top hotspot are
unknown, and there are no before/after numbers. No speculative optimization was
made or claimed.

Once the project-local Rust toolchain is available, collect Criterion results
above, identify the slowest measured workload, and profile that workload without
using sudo or changing OS settings. A project-local `cargo-flamegraph` install
and an example command are:

```sh
cargo install flamegraph --version 0.6.8 --locked --root "$PWD/.agent-local/flamegraph-tools"
export PATH="$PWD/.agent-local/flamegraph-tools/bin:$PATH"
cargo flamegraph --bin lsmkv-cli --output .agent-local/benchmarks/flamegraph.svg -- bench --seconds 60 --keys 1000000 --workload scan-100
```

Replace `scan-100` with the **measured** slowest workload. macOS profiling may
require privileges unavailable in this sandbox; if so, report that limitation
without escalating. Retain the profile only while investigating, compare the
same workload and dataset before and after an isolated change, and revert the
change if it does not measurably improve the result.
