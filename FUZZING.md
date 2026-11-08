# Decoder fuzzing

The main crate remains on the stable version in the root `rust-toolchain.toml`.
The separate `fuzz/` workspace uses `nightly-2025-03-18`; its three cargo-fuzz
targets exercise the WAL framed reader and raw operation decoder, the SSTable
footer, data-block and filter decoders, and the manifest edit and log decoders.
The targets accept at most 65,536 bytes per input. The library's `fuzzing`
feature exposes these bounded entry points only for the fuzz workspace.

## Project-local setup

Run these commands from the repository root in the supplied project-local
environment. `CARGO_HOME`, `RUSTUP_HOME`, `HOME`, and `TMPDIR` must remain inside
this repository; do not use a global install.

```sh
rustup toolchain install nightly-2025-03-18 --profile minimal
cargo install --locked cargo-fuzz --version 0.12.0 --root "$PWD/.agent-local/fuzz-tools"
export PATH="$PWD/.agent-local/fuzz-tools/bin:$PATH"
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR="$PWD/.agent-local/fuzz/target"
mkdir -p .agent-local/fuzz/corpus/wal .agent-local/fuzz/corpus/sstable .agent-local/fuzz/corpus/manifest
mkdir -p .agent-local/fuzz/artifacts/wal .agent-local/fuzz/artifacts/sstable .agent-local/fuzz/artifacts/manifest
```

Run each target for at least ten minutes, one target at a time, with bounded
input length, memory, and per-input time:

```sh
cargo +nightly-2025-03-18 fuzz run wal "$PWD/.agent-local/fuzz/corpus/wal" -- -max_total_time=600 -max_len=65536 -rss_limit_mb=512 -timeout=10 -artifact_prefix="$PWD/.agent-local/fuzz/artifacts/wal/"
cargo +nightly-2025-03-18 fuzz run sstable "$PWD/.agent-local/fuzz/corpus/sstable" -- -max_total_time=600 -max_len=65536 -rss_limit_mb=512 -timeout=10 -artifact_prefix="$PWD/.agent-local/fuzz/artifacts/sstable/"
cargo +nightly-2025-03-18 fuzz run manifest "$PWD/.agent-local/fuzz/corpus/manifest" -- -max_total_time=600 -max_len=65536 -rss_limit_mb=512 -timeout=10 -artifact_prefix="$PWD/.agent-local/fuzz/artifacts/manifest/"
```

If a target fails, preserve its artifact, minimize it with
`cargo +nightly-2025-03-18 fuzz tmin <target> <artifact>`, fix the defect, and
copy the minimized bytes into a normal test fixture under `tests/` with an
ordinary regression test. Re-run the affected target and `scripts/check.sh`.
Remove generated corpora, artifacts, and build output when they are no longer
needed; never remove regression fixtures or user files.

## Current verification

The targets have not been run here: neither Cargo nor a Rust toolchain is
available, and the project-local toolchain download failed DNS resolution. No
fuzz-discovered crashes or minimized artifacts are claimed. The normal WAL test
includes a regression for an allocation issue found by inspection: a torn record
with a large declared payload must be consumed in bounded chunks.
