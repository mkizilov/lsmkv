#!/bin/sh
set -eu

export CARGO_BUILD_JOBS=2
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
