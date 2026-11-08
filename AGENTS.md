# lsmkv: project boundaries and working agreement

- Work only in this repository. It is independent of sibling repositories.
- Do not read, modify, clean up, or run commands in sibling repositories or the user's configuration.
- Work autonomously: choose sensible defaults and document assumptions; do not ask questions or delegate.
- Never install anything globally or use sudo, Homebrew, launchd, host credentials, or host Docker sockets.
- Dependencies, toolchains, caches, browser binaries, SDKs, temporary files and test data stay in this repository. Preserve the supplied project-local environment. Python packages go in `.venv`.
- `.agent-local/` is ignored, project-local build/runtime state; preserve the supplied project-local environment.
- Use existing system tools read-only, or install pinned tools under this repository. Do not change shell profiles, user settings, Keychain, or global Git settings.
- Bind test servers to loopback; use ports 21700–21799. Limit build/test workers to two unless a required benchmark explicitly needs more.
- Docker-dependent integration tests must remain reproducible but be reported as unverified here if they require access outside this repository. Do not bypass this boundary.
- Do not touch attached hardware, enroll services, flash firmware, install agents, or alter OS settings. Supply manual checklists for such steps.
- Implement only the current numbered prompt, preserving prior behavior. Run appropriate checks, fix failures, and report genuine limitations without fabricating test or benchmark results.
- Before finishing every numbered turn: stop its servers/watchers, remove scratch outputs, update relevant documentation, and make a local conventional-commit Git commit. Do not push.
- End each turn with a short summary of changes, checks actually run, remaining limitations, and commit hash.

Extend this file with the project's language-specific architecture and verification commands while retaining these boundaries.

## Rust architecture and checks

- `src/lib.rs` exposes the database API; `src/main.rs` is the `lsmkv-cli` binary. The intended storage architecture and API assumptions are in `DESIGN.md`.
- Library code must never use `unwrap`, `expect`, or `panic!` outside tests.
- Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`, or run all three with `scripts/check.sh`. Use at most two build/test workers.

## Definition of done

For every future task, `scripts/check.sh` must pass, new code must have tests, and `DESIGN.md` must be updated if the design changed. Give a short summary of the changes, then make a local Git commit with a conventional-commit message; do not push.
