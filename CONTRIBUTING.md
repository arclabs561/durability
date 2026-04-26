# Contributing to durability

Thanks for your interest. durability provides crash-consistent persistence primitives: a directory abstraction, generic WAL, checkpoints, and recovery.

## Before you start

For non-trivial work (new storage backends, on-disk format changes, recovery-protocol changes), open an issue first to align on scope. Drive-by bug fixes and doc patches don't need an issue.

## Setup

- Rust toolchain: stable, MSRV `1.80`. Use `rustup` to manage.
- Optional: `cargo-nextest` for faster test runs (`cargo install cargo-nextest`).

```
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

## Style

- Direct, lowercase prose in commits. No marketing words ("powerful", "robust", "elegant"). No em-dashes in prose.
- Commit messages: `durability: short lowercase description`. One commit per logical change.
- `cargo fmt` and `cargo clippy --all-targets --all-features -- -D warnings` must pass before `git add`.

## Testing

- `cargo test --all-features` for the full matrix.
- Crash-consistency invariants: any change to WAL replay, checkpointing, or recovery needs a property test that exercises kill-then-recover. Don't add a code path that can succeed at write time but corrupt on recover.
- Format changes are append-breaking unless covered by a versioned reader. State the migration story in the PR.

## On-disk format

Anything that changes the binary layout of a WAL record, checkpoint, or directory entry is a breaking change for existing data. Mark such PRs explicitly and bump the major version.

## Pull requests

- Keep PRs scoped to one concern.
- Show before/after for behavior changes.
- Link the related issue.
- CI must be green before requesting review.

## License

Dual-licensed under MIT or Apache-2.0 at your option. By contributing you agree your contributions are licensed under both.
