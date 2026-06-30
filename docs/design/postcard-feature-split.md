# Design: Postcard feature split

## Problem

`durability` used postcard as an unconditional dependency even though its lowest
layers already frame and validate raw bytes. Users that only need `Directory`,
raw record logs, raw checkpoints, mmap helpers, or WAL maintenance had no way to
avoid compiling postcard and its transitive dependencies.

## Research

Sources:

- Cargo Book, "Features": https://doc.rust-lang.org/cargo/reference/features.html
- Cargo Book, "SemVer Compatibility": https://doc.rust-lang.org/cargo/reference/semver.html
- Cargo Book, "Dependency Resolution": https://doc.rust-lang.org/cargo/reference/resolver.html#features
- docs.rs, "Metadata for custom builds": https://docs.rs/about/metadata
- `syn` manifest: https://github.com/dtolnay/syn/blob/master/Cargo.toml
- `regex` manifest: https://github.com/rust-lang/regex/blob/master/Cargo.toml
- `quick-xml` manifest: https://github.com/tafia/quick-xml/blob/master/Cargo.toml

Primary source read-depth:

- Cargo Book, "Features": read the page end to end, including feature
  definition, optional dependency, `dep:` syntax, dependency feature syntax,
  default feature behavior, feature unification, resolver version notes,
  feature documentation, docs.rs metadata pointers, and feature-combination
  testing.
- Cargo Book, "SemVer Compatibility": read the Cargo feature and optional
  dependency guidance, the 0.x versioning convention, and the
  item-removal/cfg-gating section.
- Cargo Book, "Dependency Resolution": read the feature-resolution sections,
  including lockfile resolution, feature unification, resolver v2 exceptions,
  and feature removal effects.
- docs.rs, "Metadata for custom builds": read the available metadata fields for
  selecting all features, no-default features, targets, and rustdoc args.
- Implementation examples: read `syn`, `regex`, and `quick-xml` manifests for
  feature naming, `dep:` usage, feature docs, docs.rs all-feature builds, weak
  dependency feature forwarding, and feature-gated tests/examples.
- Not read: Cargo source code, the full Rust API Guidelines, and a broad survey
  of codec crates.

The relevant Cargo constraints are: features should be additive; optional
dependencies are the right mechanism for dependency opt-outs; `dep:` avoids
exposing dependency names accidentally; and moving existing public code behind a
feature is semver-sensitive.

Deeper implications:

- `postcard` is a public feature contract once published. That is acceptable
  here because the gated API names and behavior are postcard-specific. If a
  future release wants a generic typed codec surface, that should be a new
  feature/API rather than silently repurposing `postcard`.
- Keeping `postcard` in `default` preserves default-build compatibility, but
  removing it from `default` later would itself be a SemVer-sensitive change.
- Feature unification means `default-features = false` only proves this crate's
  direct no-default surface. Another dependency path can still enable defaults.
  `cargo tree -e features` is the right diagnostic when users report postcard in
  a larger graph.
- docs.rs should build with all features so gated modules and methods are
  visible, while CI should also build no-default docs because broken rustdoc
  links can appear only when `postcard` is disabled.

## Chosen approach

Make `postcard` an optional dependency behind a default-enabled `postcard`
feature, while promoting raw byte APIs to first-class public methods. Default
builds keep the existing typed serde/postcard API. `default-features = false`
builds retain raw checkpoint, recordlog, WAL append/replay, WAL maintenance, and
storage/sync primitives without compiling postcard.

## Non-goals

- Do not make `serde` optional in this pass. `WalEntry` and the typed recovery
  surface still derive/accept serde types; removing serde is a broader API split.
- Do not change the on-disk WAL, checkpoint, or recordlog frame formats. Payload
  bytes stay payload bytes; postcard is only one encoding of those bytes.
- Do not remove typed recovery or publish APIs from default builds. Existing
  users should not need a manifest change unless they intentionally disable
  default features.
- Do not add a new codec abstraction trait. The raw byte APIs are enough for
  callers that want JSON, bincode, postcard, zerocopy, or hand-rolled bytes.

## Options considered

1. Keep postcard unconditional.
   This preserves the smallest diff but leaves the raw layers unable to stand on
   their own, contradicting the crate's "primitives" shape.

2. Make postcard optional without raw APIs.
   This technically removes a dependency, but the no-default build would lose the
   useful typed methods and lack equivalent raw checkpoint/WAL entry points.

3. Make postcard optional and add raw APIs.
   This keeps default behavior stable while making the dependency opt-out real.
   This is the chosen option.

## Tradeoffs

The no-default build now has a smaller API surface: typed append/replay,
`recover`, and `publish` are behind `postcard`. That is acceptable because those
APIs are explicitly serde/postcard conveniences. The change is semver-sensitive
for users that currently set `default-features = false` and still call typed
methods, so it should ship in a minor 0.x release rather than a patch release.

## Implementation plan

1. Add raw checkpoint APIs and make postcard checkpoint methods wrappers.
2. Add raw WAL append/replay APIs and make typed WAL methods require `postcard`.
3. Gate `recover` and `publish` behind `postcard`.
4. Add CI coverage for `cargo test --no-default-features --lib`.
5. Document feature behavior in README and changelog.

## Decision gates

- If a caller needs typed recovery without postcard, add a codec trait or
  callback-based recovery API in a new design.
- If `serde` becomes the dominant remaining dependency concern, design a separate
  serde split; do not fold it into the postcard change retroactively.
- If no-default users report that hiding `recover` is too coarse, add a raw
  recovery helper rather than reintroducing postcard as unconditional.

---
Decided: 2026-06-29 | Session: Codex handoff from Claude 03edba07
