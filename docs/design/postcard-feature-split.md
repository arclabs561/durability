# Design: Postcard feature split

## Problem

`durability` used postcard as an unconditional dependency even though its lowest
layers already frame and validate raw bytes. Users that only need `Directory`,
raw record logs, raw checkpoints, mmap helpers, or WAL maintenance had no way to
avoid compiling postcard and its transitive dependencies.

## Research

Primary source read-depth:

- Cargo Book, "Features": read the feature definition, optional dependency,
  default feature, feature unification, and feature-combination testing sections.
- Cargo Book, "SemVer Compatibility": read the Cargo feature and optional
  dependency guidance, plus the item-removal/cfg-gating section.
- Not read: the full Cargo resolver chapter and the full Rust API Guidelines.

The relevant Cargo constraints are: features should be additive; optional
dependencies are the right mechanism for dependency opt-outs; `dep:` avoids
exposing dependency names accidentally; and moving existing public code behind a
feature is semver-sensitive.

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
