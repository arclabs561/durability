---
status: proposal
scope: WAL and recovery hardening
grounded-in:
  - README.md
  - Cargo.toml
  - CHANGELOG.md
  - commits 9cc17a6..b83c7de
  - local session reports in .claude/reports/research-2026-06-26*.md
review_trigger:
  - before the next release tag after v0.6.6
  - before adding WAL lifecycle metadata
  - before changing SyncWalWriter from mutex serialization to true group commit
---

# WAL Recovery Hardening Roadmap

## Current Position

There is no tracked ADR or design ledger for this crate yet. This proposal is the
first tracked planning artifact for the current WAL/recovery hardening thread.
The local `.claude/reports/` files informed this plan, but they are ignored and
are not part of a fresh clone.

The current README settles these contracts:

- `WalWriter<E>` and `WalReader<E>` are generic over serde entry types.
- `SyncWalWriter` is a mutex wrapper. `append_durable` appends one entry and
  syncs it while holding that mutex; it does not claim leader/follower group
  commit.
- Writes are buffered by default. Callers use `flush_and_sync()` for a durability
  barrier.
- Failed fsync poisons the writer. Callers restart from WAL rather than repair
  the writer in place.
- Multi-process locking is not provided. The lockfile catches in-process
  double-instantiation only.

The recent hardening commits are done and should not be re-planned:

- `9cc17a6`: point-in-time recovery stops WAL replay before ignored payloads.
- `fba52ec`: property and fuzz coverage exercise bounded recovery.
- `a113311`: checkpoint replay boundary mutation is killed.
- `c954b7a`: WAL payload-size cap is pinned on both sides of 16 MiB.
- `b83c7de`: WAL replay and strict segment range scans ignore directory noise.

Source verification also found these already-covered cases:

- Internal WAL segment ID gaps are rejected by `wal_reader_rejects_internal_segment_gap`.
- Checkpoint publish failure tests already cover marker append failure,
  durability-proof failure, and truncation failure.
- Orphan checkpoint files without a WAL checkpoint marker are already covered.
- Segment recycling has unit tests for enabled, disabled, pool eviction, and
  missing recycled paths.

## Workstreams

### 1. Release the current hardening as a patch

Consumer: current crate users who need the WAL/recovery correctness fixes that
landed after `v0.6.6`.

Deletion test: do not add more API before this release. The existing work is
already committed, tested, and CI-green; delaying the patch release only keeps
known hardening local to git users.

Gate: release checklist passes, version/changelog are updated once, `cargo
publish --dry-run` succeeds, the tag is pushed, and GitHub Actions is green for
the release commit.

Reversibility: partially reversible. A published crate version cannot be edited,
but a bad patch can be yanked and followed by another patch.

### 2. Add absence-state crash models around checkpoint/WAL boundaries

Consumer: recovery callers depending on the prefix property across checkpoint
publish and WAL truncation boundaries.

Scope: tests first. Existing tests cover common publish failures, orphaned
checkpoint files, and missing internal WAL segments. The remaining gap is a
model that treats persistence as a subset of objects after a crash: checkpoint
file present or absent, WAL checkpoint marker present or absent, prefix segment
deleted or retained, and suffix segment present or absent.

Deletion test: no production code should change unless this model finds a real
counterexample. The first deliverable is a test/fault harness.

Status: the silent-skip cell of the matrix is closed. When a checkpoint newer
than the surviving one is missing AND the WAL does not provably start at entry
1, `latest_checkpoint_from_wal` now refuses rather than falling back to the
older checkpoint (which could skip entries a prefix truncation dropped). This
is the Postgres-in-v11 choice: no automatic secondary-checkpoint fallback
across an unproven prefix. The broader object-subset matrix (checkpoint file,
WAL marker, prefix segment, suffix segment each present/absent) is still open;
the RocksDB alternative, tracking WAL identity + synced size in the manifest so
gaps are *detectable* rather than *refused*, remains a larger follow-up.

Gate: a property or matrix test enumerates valid object-subset outcomes for
checkpoint publish and recovery, and it passes under the default `cargo test`
gate.

Reversibility: reversible.

### 3. Add truncate/recycle/resume properties

Consumer: users that enable segment recycling or call `WalMaintenance` after a
checkpoint.

Scope: property tests over random segment boundaries, checkpoint cutoffs,
recycle pool sizes, restart/resume, and subsequent appends. The invariant is
that recycled files never replay under stale identity and entry IDs continue as
a strict suffix after resume.

Deletion test: keep recycling opportunistic and off by default. Do not promote
it into a stronger durability contract unless tests show the current contract is
too vague for callers.

Gate: property coverage exercises truncate-to-recycle followed by resume and
strict replay, with at least one mutation check against the segment identity or
entry-ID boundary.

Reversibility: reversible if this stays in tests; partially reversible if it
changes the public recycling contract.

### 4. Decide whether WAL lifecycle metadata is needed

Consumer: future maintainers if the current filename-plus-strict-gap model
becomes too weak for checkpoint publish, recycling, or repair.

This is a fork, not implementation. The current design rejects internal segment
ID gaps and treats recycling as caller-managed reuse of truncated files. That is
acceptable unless a test demonstrates that the crate needs durable lifecycle
state beyond filenames and segment headers.

Gate: ADR or design record accepted before adding a manifest, lifecycle log, or
other persisted metadata.

Reversibility: one-way-ish. A persisted metadata format creates compatibility
obligations once released.

### 5. Decide whether true group commit belongs in this crate

Consumer: multi-threaded write-heavy callers, if they exist.

The current `SyncWalWriter` is honest mutex serialization. Earlier research
shows leader/follower write barriers in RocksDB, TiKV raft-engine, and okaywal,
but adding that machinery would change the concurrency model and test surface.

Deletion test: do not implement group commit because the literature says it is
useful. Require either a benchmark that shows the mutex path is a bottleneck for
a target workload, or a user-visible API need such as durable tickets.

Gate: ADR or design record accepted with the chosen concurrency contract,
including how waiters observe durability and how fsync failure poisons pending
writes.

Reversibility: partially reversible before release, hard to unwind after public
API exposure.

## Sequencing

1. Phase 0, release gate: finish the patch release for the already-landed
   hardening. Do not mix new code into the release commit.
2. Phase 1, absence-model gate: add checkpoint/WAL object-subset crash tests.
   Continue only if they pass or expose a concrete bug.
3. Phase 2, recycle-property gate: add truncate/recycle/resume property and
   mutation coverage. Continue only after default tests and the targeted mutant
   check pass.
4. Phase 3, lifecycle fork gate: decide whether filename-plus-strict-gap remains
   the contract or a WAL lifecycle manifest is needed.
5. Phase 4, concurrency fork gate: decide whether to keep mutex serialization or
   design a true group-commit writer.
6. Phase 5, next release gate: release only after the chosen test/API changes
   are committed, CI is green, and the changelog says exactly what changed.

## Decision Forks

### WAL lifecycle metadata

Governs: `src/walog.rs`, `src/recover.rs`, `src/publish.rs`, `tests/fault_*`,
`tests/prop_*`.

Question: should WAL segment lifecycle stay implicit in filenames and headers,
or should the crate persist a manifest/lifecycle record?

Options:

- Keep filename-plus-strict-gap. Lowest complexity and already tested for
  internal gaps, but recovery repair remains intentionally narrow.
- Add a WAL manifest or lifecycle log. Stronger state model for recycling and
  repair, but it creates a new persisted format and migration path.

Recommended pick for now: keep filename-plus-strict-gap and add the tests in
phases 1 and 2. Revisit only if those tests find a counterexample.

### Segment recycling contract

Governs: `WalMaintenance::truncate_to_recycle`,
`WalWriter::set_recycle_capacity`, `WalWriter::recycle_segment`.

Question: is recycling only a best-effort performance optimization, or a
formal reusable-segment pool with durable identity guarantees?

Options:

- Keep recycling opportunistic and opt-in. Simpler, lower API burden, and
  consistent with the current README.
- Promote recycling to a formal persisted pool. More observable and easier to
  reason about after crash, but likely needs lifecycle metadata.

Recommended pick for now: keep it opportunistic and prove stale identity cannot
replay through the property suite.

### SyncWalWriter durability semantics

Governs: `SyncWalWriter`, README thread-safe writer section, concurrency tests.

Question: should `SyncWalWriter` stay a mutex wrapper, or grow a durable ticket
or leader/follower write-barrier API?

Options:

- Keep mutex serialization. Small API, current docs match implementation, and
  correctness remains clear.
- Add true group commit. Better throughput for concurrent durable writers, but
  it needs a new durability notification protocol and fsync-failure fanout rules.

Recommended pick for now: keep mutex serialization unless a benchmark or
consumer proves the throughput need.

## Guardrails

Do not start phase 3 until the absence-model and recycle-property tests have
shown whether WAL lifecycle metadata is necessary.

Do not start a group-commit implementation until the `SyncWalWriter` durability
semantics fork has an accepted ADR or design record.

Do not publish the next tag until the release checklist has been run against the
exact release commit and the pushed GitHub Actions run is green.
