---
status: proposed
date: 2026-07-09
scope: checkpoint and WAL on-disk formats
grounded-in:
  - /Users/arc/Documents/dev/_notes/perplexity-2026-07-09_160739-durability-segstore-critique-round-3-consumer-ecosystem.md
  - https://github.com/khonsulabs/okaywal
  - https://github.com/cberner/redb
  - https://docs.rs/sled/latest/sled/
  - https://fjall-rs.github.io/post/fjall-3/
  - https://github.com/fjall-rs/lsm-tree
---

# Design: Full-Frame Checksums

## Problem

`checkpoint` and `walog` currently CRC-check payload bytes, not the full durable
record boundary.

- Checkpoints store `last_applied_id` and `payload_len` in the header. A bit flip
  in `last_applied_id` can make recovery skip or replay the wrong WAL suffix
  while the payload CRC still passes.
- WAL frames store `length` and `entry_id` outside the CRC. Structural checks
  catch gaps, invalid lengths, and many torn-tail cases, but an `entry_id` bit
  flip that still matches the expected sequence can pass with a valid payload
  CRC.

The current checks are useful, but they are not a complete integrity proof for
the record boundary that recovery actually trusts.

## External Evidence

The adjacent Rust storage crates draw a useful boundary:

- `okaywal` is a richer WAL: segment files, fsync batching, automatic
  checkpointing, recovery callbacks, and per-chunk CRCs. Its lesson for
  `durability` is tighter frame validation, not importing the whole `LogManager`
  lifecycle.
- `redb` and `sled` are embedded databases. They own key-value APIs,
  transactions, MVCC or multi-tree semantics, reader visibility, and stable file
  formats. That scope belongs above `durability`.
- `fjall` is a log-structured key-value storage engine with versions, table/blob
  metadata, compaction, transactions, checksummed block/blob loads, and
  backpressure. `lsm-tree`, the lower-level crate beneath it, explicitly does not
  ship with a WAL. That supports keeping WAL/checkpoint primitives separate from
  LSM and database behavior.

## Decision

Keep `durability` a primitive layer. Do not add B-tree/LSM storage, query APIs,
transactions, MVCC, compaction policy, or reader visibility here.

For the next on-disk format revision, make the existing checksum fields cover
the durable record boundary:

- `checkpoint` writes a new format version whose `checksum` field is
  `crc32(header_prefix || payload)`, where `header_prefix` is:
  `magic | version | last_applied_id | payload_len`. The checksum field itself
  is excluded.
- `walog` writes a new WAL segment format version whose frame checksum is
  `crc32(length | entry_id | payload)`. The checksum field itself is excluded,
  so the frame size does not change.
- Readers should accept the previous format versions as legacy read-only inputs
  where feasible. New writes use the new checksum semantics.
- `recordlog` stays out of the first change unless tests show a semantic header
  field that can corrupt recovery while preserving the payload CRC. Its header
  has length plus checksum, not a trusted `last_applied_id` or `entry_id`.

## Rejected Options

### Keep Payload-Only CRCs

Rejected. Payload-only CRCs protect decoded values but not the recovery metadata
that decides where replay starts and what order entries have.

### Add a Separate Header CRC Field

Rejected for the first revision. It expands every frame and forces more parser
changes while still requiring a format bump. Reusing the existing CRC field with
new coverage gives the main integrity gain with less layout churn.

### Adopt an Embedded Database Crate

Rejected. `redb`, `sled`, and `fjall` solve broader key-value storage problems.
`segstore`, `postings`, and the consumer crates already own domain-specific
segment and query behavior; replacing `durability` with a database would move
policy into the wrong layer.

### Adopt OkayWAL Wholesale

Rejected for now. `okaywal` has useful ideas around fsync batching and recovery
callbacks, but adopting its lifecycle would duplicate existing `publish` and
`recover` contracts. The immediate issue is checksum coverage, not WAL ownership.

## Implementation Gates

1. Add failing tests that corrupt checkpoint `last_applied_id`, checkpoint
   `payload_len`, WAL `entry_id`, and WAL `length` while leaving payload bytes
   unchanged.
2. Add version-aware decode paths for legacy checkpoint and WAL formats.
3. Switch new checkpoint and WAL writes to the full-frame checksum semantics.
4. Extend fuzz/property tests so random header corruption either errors or
   yields a documented torn-tail prefix, never a trusted wrong metadata value.
5. Publish as a minor `0.x` release because new writes use a new on-disk format.

## Non-Goals

- No cross-process locking change.
- No transaction or MVCC API.
- No compaction, LSM table, B-tree, or query-index implementation.
- No migration tool in the first pass; reading legacy files is enough for
  compatibility, and rewriting checkpoints or rotating WAL segments naturally
  moves active stores to the new format.
