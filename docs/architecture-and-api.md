# Borromean Architecture And API

## Overview

Borromean splits storage behavior into three layers:

- ring-level storage rules in [../spec/ring.md](../spec/ring.md)
- implementation architecture and API constraints in
  [../spec/implementation.md](../spec/implementation.md)
- collection-specific payload rules such as [../spec/map.md](../spec/map.md)

The code follows the same split. Shared storage logic handles WAL sequencing, replay, reclaim, and
committed-region writes. Collection code defines how a collection's payload bytes are interpreted
and how its in-memory frontier merges with the retained durable basis.

## Supported API Surface

The Tier 1 supported API for readers and integrators is:

- `Storage` for formatting, opening, replaying, and mutating the store
- `FlashIo` for caller-provided device access bound into `Storage`
- `CollectionId` and `CollectionType` for stable collection identity
- `LsmMap` and `MapUpdate` for the durable map collection
- `MockFlash` for tests and examples

Everything else is documented as advanced reference material. Those modules are useful when
inspecting implementation details, debugging traceability, or extending the engine, but they are not
the primary onboarding path.

## Storage Lifecycle

The common storage flow is:

1. Construct a flash backend that implements `FlashIo`.
2. Format or open the store through `Storage`, which binds exclusive mutable access to the backend.
3. Create or open a map collection.
4. Apply updates, snapshot the frontier, or flush it into manifest-backed
   committed runs.
5. Re-open the store later and reconstruct collection state from replay.

Two execution styles expose the same logic:

- Blocking methods do the whole operation immediately.
- Future-returning methods package the same operation as a caller-driven future.

Both styles drive the same storage context. Normal operations use the backing and reusable scratch
owned by `Storage`; low-level runtime helpers may still expose `StorageWorkspace` for internal and
test-support code.

## Durable Map Model

The implemented durable map collection uses three supported live payload
families:

- update payloads in WAL records
- snapshot payloads in WAL records
- manifest and immutable run segment payloads in committed map regions

`MAP_REGION_V1_FORMAT`, the earlier single-region committed map format, is
retired. Helpers still cover its payload shape for historical tests, but a
live map collection that uses it as the committed durable basis is rejected.

`LsmMap` is the small object-level public handle for normal map use.
`MapFrontier` is the advanced caller-buffer frontier used by lower-level
storage helpers and tests. Open and read paths do not materialize committed
runs into RAM; they keep bounded run descriptors and read candidate run regions
on demand. When a map is reopened:

- the retained durable basis is selected from the replay-tracked empty basis, snapshot basis, or
  region basis
- a manifest region is parsed into newest-to-oldest immutable run descriptors
- later retained update payloads are replayed into the frontier in order
- reads check the frontier first, then read candidate run regions on demand

The normative byte-level rules for those payloads live in [../spec/map.md](../spec/map.md).

Flushes now write immutable sorted run regions, stage them, write a manifest,
and commit the manifest as the collection head. Reads use newest-wins semantics:
newer runs may overlap older runs, and a newer tombstone masks older values.
Compaction is deferred and explicit: `get`, `set`, and `delete` do not perform
whole-run compaction inline. `set` and `delete` may report that compaction is
needed, and callers can then invoke `Storage::compact_map` or `LsmMap::compact`
as a separate operation using the Target-Then-Greedy selection policy.

## Module Guide

- `src/lib.rs`: public crate entrypoint and ergonomic wrapper API
- `src/storage.rs`: shared runtime state and low-level WAL or reclaim operations
- `src/startup.rs`: replay and recovery logic used by open
- `src/collections/map/mod.rs`: map payload encoding, frontier logic, and map-specific storage
  helpers
- `src/mock.rs`: in-memory flash model used by tests and examples
- `src/disk.rs` and `src/wal_record.rs`: advanced reference surfaces for exact bytes on disk and in
  WAL records

## Contributor Guide

If you are adding a new durably integrated collection type, start with
[implementing-a-collection.md](./implementing-a-collection.md). That tutorial walks through the
current integration points in `lib`, `storage`, `startup`, and the collection module itself.

## Experimental Surface

The exported `channel` module is still experimental. It is covered by
[../spec/channel.md](../spec/channel.md), but it is not yet backed by the
durable storage engine.
