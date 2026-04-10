# Borromean Architecture And API

## Overview

Borromean splits storage behavior into three layers:

- ring-level storage rules in [../spec/ring.md](../spec/ring.md)
- implementation architecture and API constraints in [../spec/implementation.md](../spec/implementation.md)
- collection-specific payload rules such as [../spec/map.md](../spec/map.md)

The code follows the same split. Shared storage logic handles WAL sequencing, replay, reclaim, and committed-region writes. Collection code defines how a collection's payload bytes are interpreted and how its in-memory frontier merges with the retained durable basis.

## Supported API Surface

The Tier 1 supported API for readers and integrators is:

- `Storage` for formatting, opening, replaying, and mutating the store
- `FlashIo` for caller-owned device access
- `StorageWorkspace` for caller-owned scratch buffers
- `CollectionId` and `CollectionType` for stable collection identity
- `LsmMap` and `MapUpdate` for the durable map collection
- `MockFlash` for tests and examples

Everything else is documented as advanced reference material. Those modules are useful when inspecting implementation details, debugging traceability, or extending the engine, but they are not the primary onboarding path.

## Storage Lifecycle

The common storage flow is:

1. Construct a flash backend that implements `FlashIo`.
2. Allocate a `StorageWorkspace<REGION_SIZE>`.
3. Format or open the store through `Storage`.
4. Create or open a map collection.
5. Apply updates, snapshot the frontier, or flush it into a committed region.
6. Re-open the store later and reconstruct collection state from replay.

Two execution styles expose the same logic:

- Blocking methods do the whole operation immediately.
- Future-returning methods package the same operation as a caller-driven future.

Both styles keep I/O and workspace dependencies explicit in the function signatures.

## Durable Map Model

The durable map collection uses three payload shapes:

- update payloads in WAL records
- snapshot payloads in WAL records
- committed-region payloads in map regions

`LsmMap` keeps a bounded in-memory frontier in a caller-owned buffer. When a map is reopened:

- the retained durable basis is selected from the replay-tracked empty basis, snapshot basis, or region basis
- later retained update payloads are replayed in order
- later updates override older basis values for the same key

The normative byte-level rules for those payloads live in [../spec/map.md](../spec/map.md).

## Module Guide

- `src/lib.rs`: public crate entrypoint and ergonomic wrapper API
- `src/storage.rs`: shared runtime state and low-level WAL or reclaim operations
- `src/startup.rs`: replay and recovery logic used by open
- `src/collections/map/mod.rs`: map payload encoding, frontier logic, and map-specific storage helpers
- `src/mock.rs`: in-memory flash model used by tests and examples
- `src/disk.rs` and `src/wal_record.rs`: advanced reference surfaces for exact bytes on disk and in WAL records

## Experimental Surface

The exported `channel` module is still experimental. It has API documentation so readers can inspect the design, but it is not yet backed by the durable storage engine and is out of scope for the current collection specifications.
