# Map Collection Specification

## Purpose

This specification defines the durable payload semantics for the
Borromean map collection. Shared storage ordering, replay, reclaim, WAL
layout, and committed-region mechanics remain defined by
[spec/ring.md](ring.md). Implementation-level architecture and API
constraints remain defined by
[spec/implementation.md](implementation.md).

This document is the normative specification required by
`RING-FORMAT-012` and `RING-FORMAT-013` for the only non-WAL collection
type that the current implementation supports durably.

## Empty Logical State

The map collection represents a partial function from keys `K` to values
`V`. The empty logical state contains no visible keys.

1. `MAP-STATE-001` After a durable
`new_collection(collection_id, MAP_CODE)` basis, opening the collection
MUST yield an empty logical map.
2. `MAP-STATE-002` `LsmMap::new` MUST construct the same empty logical
state used by an empty durable map basis.

## Snapshot Payload Format

A snapshot payload is a compact complete basis for the logical map.

1. `MAP-SNAPSHOT-001` A map snapshot payload MUST be encoded as
`[entry_count:u32 little-endian][entry_bytes_len:u32 little-endian][entry_bytes][entry_refs]`.
2. `MAP-SNAPSHOT-002` Snapshot encoding MUST write `entry_count` as the
number of visible entries in the logical map and `entry_bytes_len` as
the exact byte length of the compact serialized entry data that follows.
3. `MAP-SNAPSHOT-003` Loading a valid snapshot payload MUST reconstruct
the same logical key/value visibility encoded by that payload.
4. `MAP-SNAPSHOT-004` Snapshot loaders MUST treat `entry_refs` as an
ordered, non-overlapping description of the compact entry bytes.

## Update Payload Format

Map updates represent logical mutations layered over the retained basis.

1. `MAP-UPDATE-001` A map update payload MUST be the exact `postcard`
serialization of `MapUpdate<K, V>`.
2. `MAP-UPDATE-002` Applying a `Set` update payload MUST make the key
visible with the supplied value, and applying a `Delete` update payload
MUST make the key absent from the frontier.

## Committed Region Format

The current implementation supports one committed-region map format,
`MAP_REGION_V1_FORMAT`.

1. `MAP-REGION-001` A committed map region with
`collection_format = MAP_REGION_V1_FORMAT` MUST encode its payload as
`[snapshot_len:u32 little-endian][snapshot_payload]`.
2. `MAP-REGION-002` The `snapshot_len` prefix MUST equal the exact byte
length of the embedded snapshot payload used as the region's durable
basis.
3. `MAP-REGION-003` Loading a valid committed region payload MUST
reconstruct the same logical state as loading its embedded snapshot
payload.

## Merge And Frontier Rules

The map collection uses a durable basis plus a mutable frontier.

1. `MAP-MERGE-001` When opening a live map collection, the retained
durable basis MUST be selected from the replay-tracked empty basis,
retained snapshot basis, or retained committed-region basis, and any
later retained update payloads for that collection MUST then be applied
in replay order.
2. `MAP-MERGE-002` Later retained updates MUST take precedence over
older values from the retained basis for the same key.
3. `MAP-MERGE-003` Flushing a mutable map frontier MUST write a new
immutable committed region rather than rewriting the previous live
region in place.

## Validation And Open Rules

Map-specific validation is authoritative for live map collections.

1. `MAP-VALIDATE-001` Map snapshot loading MUST reject payloads whose
lengths, entry ranges, ordering, or entry decoding are invalid.
2. `MAP-VALIDATE-002` Opening or loading a live map collection MUST
reject retained collection state whose `collection_type` is not
`MAP_CODE`.
3. `MAP-VALIDATE-003` Opening or loading a live map collection MUST
reject retained committed-region bases whose `collection_format` is not
`MAP_REGION_V1_FORMAT`.
4. `MAP-VALIDATE-004` Opening a live map collection MUST reject
retained committed-region payloads, snapshot payloads, or update
payloads that fail map-specific validation.
