# Chapter 4: WAL Model And Records

This chapter defines the append-only WAL record stream, its physical
encoding, and the validation rules that make `ApplyWalRecord` safe to
use during foreground operation, WAL-head reclaim, and startup replay.

Mechanism review:

- **Purpose**: make every replay-visible decision append-only and
  ordered while distinguishing valid records, unwritten space, and torn
  or corrupt spans.
- **State**: WAL head/tail, tail append offset, encoded record
  alignment, and the pending WAL-recovery boundary.
- **Named operations**: `AppendRawWalRecord`, `RotateWalTail`,
  `CommitWalRecovery`, and the WAL-record-preservation edges used by
  `ReclaimWalHead`.
- **Durable edge sequence**: each record is written at an aligned WAL
  slot and becomes visible only after its sync boundary.
- **Replay effect**: every valid retained record is interpreted through
  `ApplyWalRecord`; invalid complete records are corruption rather than
  optional skips.
- **Crash cuts**: torn records are ignored by scanning rules, but later
  valid records after a torn span require `wal_recovery`.

## WAL Record Types

All WAL records are append-only and ordered by physical write order
within the WAL region chain.

WAL record encoding and alignment:

Let `wal_record_area_offset` be the first offset within a WAL region
that is both:
past the end of the region `Header` plus `WalRegionPrologue`; and
aligned to `wal_write_granule`.
Replay and append scanning consider candidate WAL record starts only at
aligned offsets greater than or equal to `wal_record_area_offset`.

Let `wal_escape_byte`, `wal_escape_code_erased`,
`wal_escape_code_magic`, and `wal_escape_code_escape` be the first four
byte values in ascending order that are distinct from both
`erased_byte` and `wal_record_magic`. Because only those two byte
values are reserved globally, such a four-byte choice always exists.

1. `RING-WAL-ENC-001` Every physical WAL record MUST begin with a
one-byte `record_magic`.
2. `RING-WAL-ENC-002` `record_magic` MUST equal the storage's configured
`wal_record_magic`, and `wal_record_magic` must not equal
`erased_byte`, the byte value returned by erased flash.
3. `RING-WAL-ENC-003` After the leading `record_magic`, the rest of the
physical WAL
record is encoded with deterministic byte-stuffing over the logical WAL
record bytes:
for a logical byte equal to `erased_byte`, emit
`wal_escape_byte wal_escape_code_erased`;
for a logical byte equal to `wal_record_magic`, emit
`wal_escape_byte wal_escape_code_magic`;
for a logical byte equal to `wal_escape_byte`, emit
`wal_escape_byte wal_escape_code_escape`;
all other logical bytes are emitted unchanged.
4. `RING-WAL-ENC-004` During decoding, any `wal_escape_byte` in the
encoded body MUST be
followed by exactly one of
`wal_escape_code_erased`, `wal_escape_code_magic`, or
`wal_escape_code_escape`; any other follower byte is corruption.
5. `RING-WAL-ENC-005` Every byte after the leading `record_magic` in a
valid encoded WAL
record therefore differs from both `erased_byte` and
`wal_record_magic`.
6. `RING-WAL-ENC-006` After the full logical record through
`record_checksum` has been
decoded, any remaining bytes up to the aligned physical record end are
padding. Those padding bytes MUST all equal
`wal_escape_code_escape`.
7. `RING-WAL-ENC-007` Every WAL record start offset within a WAL region
MUST be aligned to
`wal_write_granule`, the smallest writable unit of the backing flash.
8. `RING-WAL-ENC-008` The encoded size of every WAL record MUST be
rounded up to a multiple of
`wal_write_granule`. Replay advances from one candidate record start to
the next in aligned `wal_write_granule` steps.
9. `RING-WAL-ENC-009` At an aligned candidate record start in a
reachable WAL region:
if the first byte is `erased_byte`, that slot is currently unwritten and
marks the end of the written portion of that WAL region;
if the first byte is `wal_record_magic`, that slot is a candidate WAL
record and must parse and validate normally;
if the first byte is neither, that slot lies inside a torn/corrupt WAL
record, so replay keeps scanning forward by aligned
`wal_write_granule` steps and ignores the corrupt bytes.
10. `RING-WAL-ENC-010` The recovered append point for the tail region
MUST be the first aligned
slot whose first byte is `erased_byte` after the last valid replayed
tail record. If no such slot exists, the tail region is currently full
and the next WAL append must rotate via `link` to a new WAL region.
11. `RING-WAL-ENC-011` Let `wal_link_reserve` be the aligned encoded
size needed in the
current tail region to append the trailing
`link(next_region_index, expected_sequence)` record that completes WAL
rotation.
12. `RING-WAL-ENC-012` Let `wal_rotation_reserve` be the total aligned
encoded size needed
in the current tail region to append the two WAL records required to
start and complete rotation to a new tail region:
`alloc_begin(collection_id = 0, next_region_index, free_list_head_after)` followed by
`link(next_region_index, expected_sequence)`.
13. `RING-WAL-ENC-013` Appending any WAL record to the current tail
region, other than the
specific `alloc_begin(collection_id = 0, next_region_index,
free_list_head_after)` that starts WAL rotation or the trailing `link`,
is invalid if doing so would leave fewer than `wal_rotation_reserve`
unwritten bytes in that region.
14. `RING-WAL-ENC-014` Appending the
`alloc_begin(collection_id = 0, next_region_index, free_list_head_after)`
that starts WAL rotation is invalid unless its aligned end offset still
leaves at least `wal_link_reserve` and fewer than
`wal_rotation_reserve` unwritten bytes in that region. This
reserve-window placement makes an unmatched tail `alloc_begin`
unambiguously recognizable as the WAL-rotation-start record during
startup recovery. Once that rotation `alloc_begin` is durable, the only
valid later WAL record in that region is the matching trailing `link`.

Each WAL record encodes the following fields:

1. `RING-WAL-FIELD-001` `record_type`: one of `new_collection`, `update`, `snapshot`,
`alloc_begin`, `head`, `drop_collection`, `link`, `wal_recovery`,
`free_region`, `begin_transaction`, `commit_transaction`,
`transaction_finished`, or `rollback_transaction`
2. `RING-WAL-FIELD-002` `collection_id`: required for `new_collection`, `update`,
`snapshot`, `alloc_begin`, `head`, `drop_collection`, `free_region`,
`begin_transaction`, `commit_transaction`, `transaction_finished`, and
`rollback_transaction`
3. `RING-WAL-FIELD-003` `collection_type`: required for `new_collection`, `snapshot`, and
`head`; omitted for `update`, `alloc_begin`, `drop_collection`, `link`,
`wal_recovery`, `free_region`, and transaction marker records
4. `RING-WAL-FIELD-004` `payload_len`: payload size in bytes
5. `RING-WAL-FIELD-005` `payload`: opaque bytes defined by `record_type`
6. `RING-WAL-FIELD-006` `free_list_head_after`: required for `alloc_begin`; omitted for
all other record types
7. `RING-WAL-FIELD-007` `record_checksum`: checksum covering the full logical record before
byte-stuffing encoding
8. `RING-WAL-FIELD-008` `padding`: zero or more trailing `wal_escape_code_escape` bytes so
the physical encoded record size is a multiple of `wal_write_granule`

Logical WAL record byte layout before byte-stuffing:

```text
LogicalWalRecord =
  record_type:u8
  [collection_id:u64 if required by record_type]
  [collection_type:u16 if required by record_type]
  payload_len:u32
  payload:[u8; payload_len]
  [free_list_head_after:OptRegionIndex if record_type = alloc_begin]
  record_checksum:u32
```

1. `RING-WAL-LAYOUT-001` `record_type` MUST use these canonical byte
codes:
`new_collection = 0x01`,
`update = 0x02`,
`snapshot = 0x03`,
`alloc_begin = 0x04`,
`head = 0x05`,
`drop_collection = 0x06`,
`link = 0x07`,
`wal_recovery = 0x0b`,
`free_region = 0x0c`,
`begin_transaction = 0x0d`,
`commit_transaction = 0x0e`,
`transaction_finished = 0x0f`,
`rollback_transaction = 0x10`.
2. `RING-WAL-LAYOUT-002` The logical field order before byte-stuffing
MUST be exactly the order shown above.
3. `RING-WAL-LAYOUT-003` `payload_len` MUST equal the number of logical
payload bytes only. It MUST exclude omitted optional fields,
`record_checksum`, the physical leading `record_magic`, and any
physical padding.
4. `RING-WAL-LAYOUT-004` `record_checksum` MUST be CRC-32C over the
logical WAL record bytes from `record_type` through the final byte of
the last field preceding `record_checksum`.
5. `RING-WAL-LAYOUT-005` Record types whose payload is empty
(`new_collection`, `drop_collection`, `wal_recovery`, and transaction
marker records) MUST still encode `payload_len = 0`.
6. `RING-WAL-LAYOUT-006` Payload bytes are encoded canonically by record
type:
`update` and `snapshot` payloads are opaque collection-defined bytes;
`alloc_begin`, `head`, and `free_region` payloads are a single
`u32 region_index`;
`link` payload is `next_region_index:u32` followed by
`expected_sequence:u64`;
`new_collection`, `drop_collection`, `wal_recovery`, and transaction
marker payloads are empty.

The record payloads are:

1. `RING-WAL-PAYLOAD-001` `new_collection`
Declares a new user collection with the given `collection_id` and
`collection_type`. Payload is empty. The record is the durable basis
decision for an empty collection with no committed regions, no
snapshots, and no updates in its durable basis.

2. `RING-WAL-PAYLOAD-002` `update`
Collection-local mutation delta. Applied in WAL order during replay.

3. `RING-WAL-PAYLOAD-003` `snapshot`
Full logical state for one collection at a point in time, tagged with
the collection type for that snapshot basis. Supersedes older `update`
records for that collection that appear before the snapshot.

4. `RING-WAL-PAYLOAD-004` `alloc_begin`
Reserves the current free-list head region for imminent use by
`collection_id`. The payload contains the reserved `region_index`.
The record stores `free_list_head_after`, the next free region after
removing `region_index` from the free list. Once `alloc_begin` is
durable, allocator replay state advances even if the reserved region
is erased before a later `head` or `link` record uses it.
When written, `region_index` must equal the durable free-list head in
replay order, and `free_list_head_after` must be the successor that was
observed from that head's free-pointer chain at allocation time.
`alloc_begin(collection_id, region_index, free_list_head_after)` has
two replay-visible effects:

- It advances the durable free-list head to `free_list_head_after`.
- If `collection_id = 0`, it reserves `region_index` as `ready_region`
  until the matching WAL `link(... next_region_index = region_index ...)`
  consumes it. This reservation exists only for WAL rotation recovery.
  User collection allocations are transaction-owned instead; they advance
  the allocator but do not occupy the global `ready_region` slot.

5. `RING-WAL-PAYLOAD-005` `head`
Commits a collection to a durable region head. Payload contains
the target `region_index`. The record also carries the collection type for
that durable region basis. When `collection_id = 0`, this record
commits a new WAL head region; there is no distinct WAL-head record
type. If `region_index` equals the currently reserved `ready_region`,
the `head` consumes that reservation. Otherwise, the `head` retargets
the logical collection head to
an already allocated existing region. Before appending such a
retargeting `head`, the implementation must validate that the target
region's header has the same `collection_id` and that the target
region is not currently free. For user collections, borromean core
does not impose a global mapping from `collection_format` to
`collection_type`; the collection implementation owns that
interpretation. Replay does not revalidate that not-free append-time
invariant; this is an intentional application of the checksum trust
model defined below.

6. `RING-WAL-PAYLOAD-006` `drop_collection`
Payload is empty. Durably tombstones a user collection. The record
detaches that collection's current durable basis from the live
namespace, discards any pending WAL updates for that collection, and
forbids any later WAL record for the same `collection_id`.
Previously live WAL snapshots or committed regions for that collection
become reclaimable once collection-specific cleanup removes any
remaining physical references to them. Any region associated with that
dropped collection may be added to the free list through
`free_region(collection_id, region_index)` if it is not already
reachable from the free-list chain.

7. `RING-WAL-PAYLOAD-007` `link`
Points from a full WAL region to the next WAL region. Payload contains
`next_region_index` and `expected_sequence` for the next WAL region
header.

8. `RING-WAL-PAYLOAD-008` `wal_recovery`
Payload is empty. Marks that replay or a prior open detected and
intentionally skipped one or more corrupt/torn aligned WAL slots before
resuming WAL appends. `wal_recovery` has no direct collection or
allocator effect; it only makes that recovery boundary explicit and
durable.

9. `RING-WAL-PAYLOAD-009` `free_region`
Adds `region_index` to the durable free-list chain after it has been
removed from the collection named by `collection_id`. The record
mutates global allocator state, but it is collection-scoped because the
region is leaving that collection. If the free list was non-empty, the
previous tail footer must already durably point at `region_index` before
this record is acknowledged. If the free list was empty, this record
makes `region_index` the durable free-list head.

10. `RING-WAL-PAYLOAD-010` `begin_transaction`
Starts a WAL transaction interval for `collection_id`. Until the
matching terminal marker is found or WAL end is reached, replay scans
ordinary records for that collection without applying them on the first
pass.

11. `RING-WAL-PAYLOAD-011` `commit_transaction`
Ends the transaction update phase for `collection_id`. Before this
marker, recovery abandons the collection-state update. After this
marker, recovery preserves the collection-state update and finishes
allocator cleanup.

12. `RING-WAL-PAYLOAD-012` `transaction_finished`
Ends the cleanup phase for `collection_id`. Both the collection-state
update and allocator cleanup are complete, so replay can apply the full
transaction interval in original order.

13. `RING-WAL-PAYLOAD-013` `rollback_transaction`
Records that pre-commit recovery for `collection_id` has completed.
Replay skips transaction-scoped records in the interval and does not
repeat recovery.

Ordering and validity rules:

1. `RING-WAL-VALID-001` A valid `new_collection(collection_id, collection_type)` record is
invalid if `collection_id = 0`, if `collection_type` is missing or
corrupt, or if replay has already seen any prior valid record for that
collection.
2. `RING-WAL-VALID-002` A valid `snapshot(collection_id, collection_type, ...)` record is
itself a durable WAL-snapshot head for that collection.
3. `RING-WAL-VALID-003` A `snapshot(collection_id, collection_type, ...)` record is invalid
if `collection_type` is missing or corrupt.
4. `RING-WAL-VALID-004` A `head(collection_id, collection_type, region_index)` record is the
commit point for a region flush.
5. `RING-WAL-VALID-005` A `head(collection_id, collection_type, region_index)` record is
invalid if `collection_type` is missing or corrupt.
6. `RING-WAL-VALID-006` A `drop_collection(collection_id)` record is invalid if
`collection_id = 0`.
7. `RING-WAL-VALID-007` For non-WAL collections (`collection_id != 0`), append-time
validity requires a successful earlier
`new_collection(collection_id, collection_type)` before any `update`,
`snapshot`, `head(collection_id, collection_type, region_index)`, or
`drop_collection(collection_id)` for that collection may be appended.
Replay of reclaimed WAL may no longer be able to observe that older
`new_collection`, so replay validity is defined separately below in
terms of retained basis records.
8. `RING-WAL-VALID-008` For user collections (`collection_id != 0`), `snapshot` and
`head(collection_id, collection_type, region_index)` are replay-valid
only if their `collection_type` either matches the already tracked
type for that collection or, when no retained type-bearing record for
that collection has been seen yet, establishes the replay-tracked
type from the earliest retained type-bearing basis record.
9. `RING-WAL-VALID-009` A retained `drop_collection(collection_id)` record may be the
earliest retained basis record for a user collection after reclaim. In
that case replay reconstructs only the dropped tombstone for that
`collection_id`; it does not infer a live `collection_type` from the
drop record alone.
10. `RING-WAL-VALID-010` A `head(collection_id, collection_type, region_index)` record for a
user collection is valid only if the target region header has the same
`collection_id`. Replay does not revalidate the append-time check that
an existing-region head target was not free.
11. `RING-WAL-VALID-011` For the WAL (`collection_id = 0`), `head` records are valid only if
their `collection_type` is the WAL collection type. Startup uses them
only during WAL-head discovery from the tail region; they do not create
ordinary collection replay state during the main replay pass.
12. `RING-WAL-VALID-012` A `link` is only valid as the last complete record in a WAL region.
During WAL-chain traversal, a `link` in a reachable non-tail WAL region
is valid only if its target has a valid WAL header with sequence equal
to `expected_sequence` and a valid `WalRegionPrologue`. For the known
tail WAL region only, a durable trailing `link` whose target header is
missing, corrupt, or wrong-sequence, or whose `WalRegionPrologue` is
missing or corrupt, is treated as an incomplete rotation rather than
corruption; startup may finish initializing the target region using
`expected_sequence`.
13. `RING-WAL-VALID-013` A WAL record in the current tail region, other than the specific
`alloc_begin(collection_id = 0, next_region_index,
free_list_head_after)` that starts WAL rotation or the matching
trailing `link`, is invalid if its aligned end offset leaves fewer than
`wal_rotation_reserve` bytes of currently unwritten space remaining in
that WAL region.
14. `RING-WAL-VALID-014` The
`alloc_begin(collection_id = 0, next_region_index,
free_list_head_after)` that starts WAL rotation is invalid unless its
aligned end offset leaves at least `wal_link_reserve` bytes of currently
unwritten space remaining in that WAL region.
15. `RING-WAL-VALID-015` For non-WAL collections (`collection_id != 0`), `update`
records that appear before replay has seen a retained basis decision
for that collection have no replay effect. Implementations MUST NOT
count them as retained post-basis updates.
16. `RING-WAL-VALID-016` For non-WAL collections (`collection_id != 0`), `snapshot`,
`head(collection_id, collection_type, region_index)`, and
`drop_collection(collection_id)` are invalid if replay has already seen
a prior valid `drop_collection(collection_id)` for that collection.
17. `RING-WAL-VALID-017` For non-WAL collections (`collection_id != 0`), a
`new_collection(collection_id, collection_type)` record is also invalid
if replay has already seen a prior valid
`drop_collection(collection_id)` for that collection.
18. `RING-WAL-VALID-018` An
`alloc_begin(collection_id, region_index, free_list_head_after)` record
is invalid if `free_list_head_after` is missing or corrupt, if replay's
current durable `last_free_list_head` is `none`, or if `region_index`
does not equal that durable free-list head.
19. `RING-WAL-VALID-019` A `free_region(collection_id, region_index)` record is invalid if
`region_index` is missing or corrupt, if `collection_id` does not name
the collection whose transaction or operation is freeing the region, or
if the region is already reachable from the durable free-list chain
before this free operation.
20. `RING-WAL-VALID-020` A `head(collection_id, collection_type, region_index)` or
`link(next_region_index, ...)` record that commits a newly allocated
region is append-valid only if it historically followed a matching
earlier `alloc_begin` for the same region index. Replay requires a
prior unmatched WAL-rotation `alloc_begin` only when reconstructing a
still unconsumed `ready_region`; after a durable `link` consumes that
region, the historical `alloc_begin` may be reclaimed.
21. `RING-WAL-VALID-021` Durable allocator-head advance happens at `alloc_begin`; durable
allocator-tail extension happens at `free_region`.
22. `RING-WAL-VALID-022` Replay MAY recover only from checksum-invalid or torn aligned WAL
slots. Replay tracks a pending WAL-recovery boundary from the first
ignored corrupt/torn aligned slot until a later valid `wal_recovery`
record is replayed.
23. `RING-WAL-VALID-023` If replay has a pending WAL-recovery boundary and encounters a
later valid complete record whose `record_type` is not `wal_recovery`,
startup must fail because later WAL data exists after unexplained
corruption.
24. `RING-WAL-VALID-024` If replay reaches the end of a reachable non-tail WAL region with a
pending WAL-recovery boundary that was not closed by `wal_recovery`,
startup must fail because that region contains unresolved mid-log
corruption. A pending WAL-recovery boundary may remain open only at the
end of the current replay tail region.
25. `RING-WAL-VALID-025` Any other invalidity of a complete record is storage corruption and
startup must fail rather than skipping that record. This includes
duplicate `new_collection`, collection-type mismatch, two unmatched
`alloc_begin` reservations, any record after a valid
`drop_collection` for the same collection, broken non-tail WAL chain
links, and committed-region/header mismatch.
26. `RING-WAL-VALID-026` `begin_transaction(collection_id)` is invalid if another transaction is
already open. Nested and concurrent transactions are not supported.
27. `RING-WAL-VALID-027` `commit_transaction(collection_id)`,
`transaction_finished(collection_id)`, and
`rollback_transaction(collection_id)` are valid only when they match
the currently open transaction's collection id.
28. `RING-WAL-VALID-028` A transaction interval may contain ordinary commands for its
`collection_id` and region allocation/free commands carrying that
collection id. Mutating commands for other collections are valid inside
the interval only if they do not depend on transaction-private storage
or allocator effects.
29. `RING-WAL-VALID-029` `transaction_finished(collection_id)` is valid only after a matching
`commit_transaction(collection_id)` in the same transaction interval.
30. `RING-WAL-VALID-030` `rollback_transaction(collection_id)` is valid only when it closes a
transaction interval whose pre-commit recovery was completed by a prior
open.

Checksum trust model:

1. `RING-CHECKSUM-001` During replay, borromean treats a WAL record,
region header, `WalRegionPrologue`, `StorageMetadata`, or free-pointer
footer with a valid checksum and otherwise valid local encoding as
authoritative at the layer described by this spec.
2. `RING-CHECKSUM-002` Those checksums are intended to catch non-intentional corruption
such as torn writes, random bit flips, and flash wear. They are not an
authentication mechanism against an actor who can intentionally rewrite
storage.
3. `RING-CHECKSUM-003` Replay therefore does not attempt to prove every possible global
semantic invariant by cross-checking all checksum-valid records against
all other on-disk state.
4. `RING-CHECKSUM-004` An intentionally corrupted database may survive checksum validation
and fail only when later semantic checks or collection-specific format
validation are applied.
5. `RING-CHECKSUM-005` An implementation MUST ensure that even intentionally corrupted
storage eventually produces a reported error rather than memory
unsafety, undefined behavior, control-flow corruption, infinite loops,
or unbounded resource consumption amounting to denial of service. All
replay walks, decoders, and collection-format handlers MUST remain
bounded by configured storage geometry and record sizes.

Assumptions for replay correctness:

1. `RING-REPLAY-ASSUME-001` A WAL region MUST be erased before reuse.
2. `RING-REPLAY-ASSUME-002` Replay's tail-resynchronization rule depends on this
erase-before-reuse guarantee so stale bytes from prior use cannot be
misinterpreted as new valid records.
3. `RING-REPLAY-ASSUME-003` Replay distinguishes unwritten space from a torn record by checking
the aligned slot's first byte against `erased_byte` and
`wal_record_magic`, and by relying on the escape-stuffed WAL format to
exclude both reserved byte values from record bodies.
An aligned slot whose first byte is `erased_byte` marks end of the
written portion of that WAL region.
4. `RING-REPLAY-ASSUME-004` Any operation that consumes a free-list head MUST first make the
allocator advance durable with
`alloc_begin(collection_id, region_index, free_list_head_after)`.
5. `RING-REPLAY-ASSUME-005` If replay ends with an unmatched
`alloc_begin(collection_id = 0, region_index, ...)`, that region is
treated as a reserved WAL-rotation `ready_region` instead of being
returned to the free list.
6. `RING-REPLAY-ASSUME-006` Transaction recovery must be idempotent:
if storage open crashes before appending `rollback_transaction` or
`transaction_finished`, the next open may repeat the same recovery mode
without leaking or duplicating any region.

## Encoding Helper Requirements

These requirements cover implemented byte-level helpers for canonical disk and WAL encoding.

1. `RING-IMPL-REGRESSION-035` Disk byte helpers MUST advance offsets on reads and writes and return
   BufferTooSmall with needed and available sizes for short buffers.
2. `RING-IMPL-REGRESSION-036` The WAL record area offset MUST be aligned to the configured WAL
   write granule and follow the region header and prologue area.
3. `RING-IMPL-REGRESSION-123` WAL byte helpers MUST advance offsets for byte and byte-slice reads
   and writes and report BufferTooSmall with needed and available sizes on short buffers.
4. `RING-IMPL-REGRESSION-124` Logical WAL byte encoding MUST escape erased byte, record magic, and
   escape byte with distinct derived escape codes.
5. `RING-IMPL-REGRESSION-125` WAL record decoding MUST consume all encoded physical bytes and
   report encoded and logical lengths for decoded records.
6. `RING-IMPL-REGRESSION-126` WAL record decoding MUST wait until all payload-header bytes are
   available before reading payload metadata.
7. `RING-IMPL-REGRESSION-127` WAL record decoding MUST reject an empty logical scratch buffer before
   writing the first decoded logical byte.
8. `RING-IMPL-REGRESSION-128` Logical WAL record encoding MUST serialize fixed-width fields
   little-endian in canonical order.
9. `RING-IMPL-REGRESSION-129` Logical WAL record checksums MUST use CRC-32C over logical prefix
   bytes and store the checksum little-endian.
10. `RING-IMPL-REGRESSION-130` Update WAL records MUST round-trip through physical escaping,
    padding, and decoding without changing payload bytes.
11. `RING-IMPL-REGRESSION-131` Transaction marker WAL records with no payload MUST round-trip
    through physical encoding and decoding.
12. `RING-IMPL-REGRESSION-132` Alloc-begin WAL records MUST round-trip free_list_head_after through
    physical encoding and decoding.
