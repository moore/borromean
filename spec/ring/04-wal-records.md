# Chapter 4: WAL Model And Records

This chapter defines the append-only WAL record stream, its physical
encoding, and the validation rules that make `ApplyWalRecord` safe to
use during foreground operation, WAL-head reclaim, and startup replay.

Mechanism review:

- **Purpose**: make every replay-visible decision append-only and
  ordered while distinguishing valid records, unwritten space, and torn
  or corrupt spans.
- **State**: WAL head/tail, tail append offset, encoded record
  alignment, transaction ranges, inline transaction ranges, and
  free-space collection cursors.
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

All main WAL records are append-only and ordered by physical write
order within the main WAL region chain. Transaction-log records use the
same physical record framing and validation rules. Collection mutation
effects and `free_intent` records are private until a retained
`commit_transaction` record in the main WAL imports a frozen
transaction-log range. Transaction-log `allocate_region` records advance
the free-space allocation cursor when durable, but the popped regions
remain transaction-owned until commit publishes them or rollback cleanup
returns them.

Inline transactions are bounded main-WAL ranges for short
storage-internal atomic groups. Records inside an inline transaction
are ignored until the matching `commit_inline_transaction` is durable.
If a full transaction is active, storage-internal allocation joins that
full transaction instead of opening an inline transaction.

WAL record encoding and alignment:

Let `wal_record_area_offset` be the first offset within a private log
region that is both past the end of the region `Header` plus
`LogRegionPrologue` and aligned to `wal_write_granule`. Replay and
append scanning consider candidate WAL record starts only at aligned
offsets greater than or equal to `wal_record_area_offset`.

Shared durable position types:

```text
LogPosition =
  region_index:u32
  offset:u32

TransactionLogRange =
  start:LogPosition
  end:LogPosition
```

`LogPosition` names a byte offset within a private storage log region.
The offset MUST be aligned to `wal_write_granule` and MUST be greater
than or equal to `wal_record_area_offset` for that region. A
`TransactionLogRange` names a half-open range `[start, end)` inside one
transaction log. A main-WAL transaction-control record always carries
`transaction_log_id` beside the range so concurrent transaction order is
explicit in the main WAL. The range's `start` and `end` positions MUST
name the selected transaction-log chain and `end` MUST be reachable from
`start` by following that transaction log's `link` records.

Let `wal_escape_byte`, `wal_escape_code_erased`,
`wal_escape_code_magic`, and `wal_escape_code_escape` be the first four
byte values in ascending order that are distinct from both
`erased_byte` and `wal_record_magic`. Because only those two byte values
are reserved globally, such a four-byte choice always exists.

1. `RING-WAL-ENC-001` Every physical WAL record MUST begin with a
one-byte `record_magic`.
2. `RING-WAL-ENC-002` `record_magic` MUST equal the storage's
configured `wal_record_magic`, and `wal_record_magic` must not equal
`erased_byte`.
3. `RING-WAL-ENC-003` After the leading `record_magic`, the rest of
the physical WAL record is encoded with deterministic byte-stuffing
over the logical WAL record bytes:
for a logical byte equal to `erased_byte`, emit
`wal_escape_byte wal_escape_code_erased`;
for a logical byte equal to `wal_record_magic`, emit
`wal_escape_byte wal_escape_code_magic`;
for a logical byte equal to `wal_escape_byte`, emit
`wal_escape_byte wal_escape_code_escape`;
all other logical bytes are emitted unchanged.
4. `RING-WAL-ENC-004` During decoding, any `wal_escape_byte` in the
encoded body MUST be followed by exactly one of
`wal_escape_code_erased`, `wal_escape_code_magic`, or
`wal_escape_code_escape`; any other follower byte is corruption.
5. `RING-WAL-ENC-005` Every byte after the leading `record_magic` in a
valid encoded WAL record therefore differs from both `erased_byte` and
`wal_record_magic`.
6. `RING-WAL-ENC-006` After the full logical record through
`record_checksum` has been decoded, any remaining bytes up to the
aligned physical record end are padding. Those padding bytes MUST all
equal `wal_escape_code_escape`.
7. `RING-WAL-ENC-007` Every WAL record start offset within a private
log region MUST be aligned to `wal_write_granule`.
8. `RING-WAL-ENC-008` The encoded size of every WAL record MUST be
rounded up to a multiple of `wal_write_granule`. Replay advances from
one candidate record start to the next in aligned `wal_write_granule`
steps.
9. `RING-WAL-ENC-009` At an aligned candidate record start in a
reachable private log region:
if the first byte is `erased_byte`, that slot is currently unwritten and
marks the end of the written portion of that private log region;
if the first byte is `wal_record_magic`, that slot is a candidate WAL
record and must parse and validate normally;
if the first byte is neither, that slot lies inside a torn/corrupt WAL
record, so replay keeps scanning forward by aligned
`wal_write_granule` steps and ignores the corrupt bytes.
10. `RING-WAL-ENC-010` The recovered append point for the tail region
MUST be the first aligned slot whose first byte is `erased_byte` after
the last valid replayed tail record. If no such slot exists, the tail
region is currently full and the next log append must rotate via
`link` to a new private log region.
11. `RING-WAL-ENC-011` Let `wal_link_reserve` be the aligned encoded
size needed in the current private log tail region to append the
trailing `link(next_region_index, expected_sequence)` record that
completes WAL rotation.
12. `RING-WAL-ENC-012` Let `wal_rotation_reserve` be the total aligned
encoded size needed in the current private log tail region to append
the two WAL records required to start and complete rotation to a new
tail region:
`allocate_region(next_region_index, allocation_head_after)` followed by
`link(next_region_index, expected_sequence)`.
13. `RING-WAL-ENC-013` Appending any WAL record to the current private
log tail region, other than the specific storage-core
`allocate_region(next_region_index, allocation_head_after)` that starts
WAL rotation or the trailing `link`, is invalid if doing so would leave
fewer than `wal_rotation_reserve` unwritten bytes in that private log
region.
14. `RING-WAL-ENC-014` Appending the storage-core
`allocate_region(next_region_index, allocation_head_after)` that starts
WAL rotation is invalid unless its aligned end offset still leaves at
least `wal_link_reserve` and fewer than `wal_rotation_reserve`
unwritten bytes in that private log region. This reserve-window
placement makes an unmatched tail allocation unambiguously recognizable
as the WAL-rotation-start record during startup recovery. Once that
allocation record is durable, the only valid later WAL record in that
private log region is the matching trailing `link`.

Each logical WAL record encodes the following fields:

1. `RING-WAL-FIELD-001` `record_type`: one of `new_collection`,
`update`, `snapshot`, `allocate_region`, `head`, `drop_collection`,
`link`, `erase_free_region_span`, `begin_inline_transaction`,
`commit_inline_transaction`, `wal_recovery`, `free_region`,
`begin_transaction`, `commit_transaction`, `transaction_finished`,
`rollback_transaction`, `add_transaction_collection`,
`rollback_inline_transaction`, or `free_intent`.
2. `RING-WAL-FIELD-002` `collection_id`: required for
`new_collection`, `update`, `snapshot`, `head`, `drop_collection`,
`add_transaction_collection`, and `free_intent`; omitted for allocator
commands, log control records, and transaction control records.
3. `RING-WAL-FIELD-003` `collection_type`: required for
`new_collection`, `snapshot`, and `head`; omitted for all other record
types.
4. `RING-WAL-FIELD-004` `payload_len`: payload size in bytes.
5. `RING-WAL-FIELD-005` `payload`: bytes defined by `record_type`.
6. `RING-WAL-FIELD-006` `record_checksum`: checksum covering the full
logical record before byte-stuffing encoding.
7. `RING-WAL-FIELD-007` `padding`: zero or more trailing
`wal_escape_code_escape` bytes so the physical encoded record size is a
multiple of `wal_write_granule`.

Logical WAL record byte layout before byte-stuffing:

```text
LogicalWalRecord =
  record_type:u8
  [collection_id:u64 if required by record_type]
  [collection_type:u16 if required by record_type]
  payload_len:u32
  payload:[u8; payload_len]
  record_checksum:u32
```

1. `RING-WAL-LAYOUT-001` `record_type` MUST use these canonical byte
codes:
`new_collection = 0x01`,
`update = 0x02`,
`snapshot = 0x03`,
`allocate_region = 0x04`,
`head = 0x05`,
`drop_collection = 0x06`,
`link = 0x07`,
`erase_free_region_span = 0x08`,
`begin_inline_transaction = 0x09`,
`commit_inline_transaction = 0x0a`,
`wal_recovery = 0x0b`,
`free_region = 0x0c`,
`begin_transaction = 0x0d`,
`commit_transaction = 0x0e`,
`transaction_finished = 0x0f`,
`rollback_transaction = 0x10`,
`add_transaction_collection = 0x11`,
`rollback_inline_transaction = 0x12`,
`free_intent = 0x13`.
2. `RING-WAL-LAYOUT-002` The logical field order before byte-stuffing
MUST be exactly the order shown above.
3. `RING-WAL-LAYOUT-003` `payload_len` MUST equal the number of
logical payload bytes only. It MUST exclude `record_checksum`, the
physical leading `record_magic`, and physical padding.
4. `RING-WAL-LAYOUT-004` `record_checksum` MUST be CRC-32C over the
logical WAL record bytes from `record_type` through the final byte of
the payload.
5. `RING-WAL-LAYOUT-005` Record types whose payload is empty
(`new_collection`, `drop_collection`, and `wal_recovery`) MUST still
encode `payload_len = 0`.
6. `RING-WAL-LAYOUT-006` Payload bytes are encoded canonically by
record type:
`update` and `snapshot` payloads are opaque collection-defined bytes;
`head` payload is `region_index:u32`;
`allocate_region` payload is
`region_index:u32, allocation_head_after:FreeQueuePosition`;
`free_region` payload is
`region_index:u32, append_tail_after:FreeQueuePosition`;
`erase_free_region_span` payload is
`count:u32, ready_boundary_after:FreeQueuePosition`;
`link` payload is `next_region_index:u32` followed by
`expected_sequence:u64`;
`begin_inline_transaction` payload is
`record_count:u32, encoded_len:u32`;
`commit_inline_transaction` and `rollback_inline_transaction` payloads
are `record_count:u32`;
`begin_transaction` payload is
`transaction_log_id:u32, start:LogPosition`;
`commit_transaction`, `transaction_finished`, and
`rollback_transaction` payloads are
`transaction_log_id:u32, range:TransactionLogRange`;
`add_transaction_collection` payload is
`observed_collection_generation:u64`;
`free_intent` payload is `region_index:u32`;
`new_collection`, `drop_collection`, and `wal_recovery` payloads are
empty.

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

4. `RING-WAL-PAYLOAD-004` `allocate_region`
When applied, pops the current ready entry from the free-space
collection. The payload stores the physical `region_index` that must be
at the current `allocation_head` and the self-checking
`allocation_head_after` cursor that must name the next queue position.
The record carries no owner or purpose. Ownership comes from the
enclosing full transaction, enclosing inline transaction, or privileged
storage-core operation.

5. `RING-WAL-PAYLOAD-005` `head`
Commits a collection to a durable region head. Payload contains the
target `region_index`. The record also carries the collection type for
that durable region basis. When `collection_id = 0`, this record
commits a new WAL head region; there is no distinct WAL-head record
type. For user collections, Borromean core validates that the target
region header has the same `collection_id`; the collection
implementation owns the interpretation of its `collection_format`.

6. `RING-WAL-PAYLOAD-006` `drop_collection`
Durably tombstones a user collection. The record detaches that
collection's current durable basis from the live namespace, discards
pending WAL updates for that collection, and forbids later WAL records
for the same `collection_id`. Previously live committed regions become
reclaimable once collection-specific cleanup removes any remaining
physical references to them and appends `free_region` records.

7. `RING-WAL-PAYLOAD-007` `link`
Points from a full private log region to the next private log region.
Payload contains `next_region_index` and `expected_sequence` for the
next private log region header.

8. `RING-WAL-PAYLOAD-008` `erase_free_region_span`
Publishes erase maintenance for the next `count` dirty entries starting
at the current `ready_boundary`. The physical erases happen before this
record is durable; the durable effect is to advance `ready_boundary` to
`ready_boundary_after`. If power fails after erase but before this
record is durable, replay treats those entries as still dirty and may
erase them again.

9. `RING-WAL-PAYLOAD-009` `begin_inline_transaction`
Main-WAL-only record. Opens a bounded inline transaction for a short
storage-internal atomic group. Payload stores the number of records in
the bounded body and their encoded physical length. Storage MUST reserve
enough main-WAL tail space for the entire inline range, including its
terminal record, before appending this record.

10. `RING-WAL-PAYLOAD-010` `commit_inline_transaction`
Main-WAL-only record. Atomically applies the records in the matching
bounded inline range at this commit position. Before this marker,
replay scans the bounded range for validation and cleanup information
but does not apply collection or allocator effects.

11. `RING-WAL-PAYLOAD-011` `wal_recovery`
Marks that replay or a prior open detected and intentionally skipped
one or more corrupt/torn aligned WAL slots before resuming WAL appends.
`wal_recovery` has no direct collection or allocator effect; it only
makes that recovery boundary explicit and durable.

12. `RING-WAL-PAYLOAD-012` `free_region`
Appends a detached physical region as a dirty entry in the free-space
collection. The payload stores the detached `region_index` and the
self-checking `append_tail_after` cursor that must be the next queue
position after the current `append_tail`. The record carries no owner
or purpose; cleanup correctness comes from the enclosing transaction or
privileged storage-core recovery procedure.

13. `RING-WAL-PAYLOAD-013` `begin_transaction`
Main-WAL-only record. Opens a transaction descriptor and assigns it a
transaction log. Payload is `transaction_log_id:u32,
start:LogPosition`, where `start` is the first transaction-log position
owned by this transaction.

14. `RING-WAL-PAYLOAD-014` `add_transaction_collection`
Transaction-log-only record. Enrolls `collection_id` in the open
transaction for that transaction log. The payload stores the
collection's `observed_collection_generation:u64`, which is the
committed state generation observed when the transaction copied the
collection frontier into its private transaction buffer.

15. `RING-WAL-PAYLOAD-015` `commit_transaction`
Main-WAL-only record. The payload is
`transaction_log_id:u32, range:TransactionLogRange`. It freezes the
named range in that transaction log and imports that range into
main-WAL replay at the position of this commit record. Before this
marker, recovery keeps transaction-private collection changes and free
intents non-visible and rolls back transaction-owned allocations through
the rollback protocol. After this marker, recovery preserves imported
collection state, publishes transaction-owned allocations, detaches
transaction free intents from collection live state, and finishes
ordered free-intent cleanup before the transaction is finished.

16. `RING-WAL-PAYLOAD-016` `transaction_finished`
Main-WAL-only record. The payload is
`transaction_log_id:u32, range:TransactionLogRange`. It records that the
transaction's committed or rolled-back cleanup and recovery obligations
are complete, so transaction-log garbage collection may release this
reference when no retained record or active descriptor points to the
same range.

17. `RING-WAL-PAYLOAD-017` `rollback_transaction`
Main-WAL-only record. The payload is
`transaction_log_id:u32, range:TransactionLogRange`. It records that the
transaction did not become visible and that ordered rollback cleanup is
authorized for every transaction-owned allocation in the transaction-log
range.

18. `RING-WAL-PAYLOAD-018` `rollback_inline_transaction`
Main-WAL-only record. Records that recovery cleaned an uncommitted
bounded inline range and that the range remains non-visible.

19. `RING-WAL-PAYLOAD-019` `free_intent`
Transaction-log-only record. Payload is `region_index:u32`, and the
record carries the enrolled `collection_id`. It records a
transaction-private intent to free a physical region that remains live
in the collection until the transaction commits. Before commit,
`free_intent` has no allocator effect, does not append to the
free-space collection, and does not advance `append_tail`.

## Ordering And Validity

1. `RING-WAL-VALID-001` A valid
`new_collection(collection_id, collection_type)` record is invalid if
`collection_id = 0`, if `collection_type` is missing or corrupt, or if
replay has already seen any prior valid record for that collection.
2. `RING-WAL-VALID-002` A `snapshot(collection_id,
collection_type, ...)` record is invalid if `collection_type` is
missing or corrupt.
3. `RING-WAL-VALID-003` A `head(collection_id, collection_type,
region_index)` record is invalid if `collection_type` is missing or
corrupt.
4. `RING-WAL-VALID-004` A `drop_collection(collection_id)` record is
invalid if `collection_id = 0`.
5. `RING-WAL-VALID-005` For user collections (`collection_id != 0`),
append-time validity requires a successful earlier
`new_collection(collection_id, collection_type)` before any `update`,
`snapshot`, `head`, or `drop_collection` for that collection may be
appended. Replay of reclaimed WAL may no longer observe that older
`new_collection`, so replay validity is defined in terms of retained
basis records.
6. `RING-WAL-VALID-006` For user collections, `snapshot` and `head`
records are replay-valid only if their `collection_type` either matches
the already tracked type for that collection or, when no retained
type-bearing record has been seen yet, establishes the replay-tracked
type from the earliest retained type-bearing basis record.
7. `RING-WAL-VALID-007` A retained `drop_collection(collection_id)`
record may be the earliest retained basis record for a user collection
after reclaim. In that case replay reconstructs only the dropped
tombstone; it does not infer a live `collection_type` from the drop
record alone.
8. `RING-WAL-VALID-008` A `head` record for a user collection is valid
only if the target region header has the same `collection_id`. Replay
does not revalidate every append-time reachability check for that
target; this is an intentional application of the checksum trust model.
9. `RING-WAL-VALID-009` For the WAL (`collection_id = 0`), `head`
records are valid only if their `collection_type` is the WAL collection
type. Startup uses them only during WAL-head discovery from the tail
region; they do not create ordinary collection replay state during the
main replay pass.
10. `RING-WAL-VALID-010` A `link` is only valid as the last complete
record in a private log region. During log-chain traversal, a `link` in
a reachable non-tail log region is valid only if its target has a valid
private log header with sequence equal to `expected_sequence` and a
valid `LogRegionPrologue`. For the known tail log region only, a
durable trailing `link` whose target header is missing, corrupt, or
wrong-sequence, or whose `LogRegionPrologue` is missing or corrupt, is
treated as an incomplete rotation rather than corruption; startup may
finish initializing the target region using `expected_sequence`.
11. `RING-WAL-VALID-011` A WAL record in the current private log tail
region, other than the specific storage-core `allocate_region` that
starts WAL rotation or the matching trailing `link`, is invalid if its
aligned end offset leaves fewer than `wal_rotation_reserve` bytes of
currently unwritten space remaining in that private log region.
12. `RING-WAL-VALID-012` The storage-core `allocate_region` that
starts WAL rotation is invalid unless its aligned end offset leaves at
least `wal_link_reserve` bytes of currently unwritten space remaining in
that private log region.
13. `RING-WAL-VALID-013` For user collections, `update` records that
appear before replay has seen a retained basis decision for that
collection have no replay effect. Implementations MUST NOT count them
as retained post-basis updates.
14. `RING-WAL-VALID-014` For user collections, `snapshot`, `head`, and
`drop_collection` are invalid if replay has already seen a prior valid
`drop_collection` for that collection.
15. `RING-WAL-VALID-015` For user collections, a `new_collection`
record is also invalid if replay has already seen a prior valid
`drop_collection` for that collection.
16. `RING-WAL-VALID-016` `free_region(region_index,
append_tail_after)` is invalid unless `region_index` names a valid
detached region, `append_tail_after` is the queue position immediately
after the current `append_tail`, and the region is not already present
in the free-space collection.
17. `RING-WAL-VALID-017` `erase_free_region_span(count,
ready_boundary_after)` is invalid if `count = 0`, if the dirty range has
fewer than `count` entries, or if `ready_boundary_after` is not the
position reached by advancing from the current `ready_boundary` by
`count` entries.
18. `RING-WAL-VALID-018` `allocate_region(region_index,
allocation_head_after)` is invalid if the ready range is empty, if the
current `allocation_head` entry does not name `region_index`, or if
`allocation_head_after` is not the next queue position after the current
`allocation_head`.
19. `RING-WAL-VALID-019` The cursor invariant
`allocation_head <= ready_boundary <= append_tail` MUST hold before and
after every allocator command. Replay MUST reject any command that
would violate the invariant.
20. `RING-WAL-VALID-020` Allocator commands carry no owner or purpose.
An `allocate_region` command for ordinary user/data allocation is valid
only inside an active full transaction or a bounded inline transaction.
A `free_region` command is valid only when the named region is already
detached from live reachability by a committed transaction, rollback
recovery, or privileged storage-core cleanup. An
`erase_free_region_span` command is valid only as erase maintenance for
the dirty range. A privileged storage-core `allocate_region` command is
valid only for private log rotation, transaction-log growth, allocator
metadata growth, recovery, or erase maintenance, and it MUST preserve
the ready-region reserve.
21. `RING-WAL-VALID-021` `begin_transaction`,
`commit_transaction`, `transaction_finished`, and
`rollback_transaction` records are valid only in the main WAL, and
their `transaction_log_id` MUST be less than the configured
`transaction_log_count`.
22. `RING-WAL-VALID-022` `add_transaction_collection` is valid only in
a transaction log and only while that transaction log has an open
transaction descriptor for the containing range.
23. `RING-WAL-VALID-023` A transaction log may contain records for any
collection explicitly enrolled by `add_transaction_collection` in the
same open transaction range. Collection mutation records for an
unenrolled collection are invalid in that range.
24. `RING-WAL-VALID-024`
`commit_transaction(transaction_log_id, range)` is valid only if the
range starts at the matching open transaction descriptor's start, ends
at that transaction log's current append position, contains only
complete valid records, and contains no torn record before `range.end`.
25. `RING-WAL-VALID-025`
`transaction_finished(transaction_log_id, range)` is valid only after a
retained matching `commit_transaction(transaction_log_id, range)` or
`rollback_transaction(transaction_log_id, range)` and after the ordered
cleanup cursor reaches the end of the committed free-intent list or
rolled-back transaction-owned allocation list.
26. `RING-WAL-VALID-026`
`rollback_transaction(transaction_log_id, range)` is valid only when
the range starts at the matching open transaction descriptor's start,
ends at that transaction log's current append position, contains only
complete valid records, and contains no torn record before `range.end`.
The transaction-owned allocations in the range become the ordered
rollback cleanup list. A rolled-back range MUST NOT be imported as
visible collection or allocator state.
27. `RING-WAL-VALID-027` Before appending
`commit_transaction(transaction_log_id, range)`, storage MUST verify
that each enrolled collection's current committed state generation
still equals the generation recorded by that collection's
`add_transaction_collection` record. Any mismatch fails the commit with
a transaction conflict.
28. `RING-WAL-VALID-028` Transaction-log records after a frozen
`range.end` are outside that transaction range. Torn records after
`range.end` do not invalidate the committed range; torn or malformed
records inside a committed range are corruption.
29. `RING-WAL-VALID-029` `begin_inline_transaction(record_count,
encoded_len)` is valid only in the main WAL when no full transaction or
inline transaction is active. Storage MUST reserve enough main-WAL tail
space for the whole bounded range before appending the begin record.
30. `RING-WAL-VALID-030` An inline transaction body MUST contain exactly
`record_count` complete records and use exactly `encoded_len` physical
bytes before its terminal inline transaction record.
31. `RING-WAL-VALID-031` Inline transactions MUST NOT nest. If a full
transaction is active, storage-internal allocation and other atomic
multi-record work MUST join the full transaction instead of opening an
inline transaction.
32. `RING-WAL-VALID-032` `commit_inline_transaction(record_count)` is
valid only when it closes the current inline transaction and its
`record_count` matches the opener. Replay applies the inline body
atomically at the commit record.
33. `RING-WAL-VALID-033` `rollback_inline_transaction(record_count)` is
valid only as the durable terminal marker for an uncommitted inline
range that recovery has cleaned. Replay MUST NOT apply the inline body.
34. `RING-WAL-VALID-034` Replay MAY recover only from
checksum-invalid or torn aligned WAL slots. Replay tracks a pending
WAL-recovery boundary from the first ignored corrupt/torn aligned slot
until a later valid `wal_recovery` record is replayed.
35. `RING-WAL-VALID-035` If replay has a pending WAL-recovery boundary
and encounters a later valid complete record whose `record_type` is not
`wal_recovery`, startup must fail because later WAL data exists after
unexplained corruption.
36. `RING-WAL-VALID-036` If replay reaches the end of a reachable
non-tail private log region with a pending WAL-recovery boundary that
was not closed by `wal_recovery`, startup must fail because that region
contains unresolved mid-log corruption. A pending WAL-recovery boundary
may remain open only at the end of the current replay tail region.
37. `RING-WAL-VALID-037` Any other invalidity of a complete record is
storage corruption and startup must fail rather than skipping that
record.
38. `RING-WAL-VALID-038` `free_intent(collection_id, region_index)` is
valid only in a transaction log, only for a collection enrolled in the
containing open transaction range, only for a physical region that is
live in that collection when the transaction generation is current, and
only if the same transaction has not already staged the same
`region_index` as a free intent. Ordinary pre-commit frees MUST use
`free_intent`; a transaction-log `free_region` record is invalid for
this purpose.
39. `RING-WAL-VALID-040` A transaction cleanup `free_region` record is
valid only in the main WAL while the matching committed or rolled-back
transaction owns cleanup. Its `append_tail_after` MUST name the queue
position immediately after the transaction's next cleanup slot:
`cleanup_start_tail + cleanup_index + 1`. No other main-WAL operation
may interleave before `transaction_finished`.

## Checksum Trust Model

1. `RING-CHECKSUM-001` During replay, Borromean treats a WAL record,
region header, `LogRegionPrologue`, `FreeSpaceRegionPrologue`,
`FreeSpaceEntry`, or `StorageMetadata` with a valid checksum and
otherwise valid local encoding as authoritative at the layer described
by this spec.
2. `RING-CHECKSUM-002` Those checksums are intended to catch
non-intentional corruption such as torn writes, random bit flips, and
flash wear. They are not an authentication mechanism against an actor
who can intentionally rewrite storage.
3. `RING-CHECKSUM-003` Replay therefore does not attempt to prove every
possible global semantic invariant by cross-checking all
checksum-valid records against all other on-disk state.
4. `RING-CHECKSUM-004` An intentionally corrupted database may survive
checksum validation and fail only when later semantic checks or
collection-specific format validation are applied.
5. `RING-CHECKSUM-005` An implementation MUST ensure that even
intentionally corrupted storage eventually produces a reported error
rather than memory unsafety, undefined behavior, control-flow
corruption, infinite loops, or unbounded resource consumption. All
replay walks, decoders, and collection-format handlers MUST remain
bounded by configured storage geometry and record sizes.

## Assumptions For Replay Correctness

1. `RING-REPLAY-ASSUME-001` A private log region MUST be erased before
reuse.
2. `RING-REPLAY-ASSUME-002` Replay's tail-resynchronization rule
depends on this erase-before-reuse guarantee so stale bytes from prior
use cannot be misinterpreted as new valid records.
3. `RING-REPLAY-ASSUME-003` Replay distinguishes unwritten space from a
torn record by checking the aligned slot's first byte against
`erased_byte` and `wal_record_magic`, and by relying on the
escape-stuffed WAL format to exclude both reserved byte values from
record bodies. An aligned slot whose first byte is `erased_byte` marks
end of the written portion of that private log region.
4. `RING-REPLAY-ASSUME-004` Any operation that consumes a ready
free-space entry MUST first make
`allocate_region(region_index, allocation_head_after)` durable in the
main WAL, in an inline transaction, or in a reachable
transaction-log range. The log segment prologue supplies the
free-space cursor checkpoint before later complete allocator commands
are applied.
5. `RING-REPLAY-ASSUME-005` If replay ends with an unmatched
storage-core `allocate_region` in the WAL-rotation reserve window, that
region is treated as a private log rotation target instead of being
returned to the free-space collection.
6. `RING-REPLAY-ASSUME-006` Transaction and inline transaction recovery
must be idempotent: if storage open crashes before appending the
matching rollback or finish marker, the next open may repeat the same
recovery mode without leaking or duplicating any region.

## Encoding Helper Requirements

These requirements cover implemented byte-level helpers for canonical
disk and WAL encoding.

1. `RING-IMPL-REGRESSION-035` Disk byte helpers MUST advance offsets on
   reads and writes and return `BufferTooSmall` with needed and
   available sizes for short buffers.
2. `RING-IMPL-REGRESSION-036` The WAL record area offset MUST be
   aligned to the configured WAL write granule and follow the region
   header and prologue area.
3. `RING-IMPL-REGRESSION-123` WAL byte helpers MUST advance offsets for
   byte and byte-slice reads and writes and report `BufferTooSmall`
   with needed and available sizes on short buffers.
4. `RING-IMPL-REGRESSION-124` Logical WAL byte encoding MUST escape
   erased byte, record magic, and escape byte with distinct derived
   escape codes.
5. `RING-IMPL-REGRESSION-125` WAL record decoding MUST consume all
   encoded physical bytes and report encoded and logical lengths for
   decoded records.
6. `RING-IMPL-REGRESSION-126` WAL record decoding MUST wait until all
   payload-header bytes are available before reading payload metadata.
7. `RING-IMPL-REGRESSION-127` WAL record decoding MUST reject an empty
   logical scratch buffer before writing the first decoded logical byte.
8. `RING-IMPL-REGRESSION-128` Logical WAL record encoding MUST
   serialize fixed-width fields little-endian in canonical order.
9. `RING-IMPL-REGRESSION-129` Logical WAL record checksums MUST use
   CRC-32C over logical prefix bytes and store the checksum
   little-endian.
10. `RING-IMPL-REGRESSION-130` Update WAL records MUST round-trip
    through physical escaping, padding, and decoding without changing
    payload bytes.
11. `RING-IMPL-REGRESSION-131` Transaction-control WAL records MUST
    round-trip their transaction-log positions and ranges through
    physical encoding and decoding.
12. `RING-IMPL-REGRESSION-132` Allocator WAL records MUST round-trip
    `region_index` plus `FreeQueuePosition` payload fields through
    physical encoding and decoding.
