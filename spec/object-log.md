# Object Log Collection

## Purpose

`ObjectLog` is a durable opaque object collection intended to support external
storage systems built on top of it. It is meant for callers that need stable
object addresses, durable append semantics, prefix truncation, and efficient
packing on flash-like media.

The hard part is that those goals pull in different directions. Returning a
handle before an object is flushed is useful to callers, but a handle that
changes after flush is not durable. Writing every object directly to its final
region would make handles easy, but it would also force each object append to
respect region/page programming boundaries and waste space for small objects.
Reusing the same log region in place would reduce allocation churn, but it
would work against wear leveling and make stale handles harder to detect.

The design resolves those tensions by separating public object records from
physical materialization. An append first reserves object-log record addresses
inside a frontier. Small objects can be represented by one inline object record.
Large objects can be represented by a public large record entry, optional full
auxiliary regions, and private tail chunks in ordinary object-log order. The
bytes needed to recover unflushed frontier state are persisted through WAL
updates. Later, flush materializes the frontier into the same reserved region,
so handles remain valid before and after flush.

## API And Handles

Object-log handles are deliberately opaque to callers. The collection stores
region, object-log region serial, and offset internally because those facts are
needed to find bytes and reject stale handles, but callers should not inspect
or manufacture those parts. A handle is a capability returned by append and
accepted by later object-log operations. That keeps the public API from
promising a particular bit layout and prevents callers from constructing
plausible-looking handles that never came from the collection.

Reads may fetch either a whole object or an object-relative byte range. Public
object-relative offsets and lengths are `u64` values. Whole object reads require
caller scratch large enough for the stored object. If the scratch buffer is too
small, the read fails with an error that reports the stored object length so the
caller can retry with a larger buffer or switch to a range read. A length-only
query returns the stored `u64` object length without returning object bytes. A
range read uses an opaque live handle plus `offset` and `len` values inside that
object's payload bytes. It returns only the requested byte range, so callers
need scratch capacity for the requested range rather than for the full object.

This collection also has its own durable `collection_type`. That lets generic
Borromean open and replay paths recognize object-log collections without
hard-coding application-specific behavior into core storage. The
collection-specific module owns the object payload formats and validation rules;
Borromean core only needs to know that the collection type is supported and
which empty snapshot payload represents an empty object log during WAL reclaim.

1. `RING-OBJECT-001` Appending an object MUST return an opaque
`ObjectLogHandle` that names a committed object record, and reopening the
collection MUST reconstruct unflushed frontier objects from retained WAL
updates.
2. `RING-OBJECT-002` `ObjectLogHandle` MUST remain opaque to external
callers: it MUST NOT expose public field access, an unchecked public field
constructor, or debug formatting that reveals internal handle components.
3. `RING-OBJECT-003` Opening an object-log collection by id MUST fail
if the live collection exists with a non-object-log collection type.
4. `RING-OBJECT-004` The durable object-log handle and `ObjectLogPointer`
encoding MUST be exactly 16 bytes with no padding: bytes 0 through 3 contain
`region_index` as a little-endian `u32`, bytes 4 through 11 contain `sequence`
as a little-endian `u64`, and bytes 12 through 15 contain `offset` as a
little-endian `u32`.
5. `RING-OBJECT-005` Object-log reads MUST reject handles that do not
name a live reserved object record.
6. `RING-OBJECT-015` Object-log range reads MUST accept `u64`
object-relative offset and length values, return only that committed byte range,
reject ranges outside the object, and require only enough caller scratch for the
requested range.
7. `RING-OBJECT-016` Object-log whole-object reads MUST fail with a
buffer-too-small error that reports the stored object length when caller
scratch cannot hold the full object, and object-log length queries MUST return
the stored `u64` object length without returning object bytes.
8. `RING-OBJECT-024` Object-log reads MUST treat caller scratch length as a
minimum capacity requirement: buffers at least as long as the returned whole
object or requested range MUST succeed, including exact-size buffers.

## Durability

Appends are WAL-backed because the caller needs the handle immediately, but
the object bytes may not yet be in their final data region. When an append
needs a new target region, allocation and the append update are recorded in a
collection transaction: the allocation reserves the final region and the update
records the handle metadata and object bytes needed to reconstruct the
frontier after reopen. If power is lost before the frontier is flushed, replay
rebuilds the same frontier from retained WAL updates.

Flush is the point where the reserved physical region is written. The data
region begins with an object-log prologue containing the sequence assigned to
that logical frontier region and the log metadata for the collection, followed
by typed object records. A flushed read checks the region header and the full
object-log prologue before returning bytes, so a stale handle cannot silently
read from an unrelated later use of the same physical region or from a region
formatted for a different object-log record format. After flush, the collection
persists metadata that describes the flushed regions and any still-live frontier
state.

Log metadata is a non-empty immutable opaque byte sequence supplied when the
object log is created. The object log stores and validates it but never
interprets it. It is part of the durable identity and format of the log, not an
object, handle, checkpoint, head, tail, or traversal position. Opening a log
restores and exposes the stored metadata so callers can decide how to interpret
object bytes without knowing the metadata before open. Appends, reads,
traversal, truncation, and append transactions do not modify it.

The object-log data-region prologue is encoded before object records. Integer
fields are little-endian, and object records begin immediately after the
metadata bytes. Its fields are:

- `magic: [u8; 4]`: the literal bytes `OLOG`, used to identify the region as
  an object-log data region before interpreting the rest of the prologue.
- `version: u16`: the object-log data prologue format version, currently `1`,
  used to reject data regions whose prologue or record layout is not understood.
- `sequence: u64`: the object-log region serial named by handles for records in
  this region, used with the physical region index to reject stale handles
  after storage reuses a region for a later object-log frontier.
- `log_metadata_len: u32`: the byte length of the immutable log metadata copy
  that follows, used to validate bounds and locate the first object record.
- `log_metadata: [u8; log_metadata_len]`: a verbatim copy of the collection's
  immutable log metadata, used to reject flushed regions whose durable format
  identity differs from the collection being opened or read.

`ObjectLogPointer` is the persisted pointer type used inside object records. It
is encoded exactly like `ObjectLogHandle`: `[region_index:u32
little-endian][sequence:u64 little-endian][offset:u32 little-endian]`.
`region_index` names the physical region, `sequence` is the object-log region
serial that prevents stale-region aliasing, and `offset` is the region-local
record offset. Region-local persisted offsets are `u32`; object-relative offsets
and object lengths exposed by the read API are `u64`.

`AuxRegionPointer` is the persisted pointer type used to name a private
large-object auxiliary region. It is exactly 4 bytes:
`[region_index:u32 little-endian]`. The pointed-to region MUST validate as an
object-log auxiliary region for the same collection before any auxiliary chunk
bytes are read.

Object-log V1 data regions contain typed object records. Each record has the
common header `[record_type:u8][body_len:u32 little-endian][body_crc32c:u32
little-endian][body]`. `body_len` is the exact byte length of `body`, and
`body_crc32c` is CRC32C over `body` using the same CRC-32C/ISCSI polynomial used
elsewhere in Borromean disk structures.

Record type `0x01` is `InlineObject`. Its body is the raw object bytes, and the
public object handle points directly at the `InlineObject` record.

Record type `0x02` is `ObjectChunk`. In ordinary object-log data regions,
`ObjectChunk` is a private tail-chunk record used only after a large record
entry. Its body is `[logical_start:u64 little-endian][chunk_len:u32
little-endian][chunk_crc32c:u32 little-endian][chunk_bytes]`. `logical_start`
is the object-relative offset of the first byte in `chunk_bytes`. `chunk_len`
is the logical byte length of `chunk_bytes`. `chunk_crc32c` is CRC32C over
exactly `chunk_bytes`. The common record-header `body_len` MUST equal
`16 + chunk_len`.

Record type `0x03` is `LargeRecordEntry`. Its body is
`[total_object_len:u64 little-endian][tail_logical_len:u32
little-endian][first_aux:AuxRegionPointer]`. Large-object public handles point
at `LargeRecordEntry` records. `total_object_len` is the full object length.
`tail_logical_len` is the number of logical object bytes stored in the private
tail chunks that immediately follow this record through the ordinary object-log
path. The auxiliary logical length is
`total_object_len - tail_logical_len`. If `total_object_len ==
tail_logical_len`, there is no auxiliary chain and the `first_aux` pointer bytes
MUST be zero. If `total_object_len > tail_logical_len`, `first_aux` names the
first auxiliary region; region index zero is valid in that case if it names an
allocated auxiliary region. `tail_logical_len` MUST NOT exceed
`total_object_len` and MUST be less than one complete auxiliary-region image's
logical chunk capacity.

Large-object auxiliary regions use the ordinary Borromean region header, then an
object-log auxiliary prologue with its own version. The auxiliary prologue is:

- `aux_magic: [u8; 4]`: the literal bytes `OLAX`.
- `aux_version: u16`: the auxiliary-region format version, currently `1`.
- `chunk_slot_len: u32`: the exact physical byte length of each chunk slot.
- `chunk_slot_count: u32`: the number of chunk slots in the auxiliary chunk
  area.
- `log_metadata_len: u32`: the byte length of the immutable log metadata copy
  that follows.
- `log_metadata: [u8; log_metadata_len]`: a verbatim copy of the collection's
  immutable log metadata.
- `prologue_crc32c: u32`: CRC32C over all preceding auxiliary prologue fields,
  including `log_metadata`.
- zero padding to the next `wal_write_granule` boundary.

After the aligned auxiliary prologue, the auxiliary chunk area contains
`chunk_slot_count` fixed-size chunk slots. Each chunk slot has exact physical
length `chunk_slot_len`; `chunk_slot_len` MUST divide the auxiliary chunk area
exactly and MUST be a multiple of `wal_write_granule`. Each auxiliary chunk slot
is `[record_type:u8 = 0x02][logical_start:u64 little-endian][chunk_len:u32
little-endian][chunk_crc32c:u32 little-endian][chunk_bytes][zero_fill]`.
`chunk_len` is the logical byte length only. `chunk_crc32c` covers only the
logical `chunk_bytes`. `zero_fill` extends the encoded slot to `chunk_slot_len`.

Each auxiliary region ends with a reserved next-link slot. An erased or
all-empty next-link slot means the auxiliary region has no successor. A present
next-link slot is `[present:u8 = 1][next:AuxRegionPointer][link_crc32c:u32
little-endian]`, padded to `wal_write_granule`. `link_crc32c` covers `present`
and `next`. Other `present` values are invalid.

Durable object-log state has to be canonical and self-delimiting. The reader is
often deciding whether bytes came from the intended object-log collection, from
a stale reused region, from a partially replayed transaction, or from
corruption after the last valid byte. The format therefore specifies exact
boundary behavior instead of leaving padding, trailing bytes, unknown tags, or
out-of-range record bodies to implementation convention.

Replay is collection-scoped because the shared WAL interleaves lifecycle,
transaction, and update records for every collection. Object-log replay must
reconstruct the target collection without letting another collection's markers
publish, roll back, create, or drop target object-log state.

Append placement is also a durability concern. Handles are reserved before
their bytes are written to a data region, so an off-by-one capacity decision can
change a handle, waste a region, or leave a large-object append unable to make
progress. Exact-fit and no-progress cases are specified to keep small records,
large record entries, private chunks, flush, and replay using the same address
boundaries.

1. `RING-OBJECT-006` Flushing an object-log frontier MUST write the
frontier bytes into the previously reserved physical data region, persist
metadata sufficient to read flushed handles after reopen, and assign a
new sequence to a later reserved frontier region.
2. `RING-OBJECT-007` Object-log metadata MUST be a non-empty immutable
opaque byte sequence supplied at collection creation, persisted with
collection state, restored on open, and exposed to callers without requiring
the caller to know it before opening the collection.
3. `RING-OBJECT-008` Every object-log data region MUST contain the full
immutable log metadata in its object-log prologue, and opening or reading a
flushed region MUST reject a prologue whose metadata differs from the
collection metadata.
4. `RING-OBJECT-014` Object-log region sequences MUST be monotonic `u64`
values that never wrap. If replay, snapshot decode, or open observes state
that would require advancing past `u64::MAX`, the collection MUST be treated
as corrupt.
5. `RING-OBJECT-017` Object-log V1 data regions MUST encode object records
with the common typed-record header
`[record_type:u8][body_len:u32 little-endian][body_crc32c:u32
little-endian][body]`, MUST compute `body_crc32c` as CRC32C over `body`, and
MUST reject unknown record types.
6. `RING-OBJECT-018` Small objects with length less than or equal to one
chunk's logical capacity MUST be encoded as public record type `0x01`
`InlineObject` whose body is the raw object bytes and whose public handle names
that record.
7. `RING-OBJECT-019` Large-object handles MUST point to public record type
`0x03` `LargeRecordEntry` records encoded as `[total_object_len:u64
little-endian][tail_logical_len:u32 little-endian][first_aux:AuxRegionPointer]`,
where `tail_logical_len` names the contiguous ordinary-log tail byte count;
when `tail_logical_len == total_object_len`, `first_aux` bytes are zero, and
otherwise `first_aux` names the first allocated auxiliary region.
8. `RING-OBJECT-020` Object chunks MUST be private record type `0x02`
`ObjectChunk` records encoded as `[logical_start:u64 little-endian]
[chunk_len:u32 little-endian][chunk_crc32c:u32 little-endian][chunk_bytes]`
in ordinary object-log regions and fixed auxiliary chunk slots, and MUST
validate `chunk_crc32c` over exactly the logical `chunk_bytes`.
9. `RING-OBJECT-025` Object-log durable state MUST be canonical and
self-delimiting: persisted handles, data-region prologues, object records,
snapshots, and WAL update payloads MUST accept exact valid boundaries and
reject padding, trailing bytes, malformed bounds, unknown tags, metadata
changes, and record-body requests that cannot be valid for the encoded
object kind.
10. `RING-OBJECT-026` Object-log append placement MUST preserve stable handles
at ordinary frontier boundaries: a small inline record that exactly fits the
current reserved frontier MUST be written there, and insufficient remaining
frontier space MUST materialize the frontier and continue in the next reserved
ordinary region without changing already returned handles.
11. `RING-OBJECT-030` Object-log append routing MUST classify objects by chunk
count rather than current frontier free space: objects with length less than or
equal to one chunk's logical capacity use `InlineObject`, and objects requiring
more than one chunk use one public `LargeRecordEntry` plus private chunks.
12. `RING-OBJECT-031` Large-object append placement MUST fail impossible
no-progress geometries and MUST keep each private auxiliary or tail chunk span
associated with exactly one public `LargeRecordEntry`.
13. `RING-OBJECT-027` Object-log WAL replay MUST rebuild only the target
object-log collection: records for other collection ids or collection types
MUST NOT alter target state, and lifecycle or transaction markers MUST affect
target updates only when the marker belongs to the target collection.
14. `RING-OBJECT-028` Before returning object bytes, Object-log reads MUST
validate that flushed data-region headers and prologues still identify the live
collection and that large objects expose only public `LargeRecordEntry` handles
with valid auxiliary-region identity, auxiliary links, fixed-slot chunk bounds,
tail chunk ordering, logical positions, and CRCs.

## Large Objects

Small and large object classification is based on chunk count, not on the
current amount of free space in the ordinary frontier. An object whose logical
length is less than or equal to one chunk's logical capacity is a small object
and is stored as an inline public record. An object that requires more than one
chunk is a large object and is published through one public `LargeRecordEntry`
plus private chunks.

Auxiliary regions exist only to materialize large-object data when the
large-object scratch buffer is full before the whole object has been written.
They are not ordinary object-log chain regions, and they do not contain records
from multiple objects. Each auxiliary region belongs to exactly one large object
and is reachable from that object's committed `LargeRecordEntry`.

A large write requires a region-capacity scratch buffer. The writer fills
scratch with private chunks until scratch contains one complete auxiliary-region
image: aligned auxiliary prologue, a whole number of fixed chunk slots, and the
reserved next-link slot. If the object continues, that complete image is
materialized as a transaction-reserved auxiliary region. If another auxiliary
region follows, the previous auxiliary region's reserved next-link slot is
written once to point to it.

If the object ends exactly when scratch contains a complete auxiliary-region
image, that image becomes the final auxiliary region. If the object ends with a
partial scratch image, the writer publishes the object through the ordinary
object-log path by appending a `LargeRecordEntry` followed immediately by the
remaining private tail chunks. There may be zero or more tail chunks, up to one
fewer chunk than fits in an auxiliary region. The large record entry and its
tail chunks are contiguous in ordinary object-log order, although that span may
cross ordinary object-log region boundaries. Public traversal skips tail chunks.

Crossing a chunk, span, or region boundary does not create logical object
padding. `chunk_len` records only logical bytes. Auxiliary chunk zero fill and
ordinary storage write-alignment padding are outside the logical object bytes.

Every auxiliary region is allocated and written inside the large-object
transaction. Before commit, those regions are transaction-owned and recoverable.
The commit publishes the `LargeRecordEntry` that makes the auxiliary chain
reachable from exactly one object. If the transaction aborts before commit, all
reserved auxiliary regions are reclaimed. If it commits, the auxiliary data and
auxiliary links are durable before the large record entry becomes visible.

Large-object append has to distinguish impossible geometry from ordinary
frontier pressure. If the auxiliary prologue, next-link slot, storage geometry,
or `wal_write_granule` leave no valid fixed chunk slot, the object cannot be
represented in that geometry. If the ordinary frontier is merely full after
prior object-log records, those bytes are already part of a reserved address
range, so the frontier is materialized and the publish span continues in the
next reserved ordinary region.

1. `RING-OBJECT-021` Auxiliary regions MUST use the `OLAX` auxiliary prologue
with version `1`, exact fixed chunk slots that divide the auxiliary chunk area,
chunk slots whose physical length is a multiple of `wal_write_granule`, and a
reserved next-link slot that is either erased/all-empty or a CRC-protected
`AuxRegionPointer`.
2. `RING-OBJECT-022` Large-object append placement MUST use one
region-capacity scratch buffer, materialize only complete auxiliary-region
images before object completion, write each prior auxiliary next-link slot at
most once when another auxiliary region follows, and publish any final partial
scratch contents as contiguous private tail chunks immediately after the public
`LargeRecordEntry`.
3. `RING-OBJECT-023` Every auxiliary region written for a large object MUST be
transaction-reserved before write, transaction-owned and recoverable before
commit, reachable from exactly one committed `LargeRecordEntry` after commit,
and reclaimed if the transaction aborts before commit.

## Committed Visibility

The frontier has two ends. The planned end is where the next append will be
placed, while the committed end is the boundary visible to callers. Keeping
those offsets separate lets the object log reserve stable handles and pack
bytes before publication without letting partially recorded objects leak into
reads, traversal, or truncation.

For a standalone append, the object becomes visible only after its WAL update
is durable. For a scoped append transaction, each appended object gets its
final handle immediately, but those handles remain planned until the
transaction commit record is durable. Public operations validate against the
committed end, not the planned end, so a planned handle cannot be read,
traversed, or used as a truncation boundary before commit.

1. `RING-OBJECT-009` Object-log reads, traversal, and truncation MUST
observe only committed object bounds.

## Truncation

Linear storage discards prefixes rather than arbitrary individual
objects. The object log therefore tracks live bounds as a head and tail over
the same internal region, sequence, and offset facts used by handles.
Truncation advances the head to an object boundary by taking a live handle as
the exclusive boundary: objects before that handle are discarded, and the
provided handle remains live. Requiring a handle boundary keeps truncation
aligned with object records without exposing raw offsets to callers.

Whole regions that fall before the new head are returned to Borromean storage.
They are not reused in place by the object log. A later append will allocate
from the storage free-space collection like any other collection operation,
which preserves the storage engine's wear-leveling behavior. If the same
physical region is allocated again later, the object log assigns a new
sequence, so stale handles do not alias new data.

Large-object truncation uses the public `LargeRecordEntry` handle as the
boundary. Retaining that boundary retains its auxiliary chain and any private
tail chunks that follow it in ordinary object-log order. Truncating away a
large record entry frees its whole auxiliary chain. Auxiliary regions contain
data for only one object, so truncation never has to retain unrelated auxiliary
data; the only unavoidable retention is an ordinary object-log region that
still contains the live head.

1. `RING-OBJECT-010` Truncating an object log MUST accept a live
`ObjectLogHandle` as an exclusive boundary, invalidate handles before that
boundary while retaining the boundary handle, and return fully obsolete data
regions to Borromean storage.
2. `RING-OBJECT-029` Large-object truncation MUST retain the auxiliary chain
and private tail chunks for a retained boundary `LargeRecordEntry`, free the
entire auxiliary chain for any truncated-away `LargeRecordEntry`, and retain no
unrelated object data beyond ordinary object-log regions that still contain the
live head.

## Live Traversal

Traversal is deliberately live rather than snapshot-based. A caller asks for
the first committed live object handle, then repeatedly asks for the committed
live handle after the previous one. There is no cursor object, no cursor close,
and no retained-region lease. If truncation removes the handle a caller was
using as its current position, advancing from that handle fails the same way
reading it would fail. The caller can recover by asking for the first live
handle again.

The API distinguishes absence from invalid access. An empty log has no first
handle, and the tail object has no next handle. A stale, truncated, forged, or
corrupt handle is rejected with an object-log error instead of being treated
as end-of-log.

1. `RING-OBJECT-011` Object-log traversal MUST provide a way to obtain
the first live `ObjectLogHandle` and a way to obtain the next live
`ObjectLogHandle` after a provided live handle. Empty logs and tail handles
MUST return no handle, while handles outside the current live log MUST be
rejected as invalid.

## Append Transactions

Some callers need to append a group of objects atomically. The object log
supports that through a scoped append transaction rather than a cursor-like
transaction object that callers must remember to close. The closure receives a
transaction view that can append objects and return their planned stable
handles. When the closure succeeds, the object log writes the commit marker
and publishes the staged objects by advancing committed bounds. When the
closure fails, the object log restores the pre-transaction in-memory view and
cleans up transaction allocations.

Rollback is intentionally narrow. It exists only to clean up an uncommitted
append transaction; committed log state is not rolled back. Transaction start
copies the current region metadata and frontier bytes into caller-owned
rollback memory. Transaction bytes may share the in-memory frontier with
already committed bytes, but only the planned end advances until commit. That
checkpoint gives rollback a clear state to restore without forcing a partially
filled data region to be flushed just to create a rollback boundary.

A transaction may span more than one frontier region. When the active frontier
does not have room for the next transactional object, the object log may
materialize the closed frontier only when the target ordinary data region was
reserved by the transaction. A frontier whose target region existed before the
transaction may still receive planned in-memory records, but the transaction
must fail and roll back instead of forcing that preexisting region to be
written. This preserves stable planned handles while keeping rollback limited
to volatile frontier state and transaction-reserved regions. If cleanup must
return a transaction-reserved region to storage, rollback erases or otherwise
prepares it before freeing it. If power is lost during cleanup, startup
recovery skips the uncommitted object-log updates, returns any remaining
transaction allocations to storage, and records rollback completion.

1. `RING-OBJECT-012` Scoped append transactions MUST keep appended
objects invisible until the durable commit record.
2. `RING-OBJECT-013` Failed or uncommitted append transactions MUST roll
back cleanly by discarding staged object-log state and returning
transaction-reserved regions to storage without making planned handles live.
3. `RING-OBJECT-032` During an append transaction, object-log frontier
materialization MUST write only transaction-reserved ordinary data regions. If
the active frontier's reserved data region existed before the transaction began
and the next append would require materializing that frontier, the append
transaction MUST fail and roll back rather than writing the preexisting
reserved region. The transaction MAY still append planned records to the
in-memory frontier and MAY write large-object auxiliary regions or later
ordinary regions that were allocated by the transaction.
