# Object Log Collection

## Purpose

`ObjectLog` is a durable opaque object collection intended to support
external storage systems built on top of it. It is meant for callers that need stable
object addresses, durable append semantics, prefix truncation, and efficient
packing on flash-like media.

The hard part is that those goals pull in different directions. Returning a
handle before an object is flushed is useful to callers, but a handle that
changes after flush is not durable. Writing every object directly to its final
region would make handles easy, but it would also force each object append to
respect region/page programming boundaries and waste space for small objects.
Reusing the same log region in place would reduce allocation churn, but it
would work against wear leveling and make stale handles harder to detect.

The design resolves those tensions by separating address reservation from
physical materialization. An append first reserves the physical data region and
the exact frame offset that will eventually contain the object. The bytes are
then persisted through a WAL update and packed into an in-memory frontier for
that reserved region. Later, flush materializes the frontier into the same
reserved region, so the handle remains valid before and after flush.

## API And Handles

Object-log handles are deliberately opaque to callers. The collection stores
region, object-log region serial, and offset internally because those facts are
needed to find bytes and reject stale handles, but callers should not inspect
or manufacture those parts. A handle is a capability returned by append and
accepted by later object-log operations. That keeps the public API from
promising a particular bit layout and prevents callers from constructing
plausible-looking handles that never came from the collection.

This collection also has its own durable `collection_type`. That lets generic
Borromean open and replay paths recognize object-log collections without
hard-coding application-specific behavior into core storage. The collection-specific
module owns the object payload formats and validation rules; Borromean core
only needs to know that the collection type is supported and which empty
snapshot payload represents an empty object log during WAL reclaim.

1. `RING-OBJECT-001` Appending an object MUST return an opaque
`ObjectLogHandle` that names the reserved final data-region frame, and
reopening the collection MUST
reconstruct unflushed frontier objects from retained WAL updates.
2. `RING-OBJECT-002` `ObjectLogHandle` MUST remain opaque to external
callers: it MUST NOT expose public field access, an unchecked public field
constructor, or debug formatting that reveals internal handle components.
3. `RING-OBJECT-003` Opening an object-log collection by id MUST fail
if the live collection exists with a non-object-log collection type.
4. `RING-OBJECT-004` The durable object-log handle encoding MUST be
exactly 16 bytes with no padding: bytes 0 through 3 contain
`region_index` as a little-endian `u32`, bytes 4 through 11 contain
`sequence` as a little-endian `u64`, and bytes 12 through 15 contain
`offset` as a little-endian `u32`.
5. `RING-OBJECT-005` Object-log reads MUST reject handles that do not
name a live reserved frame.

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
by packed object frames. A flushed read checks the region header and the full
object-log prologue before returning bytes, so a stale handle cannot silently
read from an unrelated later use of the same physical region or from a region
formatted for a different object-log record format. After flush, the collection
persists metadata that describes the flushed regions and any still-live
frontier state.

Log metadata is a non-empty immutable opaque byte sequence supplied when the
object log is created. The object log stores and validates it but never
interprets it. It is part of the durable identity and format of the log, not an
object, handle, checkpoint, head, tail, or traversal position. Opening a log
restores and exposes the stored metadata so callers can decide how to interpret
object bytes without knowing the metadata before open. Appends, reads,
traversal, truncation, and append transactions do not modify it.

The object-log data-region prologue is encoded before object frames. Integer
fields are little-endian, and object frames begin immediately after the
metadata bytes. Its fields are:

- `magic: [u8; 4]`: the literal bytes `OLOG`, used to identify the region as
  an object-log data region before interpreting the rest of the prologue.
- `version: u16`: the object-log data prologue format version, currently `1`,
  used to reject data regions whose prologue or frame layout is not understood.
- `sequence: u64`: the object-log region serial named by handles for frames in
  this region, used with the physical region index to reject stale handles
  after storage reuses a region for a later object-log frontier.
- `log_metadata_len: u32`: the byte length of the immutable log metadata copy
  that follows, used to validate bounds and locate the first object frame.
- `log_metadata: [u8; log_metadata_len]`: a verbatim copy of the collection's
  immutable log metadata, used to reject flushed regions whose durable format
  identity differs from the collection being opened or read.

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
aligned with object frames without exposing raw offsets to callers.

Whole regions that fall before the new head are returned to Borromean storage.
They are not reused in place by the object log. A later append will allocate
from the storage free list like any other collection operation, which preserves
the storage engine's wear-leveling behavior. If the same physical region is
allocated again later, the object log assigns a new sequence, so stale handles
do not alias new data.

1. `RING-OBJECT-010` Truncating an object log MUST accept a live
`ObjectLogHandle` as an exclusive boundary, invalidate handles before that
boundary while retaining the boundary handle, and return fully obsolete data
regions to Borromean storage.

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
does not have room for the next transactional object, the object log may write
that closed frontier to its reserved region and continue in another reserved
region. That write does not publish the transaction: reads, traversal, and
truncation still stop at committed bounds until the commit record is durable.
If cleanup must return a transaction-reserved region to storage, rollback
erases or otherwise prepares it before freeing it. If power is lost during
cleanup, startup recovery skips the uncommitted object-log updates, returns
any remaining transaction allocations to storage, and records rollback
completion.

1. `RING-OBJECT-012` Scoped append transactions MUST keep appended
objects invisible until the durable commit record.
2. `RING-OBJECT-013` Failed or uncommitted append transactions MUST roll
back cleanly by discarding staged object-log state and returning
transaction-reserved regions to storage without making planned handles live.
