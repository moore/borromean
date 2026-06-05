# Object Log Collection

## Purpose

`ObjectLog` is a durable opaque object collection intended to support
higher-level linear log storage. It is meant for callers that need stable
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
2. `RING-OBJECT-005` `ObjectLogHandle` MUST NOT expose public field
access or an unchecked public field constructor, and object-log reads MUST
reject handles that do not name a live reserved frame.
3. `RING-OBJECT-006` Opening an object-log collection by id MUST fail
if the live collection exists with a non-object-log collection type.
4. `RING-OBJECT-011` The durable object-log handle encoding MUST be
exactly 12 bytes with no padding: bytes 0 through 3 contain
`region_index` as a little-endian `u32`, bytes 4 through 7 contain
`sequence` as a little-endian `u32`, and bytes 8 through 11 contain
`offset` as a little-endian `u32`.

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
that logical frontier region, followed by packed object frames. A flushed read
checks both the region header and the object-log prologue before returning
bytes, so a stale handle cannot silently read from an unrelated later use of
the same physical region. After flush, the collection persists metadata that
describes the flushed regions and any still-live frontier state.

Root metadata is intentionally small and opaque. Higher-level storage can use
it to record its own current root, checkpoint, or log metadata without forcing
Borromean core to understand that higher-level structure.

1. `RING-OBJECT-002` Flushing an object-log frontier MUST write the
frontier bytes into the previously reserved physical data region, persist
metadata sufficient to read flushed handles after reopen, and assign a
new sequence to a later reserved frontier region.
2. `RING-OBJECT-004` Object-log root metadata MUST be persisted through
WAL state and restored when the collection is reopened.

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

1. `RING-OBJECT-008` Object-log reads, traversal, and truncation MUST
observe only committed object bounds.

## Truncation

Linear storage normally discards prefixes rather than arbitrary individual
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

1. `RING-OBJECT-003` Truncating an object log MUST accept a live
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

1. `RING-OBJECT-007` Object-log traversal MUST provide a way to obtain
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

1. `RING-OBJECT-009` Scoped append transactions MUST keep appended
objects invisible until the durable commit record.
2. `RING-OBJECT-010` Failed or uncommitted append transactions MUST roll
back cleanly by discarding staged object-log state and returning
transaction-reserved regions to storage without making planned handles live.
