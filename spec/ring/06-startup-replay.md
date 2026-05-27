# Chapter 6: Startup And Replay

This chapter describes `Opening(OpenMode)`: scan media, recover any
incomplete WAL rotation, replay retained WAL records through the shared
state-machine rules, validate live collection data, and finish pending
recovery work.

Mechanism review:

- **Purpose**: turn durable media into the stable runtime vector without
  inventing recovery-specific collection or allocator semantics.
- **State**: scanned region headers, WAL chain, replay tracker,
  pending WAL-recovery boundary, staged regions, pending reclaims, and
  live collection validation state.
- **Named operations**: `OpenStorage` orchestrates replay and may invoke
  recovery sub-operations such as `RotateWalTail` completion and
  `ReclaimRegion`.
- **Durable edge sequence**: normal replay is read-only; recovery writes
  only the edges required to finish an incomplete rotation, close a WAL
  recovery boundary, or complete safe reclaim work.
- **Replay effect**: retained WAL records are applied by the same
  `ApplyWalRecord` table used by foreground operation.
- **Crash cuts**: opening can be retried after reset because every
  recovery write either preserves the previous replay result or moves to
  another replayable prefix.

## Startup Replay Algorithm

Startup recovery is the concrete `Opening(OpenMode)` procedure. It
reconstructs the stable runtime vector by scanning durable media,
walking the WAL chain, and applying each retained WAL record through
`ApplyWalRecord`. The detailed steps below define validation,
discovery, and recovery behavior that surrounds those shared
per-record transitions.

Startup recovery reconstructs eight things:

1. `RING-STARTUP-RESULT-001` Durable collection states (live heads plus dropped tombstones)
2. `RING-STARTUP-RESULT-002` In-memory working state for collections with uncommitted updates
3. `RING-STARTUP-RESULT-003` Durable free-list head
4. `RING-STARTUP-RESULT-004` Reserved `ready_region`, if an allocation was started but not yet
committed by `head` or `link`
5. `RING-STARTUP-RESULT-005` Runtime `free_list_tail`, reconstructed from the free-pointer chain
after the durable free-list head is known
6. `RING-STARTUP-RESULT-006` Runtime `max_seen_sequence`, initially the largest `sequence`
observed in any valid region header during region scan, then advanced
further if startup recovery initializes an incomplete WAL rotation
7. `RING-STARTUP-RESULT-007` Ordered incomplete reclaim transactions that still need post-replay
recovery work
8. `RING-STARTUP-RESULT-008` Ordered staged regions that have left
`ready_region` but are not yet known to be free

Algorithm:

1. `RING-STARTUP-001` Read `StorageMetadata`, validate
`metadata_checksum`, and validate static geometry (`region_size`,
`region_count`, `min_free_regions`, `erased_byte`,
`wal_write_granule`, `wal_record_magic`, and storage version support).
2. `RING-STARTUP-002` Scan all regions, collect candidate WAL regions
(`collection_id == 0` plus `collection_format = wal_v1`) with valid
headers, and track
`max_seen_sequence` as the largest `sequence` value seen in any valid
region header.
3. `RING-STARTUP-003` Select WAL tail as the unique candidate WAL region with the largest
valid sequence. If no candidate WAL region exists, or if multiple
candidate WAL regions share that largest valid sequence, return an
error.
4. `RING-STARTUP-004` Read and validate the `WalRegionPrologue` stored at the start of the
tail region's user-data area, and use its `wal_head_region_index` as
the initial WAL-head candidate. Then scan that tail region using the
same aligned candidate-start and record-validation rules defined in
step 6, and let the last valid
`head(collection_id = 0, collection_type = wal, region_index)`
record override that candidate.
5. `RING-STARTUP-005` Walk the WAL region chain from the resulting WAL head to tail using
`link` records.
If a `link` is missing/invalid before reaching the known tail, return
an error (corrupted WAL chain).
If the known tail contains a trailing `link` whose target header is
missing/corrupt or has the wrong sequence, treat this as an incomplete
rotation after `link`. Use the known tail as replay tail until that
recovery finishes.
If instead the known tail's last valid record is an
`alloc_begin(next_region_index, free_list_head_after)` whose aligned
end offset leaves at least `wal_link_reserve` and fewer than
`wal_rotation_reserve` unwritten bytes in that region, treat this as
an incomplete rotation before `link`. That reserve-window placement is
what makes this durable tail `alloc_begin` unambiguously the
WAL-rotation-start record rather than an ordinary allocation
reservation.
For incomplete rotation recovery:
if a durable trailing `link(next_region_index, expected_sequence)` is
already present, use that `expected_sequence`;
otherwise let `expected_sequence = max_seen_sequence + 1`, append and
sync the missing `link(next_region_index, expected_sequence)` into the
reserved tail space, and treat any failure of that recovery append as a
startup error.
Then finish initializing the target WAL region:
erase target region if needed, write a valid WAL header with
`collection_id = 0` and `sequence = expected_sequence`, then write a
valid `WalRegionPrologue` whose `wal_head_region_index` equals the WAL
head already determined for this WAL chain before the incomplete
rotation target is considered. Sync the initialized target region, set
in-memory `max_seen_sequence = expected_sequence`, and use the target
region as the active append tail. If this recovery init fails, startup
fails with error.
6. `RING-STARTUP-006` Parse records in WAL order (region order, then offset order).
Record parsing begins only at offsets aligned to `wal_write_granule`
and greater than or equal to `wal_record_area_offset` within each WAL
region.
Maintain a replay-local flag `pending_wal_recovery_boundary`,
initially clear.
If an aligned candidate start byte equals `erased_byte`, treat that
slot as currently unwritten and stop scanning that WAL region.
If the aligned start byte equals `wal_record_magic`, parse the record.
If parsing or checksum validation fails, treat that aligned slot as a
corrupt/torn WAL slot, set `pending_wal_recovery_boundary`, and keep
scanning forward in aligned `wal_write_granule` steps.
If the aligned start byte is neither `erased_byte` nor
`wal_record_magic`, treat that aligned slot as corrupt/torn WAL bytes,
set `pending_wal_recovery_boundary`, and keep scanning forward in
aligned `wal_write_granule` steps. Do not attempt to decode or repair
those corrupt bytes.
If a later valid record is found while
`pending_wal_recovery_boundary` is set, that record must be
`wal_recovery`; otherwise return an error.
At the end of each reachable non-tail WAL region,
`pending_wal_recovery_boundary` must be clear; otherwise return an
error.
After scanning the tail region, recover the append point as the first
aligned slot whose first byte is `erased_byte` after the last valid
replayed tail record. If no such slot exists, the tail region is full.
7. `RING-STARTUP-007` Maintain replay state:
per collection optional live `collection_type`, explicit collection
state, `basis_pos`, and
`pending_updates`, plus global `last_free_list_head`, optional
reserved `ready_region`, ordered staged regions, ordered pending region reclaims, and the
replay-local `pending_wal_recovery_boundary`.
Initialize `last_free_list_head` to `Some(1)` iff `region_count >= 2`,
otherwise `None`, because format establishes that as the initial
durable free-list head. Later `alloc_begin` and `free_list_head`
records override this baseline in replay order.
8. `RING-STARTUP-008` On `new_collection(collection_id, collection_type)`:
if `collection_id` is already tracked, return an error.
otherwise create replay state for that collection with durable basis
`EmptyClean`, set tracked `collection_type` from the record, set
`basis_pos` to this record's WAL position, and start with no pending
updates.
9. `RING-STARTUP-009` On `update(collection_id)`:
if `collection_id` is not tracked, return an error.
if that collection's collection state is `Dropped`, return an error.
append to `pending_updates` for that collection.
10. `RING-STARTUP-010` On `snapshot(collection_id, collection_type)`:
if `collection_id` is not tracked, create replay state for that
collection because an earlier `new_collection` may have been reclaimed,
and set tracked `collection_type` from this record.
if that collection's collection state is `Dropped`, return an error.
if this record's `collection_type` does not match the tracked
`collection_type`, return an error.
set collection state to `WALSnapshotClean`, set `basis_pos` to this
record's WAL position, and clear older pending updates for that
collection at WAL positions up to and including this snapshot.
11. `RING-STARTUP-011` On `alloc_begin(region_index, free_list_head_after)`:
if `ready_region` is already set, return an error because replay found
two unmatched allocation reservations.
if `last_free_list_head = none`, return an error because allocation
cannot consume an empty durable free list.
if `last_free_list_head != region_index`, return an error because
`alloc_begin` did not consume the current durable free-list head.
set durable `last_free_list_head` to `free_list_head_after`.
set `ready_region = region_index`.
12. `RING-STARTUP-011A` On `stage_region(region_index)`:
if `ready_region != region_index`, return an error.
otherwise append `region_index` to staged regions and clear
`ready_region`.
13. `RING-STARTUP-012` On `head(collection_id, collection_type, region_index)`:
if `collection_id = 0`, this is a WAL-head control record. Its replay
effect was already consumed in step 4 while determining the WAL-head
candidate from the tail region. If `collection_type != wal`, return an
error; otherwise ignore this record during the main per-record replay
pass.
otherwise, if `collection_id` is not tracked, create replay state for
that collection because an earlier `new_collection` may have been
reclaimed, and set tracked `collection_type` from this record.
if that collection's collection state is `Dropped`, return an
error.
if this record's `collection_type` does not match the tracked
`collection_type`, return an error.
if the target region header is missing, corrupt, or has a different
`collection_id`, return an error.
Core replay does not impose any further global `collection_format`
check for user collections; if that region is later loaded as a
committed basis, its collection implementation validates that the
stored `collection_format` is one it understands.
set collection state to `RegionClean`, set `basis_pos` to this
record's WAL position, and clear WAL updates/snapshots older than this
basis decision.
if `ready_region = region_index`, clear `ready_region`;
otherwise leave `ready_region` unchanged because this `head` either
retargeted the collection to an already allocated existing region or
refers to a region whose historical `alloc_begin` was already consumed
and later reclaimed.
If `region_index` is in staged regions, remove it from staged regions
because the head directly makes that region live.
14. `RING-STARTUP-013` On `link(next_region_index, expected_sequence)`:
if `ready_region = next_region_index`, clear `ready_region`.
otherwise leave `ready_region` unchanged because this `link` may refer
to a WAL-region allocation whose historical `alloc_begin` was already
consumed and later reclaimed.
If `next_region_index` is in staged regions, remove it from staged
regions because the link directly makes that region part of the WAL
chain.
15. `RING-STARTUP-014` On `drop_collection(collection_id)`:
if `collection_id` is not tracked, create replay state for that
collection because older retained basis records may already have been
reclaimed; record this collection as durably `Dropped`, with no
retained live `collection_type`, set `basis_pos` to this record's WAL
position, and leave no pending updates.
otherwise if that collection's collection state is `Dropped`,
return an error.
otherwise set collection state to `Dropped`, set `basis_pos` to this
record's WAL position, and clear all pending updates for that
collection.
16. `RING-STARTUP-015` On `free_list_head(region_index_or_none)`:
set tentative durable `last_free_list_head` to `region_index_or_none`.
17. `RING-STARTUP-016` On `reclaim_begin(region_index)`:
append `region_index` to pending reclaims unless a later matching
`reclaim_end` removes it. If `region_index` is in staged regions,
remove it from staged regions because reclaim now owns the cleanup.
18. `RING-STARTUP-017` On `reclaim_end(region_index)`:
mark the matching pending reclaim as finished.
19. `RING-STARTUP-018` On `wal_recovery()`:
if `pending_wal_recovery_boundary` is clear, return an error.
otherwise clear `pending_wal_recovery_boundary`.
20. `RING-STARTUP-019` After replay, for each collection:
reconstruct its durable basis from the collection state. If the state
is `EmptyClean` or `EmptyDirty`, the basis is the empty collection
declared by `new_collection`; if that collection has post-basis
updates, initialize empty mutable state in RAM and apply those
`pending_updates` in WAL order. If the state is `RegionClean` or
`RegionDirty`, the basis may remain in-place in flash until a read or
mutation needs to materialize it. If the state is `WALSnapshotClean`
or `WALSnapshotDirty` and the collection has post-basis updates, load
that snapshot into RAM and apply the remaining `pending_updates` in
WAL order to reconstruct mutable working state. If the state is
`WALSnapshotClean` and there are no post-basis updates, the snapshot
may remain dormant until the next mutation, but it must be loaded into
RAM before accepting that mutation. If the state is `Dropped`, do not
reconstruct mutable state for that collection and do not accept
further mutations for that collection id.
21. `RING-STARTUP-020` Initialize allocator state from `last_free_list_head`.
22. `RING-STARTUP-021` Reconstruct runtime `free_list_tail` by following free-pointer
links starting at `last_free_list_head` until reaching a free region
whose free-pointer slot is uninitialized.
If this walk encounters a checksum-invalid or malformed free-pointer
footer, a region that is not
a valid member of that free-list chain, or exceeds `region_count`
visited regions before reaching an uninitialized tail slot, return an
error because the
durable free-list head does not name a valid free-list chain.
If `last_free_list_head = none`, then `free_list_tail = none`.
23. `RING-STARTUP-022` If `ready_region` is set, hold it in memory as the next region to
use before consuming another free-list entry.
24. `RING-STARTUP-023` Keep `max_seen_sequence` as the runtime source of the next region
sequence. The next newly allocated region must use
`max_seen_sequence + 1` as its header `sequence`, then update
`max_seen_sequence` in memory to that new value.
25. `RING-STARTUP-024` After live collection type and retained data
validation has succeeded, process each pending reclaim in WAL order:
if the target region is still reachable from any live collection head
or the WAL chain, leave it allocated because the reclaim did not reach
the detach point durably.
If the target region is unreachable from live state and not yet in the
free-list chain, complete the free-list append using the Region
Reclaim procedure.
If the target region is already reachable from the free-list chain,
finish the reclaim transaction by appending `reclaim_end(region_index)`.
If an ordered staged region is not reachable from validated live
collection state or the WAL chain, recover it through the same
WAL-tracked reclaim procedure; if it is reachable, remove it from
staged runtime state.
26. `RING-STARTUP-025` If replay encountered a torn or checksum-invalid tail record,
retain all state recovered from earlier complete records. The WAL head
is unchanged. Replay may still recover and apply later valid tail
records that begin after the torn bytes, but the first such later valid
record must be `wal_recovery`. The recovered append point is the first
aligned slot whose first byte is `erased_byte` after the last valid
replayed tail record, so later WAL appends may resume there while the
ignored corrupt span before that point remains uninterpreted until that
region is reclaimed or erased for reuse.
27. `RING-STARTUP-026` If replay yields a live collection whose
`collection_type` is unsupported by the implementation, startup MUST
fail before any pending reclaim or abandoned staged region is freed
based on collection reachability.
28. `RING-STARTUP-027` If replay yields a live collection with unsupported or invalid retained
    collection data under that collection's normative specification, startup MUST fail before open
    succeeds and before any pending reclaim or abandoned staged region is freed based on
    collection reachability.
29. `RING-STARTUP-028` A dropped tombstone whose old
`collection_type` is unsupported MAY remain as inert metadata and does
not by itself require startup failure.

## Startup Replay Implementation Requirements

These requirements cover implemented startup replay edge cases and validation helpers.

1. `RING-IMPL-REGRESSION-046` Startup tail selection MUST ignore regions with nonzero collection_id
   even when their format is wal_v1 while still tracking max seen sequence.
2. `RING-IMPL-REGRESSION-047` Startup replay MUST preserve staged regions when a WAL head-control
   record is replayed.
3. `RING-IMPL-REGRESSION-048` Startup replay MUST preserve staged regions when non-map collection
   head and drop records are replayed.
4. `RING-IMPL-REGRESSION-049` Startup replay MUST count multiple live collections independently.
5. `RING-IMPL-REGRESSION-050` Startup replay MUST accept a committed-region head basis and recover
   the collection basis, collection type, and max seen sequence from that region.
6. `RING-IMPL-REGRESSION-051` Startup replay MUST accept a reclaimed historical head after
   replacement and recover the live replacement head with no pending reclaim.
7. `RING-IMPL-REGRESSION-052` Startup replay MUST track pending updates on an empty collection
   basis and preserve their count.
8. `RING-IMPL-REGRESSION-053` Startup replay MUST reject update records that appear after a
   collection drop tombstone for the same collection.
9. `RING-IMPL-REGRESSION-054` Strict WAL-region reads MUST reject regions whose collection_id is
   nonzero even if collection_format is wal_v1.
10. `RING-IMPL-REGRESSION-055` WAL target validation MUST require both collection_id 0 and
    collection_format wal_v1.
11. `RING-IMPL-REGRESSION-056` Live committed-region basis validation MUST reject a region whose
    header belongs to a different collection.
12. `RING-IMPL-REGRESSION-057` Region index validation MUST reject a region_index equal to
    region_count.
13. `RING-IMPL-REGRESSION-058` Startup replay MUST recover a WAL rotation after a durable link by
    selecting the linked tail, resetting tail append offset, updating allocator state, and advancing
    max sequence.
14. `RING-IMPL-REGRESSION-059` Startup replay MUST recover a WAL rotation when alloc_begin is
    durable but link is absent and only rotation reserve remains.
15. `RING-IMPL-REGRESSION-060` Startup replay MUST recover a WAL rotation when only the link record
    fits after alloc_begin at the tail boundary.
16. `RING-IMPL-REGRESSION-061` Startup replay MUST reject an unrecovered corrupt boundary in a
    non-tail WAL region as a broken WAL chain.
17. `RING-IMPL-REGRESSION-062` Opening a freshly formatted store MUST initialize allocator
    free-list head and tail from the formatted free-list chain.

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 180}} }%%
flowchart TD
    OpenStore([Open store])
    ReadMeta["`Read and validate storage metadata`"]
    ScanRegions["`Scan regions and track max seen sequence`"]
    TailOk{"`Unique valid WAL tail?`"}
    Fail([Open fails])
    ReadHead["`Read WAL prologue and derive WAL head candidate`"]
    ChainOk{"`WAL chain valid?`"}
    Rotate{"`Incomplete WAL rotation?`"}
    RecoverRotate["`Recover missing link or finish target WAL init`"]
    Replay["`Replay reachable WAL records in WAL order`"]
    Rebuild["`Rebuild collection state allocator state and free list tail`"]
    FinishReclaims["`Finish pending reclaims that detached durably`"]
    OpenReady([Open complete])

    OpenStore --> ReadMeta --> ScanRegions --> TailOk
    TailOk -->|no| Fail
    TailOk -->|yes| ReadHead --> ChainOk
    ChainOk -->|no| Fail
    ChainOk -->|yes| Rotate
    Rotate -->|yes| RecoverRotate --> Replay
    Rotate -->|no| Replay
    Replay --> Rebuild --> FinishReclaims --> OpenReady
```

## Why Reclaimed WAL Regions Cannot Confuse Startup

Startup region scan may encounter free-list regions whose stale header
bytes still look like old WAL headers. That does not let a reclaimed
WAL region take over bootstrap.

1. `RING-BOOTSTRAP-001` Startup chooses the WAL tail as the candidate WAL region with the
largest valid `sequence`.
2. `RING-BOOTSTRAP-002` Each newly allocated region uses `sequence = max_seen_sequence + 1`,
then advances `max_seen_sequence` in memory.
3. `RING-BOOTSTRAP-003` Therefore, once a WAL region has been superseded by a later live WAL
tail or by any later successful region allocation, that reclaimed
region's stale `sequence` is permanently older than the current maximum
durable sequence seen at startup.
4. `RING-BOOTSTRAP-004` A reclaimed former WAL region may still be discovered during region
scan, but it cannot win WAL-tail selection unless the monotonic
sequence rule has already been violated.
5. `RING-BOOTSTRAP-005` Startup derives the WAL head only from the selected tail's
`WalRegionPrologue` plus any later `head(collection_id = 0, ...)`
records found in that same tail region. Stale headers in free-list
regions therefore do not influence WAL-head recovery once they lose
tail selection.

Under the monotonic-sequence rule, stale free-list WAL headers may be
visible during scan, but they cannot outrank the live WAL tail and so
cannot redirect startup onto the wrong WAL chain.

## no_std Tracker Types (Rust)

The replay and allocator terms above map to the following explicit
`no_std` tracker state. These structs are runtime state, not on-disk
layout. Region references in tracker state are indexes into the
configured region array, not opaque identifiers.

```rust
#![no_std]

use heapless::Vec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionIndex(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionType(pub u16);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionSequence(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalOffset(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalPosition {
  pub region_index: RegionIndex,
  pub offset: WalOffset,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectionMachineState {
  EmptyClean,
  EmptyDirty,
  RegionClean { region_index: RegionIndex },
  RegionDirty { region_index: RegionIndex },
  WALSnapshotClean { wal_pos: WalPosition },
  WALSnapshotDirty { wal_pos: WalPosition },
  Dropped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionReplayState {
  pub collection_id: CollectionId,
  // `None` is used only for a retained drop-only tombstone whose older
  // type-bearing records were reclaimed.
  pub collection_type: Option<CollectionType>,
  pub state: CollectionMachineState,
  // WAL position of the durable basis decision record that established
  // the current durable basis (`new_collection`, `snapshot`,
  // `drop_collection`, or `head`).
  pub basis_pos: WalPosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingUpdateRef {
  pub collection_id: CollectionId,
  pub wal_pos: WalPosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingReclaim {
  pub region_index: RegionIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagedRegion {
  pub region_index: RegionIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FreeListTracker {
  // Durable allocator cursor reconstructed from replay decisions.
  pub last_free_list_head: Option<RegionIndex>,
  // Region reserved by `alloc_begin` but not yet consumed by a durable
  // `head`, `link`, or `stage_region` record.
  pub ready_region: Option<RegionIndex>,
  // Runtime-only convenience for append-on-free operations.
  pub free_list_tail: Option<RegionIndex>,
}

pub struct ReplayTracker<
  const MAX_COLLECTIONS: usize,
  const MAX_PENDING_UPDATES: usize,
  const MAX_PENDING_RECLAIMS: usize,
> {
  pub free_list: FreeListTracker,
  pub max_seen_sequence: RegionSequence,
  pub collections: Vec<CollectionReplayState, MAX_COLLECTIONS>,
  pub pending_updates: Vec<PendingUpdateRef, MAX_PENDING_UPDATES>,
  pub staged_regions: Vec<StagedRegion, MAX_PENDING_RECLAIMS>,
  pub pending_reclaims: Vec<PendingReclaim, MAX_PENDING_RECLAIMS>,
}
```

`heapless` dependency form:

```toml
[dependencies]
heapless = { version = "0.8", default-features = false }
```

Field mapping to this spec:

1. `CollectionReplayState.state` maps to the explicit collection
submachine state for a tracked collection. `NoCollection` is represented
by the absence of a tracker entry for that collection id.
2. `WalPosition` identifies a WAL record by WAL region index plus
byte offset within that region.
3. `CollectionReplayState.basis_pos` is `B(c)`, the WAL position of
the durable basis decision record for that collection's current clean
or dirty basis.
4. `CollectionReplayState.collection_type` is the replay-tracked
collection type established by the earliest retained valid
type-bearing record for that collection and validated by later
type-bearing records. It is `None` only for a drop-only retained
tombstone whose older type-bearing records were reclaimed.
5. `FreeListTracker.last_free_list_head` maps to replay
`last_free_list_head`.
6. `FreeListTracker.ready_region` maps to replay `ready_region`.
7. `FreeListTracker.free_list_tail` is runtime state reconstructed by
walking the free-pointer chain from `last_free_list_head`; reclaim uses
it to link `t_prev.next_tail = r`.
8. `ReplayTracker.staged_regions` maps to replay's ordered staged
regions that are allocated but no longer occupy `ready_region`.
9. `ReplayTracker.pending_reclaims` maps to replay's ordered pending
region reclaims that remain incomplete after WAL replay and are
processed during post-replay recovery.
10. `ReplayTracker.max_seen_sequence` is initialized from the largest
region `sequence` value observed during startup region scan, and may be
advanced further if startup recovery initializes an incomplete WAL
rotation. Each newly allocated region uses the next value
(`max_seen_sequence + 1`), then updates this runtime field.
