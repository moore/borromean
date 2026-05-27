# Chapter 9: Current Implementation Coverage

This appendix records requirements that trace the current implementation
surface and regression tests. They remain in this ring spec for this
reorganization pass, but they may later move to the implementation spec
once the state-machine refactor has a stable Rust shape.

The requirements in this chapter are not the conceptual source of the
ring design. They describe implementation behavior that is currently
covered by tests and kept here so traceability remains continuous while
the specification is being refactored toward the operation-first model.

## Storage Runtime State Requirements

These requirements cover implemented runtime state updates for formatting, opening, appending,
rotation, reclaim, and WAL/state facade helpers.

1. `RING-IMPL-REGRESSION-063` Committed region writes MUST accept a payload that exactly fills
   committed payload capacity and persist the full payload bytes.
2. `RING-IMPL-REGRESSION-064` Formatting storage MUST return fresh runtime state with metadata, WAL
   head/tail, allocator, collection, and reclaim fields initialized.
3. `RING-IMPL-REGRESSION-065` WAL record visitation MUST report snapshot and update records after a
   new collection in durable WAL order.
4. `RING-IMPL-REGRESSION-066` Opening storage MUST return replayed runtime state with append
   offset, max sequence, collection type, committed basis, and pending update count.
5. `RING-IMPL-REGRESSION-067` Opening storage MUST complete reclaims for regions already on the
   free list and clear pending reclaim state.
6. `RING-IMPL-REGRESSION-068` Opening storage MUST discard pending reclaim records for regions
   still reachable from live collection state.
7. `RING-IMPL-REGRESSION-069` Appending a new collection and update MUST refresh runtime collection
   state and pending update count.
8. `RING-IMPL-REGRESSION-070` Appending a snapshot MUST move the collection to WAL snapshot basis
   and clear prior pending updates.
9. `RING-IMPL-REGRESSION-071` Appending head and drop records MUST refresh runtime basis to
   committed region and then dropped tombstone while reducing tracked live collection count.
10. `RING-IMPL-REGRESSION-072` Appending WAL recovery MUST clear pending recovery boundary and
    advance append offset; appending free-list-head MUST refresh allocator head and tail.
11. `RING-IMPL-REGRESSION-133` Control-record appends MUST refresh the in-memory runtime state
    without reopening and replaying the WAL.
12. `RING-IMPL-REGRESSION-134` Completing reclaim MUST refresh the free-list tail from footers,
    not by reopening the store.
13. `RING-IMPL-REGRESSION-073` WAL rotation start/finish appends MUST reserve the next free region,
    advance allocator state, then move WAL tail to the new region and clear ready_region.
14. `RING-IMPL-REGRESSION-074` WAL rotation MUST initialize the new WAL region at
    `max_seen_sequence + 1` and update runtime max_seen_sequence.
15. `RING-IMPL-REGRESSION-075` Reopening with uncommitted staged regions MUST reclaim staged
    regions and leave no ready or staged regions live.
16. `RING-IMPL-REGRESSION-076` Staging a region MUST reject region indexes that do not match the
    current ready_region.
17. `RING-IMPL-REGRESSION-077` Normal WAL appends MUST reject writes that would consume rotation
    reserve until WAL rotation completes, after which appends may continue.
18. `RING-IMPL-REGRESSION-078` WAL rotation start MUST reject calls made before the WAL tail has
    entered the rotation window.
19. `RING-IMPL-REGRESSION-079` Head append room checks MUST perform WAL rotation when the current
    tail lacks room for a head record.
20. `RING-IMPL-REGRESSION-080` Stage-region append room checks MUST reject staging when allocator
    state no longer matches the target region.
21. `RING-IMPL-REGRESSION-081` Encoded append reserve checks for alloc_begin MUST require a free
    region and return WalRotationRequired when none remains.
22. `RING-IMPL-REGRESSION-082` Encoded append reserve checks MUST allow alloc_begin when the tail
    has exactly the rotation reserve plus encoded record length remaining.
23. `RING-IMPL-REGRESSION-083` WAL-head reclaim classification MUST copy only head records that
    still reference the retained live region and skip stale head records.
24. `RING-IMPL-REGRESSION-084` WAL-head reclaim classification MUST copy drop tombstones only for
    collections that remain dropped and skip drops for live collections.
25. `RING-IMPL-REGRESSION-085` Foreground allocation headroom checks MUST reject allocations that
    would consume the configured minimum free-region reserve.
26. `RING-IMPL-REGRESSION-086` WAL-head reclaim copying MUST stop cleanly when a copied tail record
    ends exactly at the region end.
27. `RING-IMPL-REGRESSION-087` Live-state reachability checks MUST NOT parse non-map collection
    heads as maps.
28. `RING-IMPL-REGRESSION-088` Live-state reachability checks MUST follow live map manifest heads to
    referenced run regions.
29. `RING-IMPL-REGRESSION-089` Dropping a staged region in memory MUST remove only the matching
    staged region and preserve other staged regions.
30. `RING-IMPL-REGRESSION-090` WAL record visitation MUST process a tail record that ends exactly at
    the append limit and then stop.
31. `RING-IMPL-REGRESSION-091` WAL-chain membership checks MUST follow durable link targets to
    determine whether a region belongs to the chain.
32. `RING-IMPL-REGRESSION-092` CollectionId helpers MUST expose little-endian bytes and checked
    increment semantics, returning none on u64 overflow.
33. `RING-IMPL-REGRESSION-093` Storage facade accessors MUST reflect underlying runtime state and
    tracked collection metadata.
34. `RING-IMPL-REGRESSION-094` Storage facade raw WAL wrapper methods MUST update runtime
    collection, allocator, free-list, and reclaim state.
35. `RING-IMPL-REGRESSION-095` Storage facade WAL recovery append MUST reject recovery records when
    no recovery boundary is pending.
36. `RING-IMPL-REGRESSION-096` Storage facade recovery status MUST report pending WAL recovery
    boundaries and clear them after appending wal_recovery.
37. `RING-IMPL-REGRESSION-104` Storage append operations MUST persist new collection and update
    records so reopening through flash restores the collection and pending update state.
38. `RING-IMPL-REGRESSION-105` WAL-head reclaim MUST update runtime WAL head and tail to a fresh
    continuation region.
39. `RING-IMPL-REGRESSION-106` WAL-head reclaim MUST rewrite a live `EmptyClean` map as a WAL
    snapshot basis while preserving pending updates.
40. `RING-IMPL-REGRESSION-107` Internal WAL rotation with a large pending record MUST bridge an
    early rotation-window gap without surfacing InvalidRotationWindow to the caller.
41. `RING-IMPL-REGRESSION-108` A long mixed map workload MUST preserve collection identity across
    writes, deletes, compactions, and storage reclamation.
42. `RING-IMPL-REGRESSION-109` WAL lifecycle stress MUST rotate through every data region, reclaim
    WAL prefixes, reuse reclaimed regions, and reopen with live collection state intact.
43. `RING-IMPL-REGRESSION-110` Map lifecycle stress MUST preserve modeled key/value state across
    writes, deletes, compactions, committed-region reclaims, WAL rollovers, and WAL-head reclaims.
44. `RING-IMPL-REGRESSION-111` WAL-head reclaim capacity stress MUST reclaim a bounded WAL prefix
    when the full chain is longer than `MAX_PENDING_RECLAIMS`.
