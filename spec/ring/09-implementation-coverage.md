# Chapter 9: Current Implementation Coverage

This appendix records implementation-facing coverage targets for the
ring spec. The list is not the conceptual source of the design; it is a
traceability checklist for tests and helper behavior that should exist
as the v2 free-space collection allocator is implemented.

## Storage Runtime State Requirements

These requirements cover runtime state updates for formatting, opening,
appending, rotation, reclaim, transactions, and WAL/state facade
helpers.

1. `RING-IMPL-REGRESSION-063` Committed region writes MUST accept a
   payload that exactly fills committed payload capacity and persist the
   full payload bytes.
2. `RING-IMPL-REGRESSION-064` Formatting storage MUST return fresh
   runtime state with metadata, WAL head/tail, free-space cursors, and
   collection fields initialized.
3. `RING-IMPL-REGRESSION-065` WAL record visitation MUST report
   snapshot and update records after a new collection in durable WAL
   order.
4. `RING-IMPL-REGRESSION-066` Opening storage MUST return replayed
   runtime state with append offset, max sequence, collection type,
   committed basis, pending update count, and free-space cursors.
5. `RING-IMPL-REGRESSION-067` Opening storage MUST complete transaction
   cleanup for regions already present in the free-space collection and
   clear incomplete transaction state.
6. `RING-IMPL-REGRESSION-068` Opening storage MUST discard incomplete
   cleanup records for regions still reachable from live collection
   state.
7. `RING-IMPL-REGRESSION-069` Appending a new collection and update
   MUST refresh runtime collection state and pending update count.
8. `RING-IMPL-REGRESSION-070` Appending a snapshot MUST move the
   collection to WAL snapshot basis and clear prior pending updates.
9. `RING-IMPL-REGRESSION-071` Appending head and drop records MUST
   refresh runtime basis to committed region and then dropped tombstone
   while reducing tracked live collection count.
10. `RING-IMPL-REGRESSION-072` Appending WAL recovery MUST clear
    pending recovery boundary and advance append offset; appending
    allocator records MUST refresh free-space cursors.
11. `RING-IMPL-REGRESSION-133` Control-record appends MUST refresh the
    in-memory runtime state without reopening and replaying the WAL.
12. `RING-IMPL-REGRESSION-134` Completing transaction cleanup MUST
    refresh `append_tail` from `free_region(region_index,
    append_tail_after)` without reopening the store.
13. `RING-IMPL-REGRESSION-073` WAL rotation start/finish appends MUST
    reserve the next ready region with `allocate_region`, advance
    `allocation_head`, move WAL tail to the new region, and clear the
    matching storage-core private allocation reservation.
14. `RING-IMPL-REGRESSION-074` WAL rotation MUST initialize the new WAL
    region at `max_seen_sequence + 1` and update runtime
    `max_seen_sequence`.
15. `RING-IMPL-REGRESSION-075` Reopening with incomplete transaction
    allocation state MUST return transaction-owned allocated regions to
    the dirty range through ordered cleanup, write
    `transaction_finished`, and leave no abandoned private allocation
    reservation live.
16. `RING-IMPL-REGRESSION-076` Allocation cleanup MUST reject region
    indexes or cleanup slots that do not match the current transaction
    recovery state or storage-core private allocation reservation.
17. `RING-IMPL-REGRESSION-077` Normal WAL append capacity MUST exclude
    a logical reserve large enough for the rotation allocation plus
    rotation-link record.
18. `RING-IMPL-REGRESSION-078` WAL rotation start MUST be accepted only
    after normal append capacity is exhausted and while the rotation-link
    reserve remains available.
19. `RING-IMPL-REGRESSION-079` Head append room checks MUST perform WAL
    rotation when the current tail lacks room for a head record.
20. `RING-IMPL-REGRESSION-080` Transaction cleanup append room checks
    MUST reject cleanup when free-space cursor state no longer matches
    the target append position.
21. `RING-IMPL-REGRESSION-081` Encoded append reserve checks for
    `allocate_region` MUST require a ready entry and return
    `WalRotationRequired` or an equivalent capacity signal when no
    ready entry remains.
22. `RING-IMPL-REGRESSION-082` Encoded append reserve checks MUST allow
    the rotation `allocate_region` when the tail has exactly the
    rotation reserve plus encoded record length remaining.
23. `RING-IMPL-REGRESSION-083` WAL-head reclaim classification MUST
    copy only head records that still reference the retained live region
    and skip stale head records.
24. `RING-IMPL-REGRESSION-084` WAL-head reclaim classification MUST
    copy drop tombstones only for collections that remain dropped and
    skip drops for live collections.
25. `RING-IMPL-REGRESSION-085` Foreground allocation headroom checks
    MUST reject ordinary allocations that would consume the configured
    ready-region reserve.
26. `RING-IMPL-REGRESSION-086` WAL-head reclaim copying MUST stop
    cleanly when a copied tail record ends exactly at the region end.
27. `RING-IMPL-REGRESSION-087` Live-state reachability checks MUST NOT
    parse non-map collection heads as maps.
28. `RING-IMPL-REGRESSION-088` Live-state reachability checks MUST
    follow live map manifest heads to referenced run regions.
29. `RING-IMPL-REGRESSION-089` Dropping a transaction-owned region in
    memory MUST remove only the matching region and preserve other
    transaction recovery state.
30. `RING-IMPL-REGRESSION-090` WAL record visitation MUST process a
    tail record that ends exactly at the append limit and then stop.
31. `RING-IMPL-REGRESSION-091` WAL-chain membership checks MUST follow
    durable link targets to determine whether a region belongs to the
    chain.
32. `RING-IMPL-REGRESSION-092` `CollectionId` helpers MUST expose
    little-endian bytes and checked increment semantics, returning none
    on u64 overflow.
33. `RING-IMPL-REGRESSION-093` Storage facade accessors MUST reflect
    underlying runtime state and tracked collection metadata.
34. `RING-IMPL-REGRESSION-094` Storage facade raw WAL wrapper methods
    MUST update runtime collection, allocator, and transaction state.
35. `RING-IMPL-REGRESSION-095` Storage facade WAL recovery append MUST
    reject recovery records when no recovery boundary is pending.
36. `RING-IMPL-REGRESSION-096` Storage facade recovery status MUST
    report pending WAL recovery boundaries and clear them after
    appending `wal_recovery`.
37. `RING-IMPL-REGRESSION-147` Startup recovery record writing MUST
    treat the private-log region boundary as an exact valid end: a
    recovery record whose aligned encoded end equals the boundary
    advances the apply path to that boundary and reports the raw encoded
    length.
38. `RING-IMPL-REGRESSION-148` Startup WAL-gap bridging during recovery
    MUST reject invalid geometry before writing: zero
    `wal_write_granule` metadata and gap placements that overflow the
    private log tail are errors.
39. `RING-IMPL-REGRESSION-149` Startup corrupt-boundary marking MUST
    choose a sentinel byte distinct from both the configured erased byte
    and the configured WAL record magic byte.
40. `RING-IMPL-REGRESSION-150` Transaction recovery bookkeeping MUST
    scope allocator observations to the open transaction or inline
    transaction and ignore non-visible allocator records from other
    ranges.
41. `RING-IMPL-REGRESSION-151` Startup replay MUST publish committed
    transaction intervals atomically: after `commit_transaction`,
    transaction collection mutations and transaction-owned allocations
    from the committed range are imported and visible in replayed
    collection state.
42. `RING-IMPL-REGRESSION-152` Post-commit transaction cleanup recovery
    MUST preserve committed collection state, recover cleanup frees in
    retained free-intent order, append dirty free-space entries at the
    expected cleanup slots, and remain stable across reopen.
43. `RING-IMPL-REGRESSION-153` Map replacement-flush cleanup MUST make
    the replaced committed map region part of the recovered free-space
    collection after cleanup completes.
44. `RING-IMPL-REGRESSION-154` Typed map opening after storage replay
    MUST validate retained map committed-region payloads, snapshot
    payloads, and update payloads and reject any that fail map-specific
    validation.
45. `RING-IMPL-REGRESSION-104` Storage append operations MUST persist
    new collection and update records so reopening through flash
    restores the collection and pending update state.
46. `RING-IMPL-REGRESSION-105` WAL-head reclaim MUST update runtime WAL
    head and tail to a fresh continuation region.
47. `RING-IMPL-REGRESSION-106` WAL-head reclaim MUST rewrite a live
    `EmptyClean` map as a WAL snapshot basis while preserving pending
    updates.
48. `RING-IMPL-REGRESSION-107` Internal WAL rotation with a large
    pending record MUST bridge an early rotation-window gap without
    surfacing `InvalidRotationWindow` to the caller.
49. `RING-IMPL-REGRESSION-108` A long mixed map workload MUST preserve
    collection identity across writes, deletes, compactions, and
    storage reclamation.
50. `RING-IMPL-REGRESSION-109` WAL lifecycle stress MUST rotate through
    every data region, reclaim WAL prefixes, reuse reclaimed regions,
    and reopen with live collection state intact.
51. `RING-IMPL-REGRESSION-110` Map lifecycle stress MUST preserve
    modeled key/value state across writes, deletes, compactions,
    committed-region reclaims, WAL rollovers, and WAL-head reclaims.
52. `RING-IMPL-REGRESSION-111` WAL-head reclaim capacity stress MUST
    reclaim a bounded WAL prefix when the full chain is longer than the
    cleanup batch capacity.

## Free-Space Collection Coverage Targets

1. `RING-IMPL-FREE-001` Startup replay MUST recover
`allocation_head`, `ready_boundary`, and `append_tail` from
`free_space_v2` metadata and retained allocator WAL commands.
2. `RING-IMPL-FREE-002` Replay MUST reject any allocator command whose
self-checking cursor does not match the current free-space cursor.
3. `RING-IMPL-FREE-003` Replay MUST reject cursor states that violate
`allocation_head <= ready_boundary <= append_tail`.
4. `RING-IMPL-FREE-004` `free_region(region_index,
append_tail_after)` MUST append a dirty entry and must not make it
allocatable.
5. `RING-IMPL-FREE-005` If physical erase completed but
`erase_free_region_span` is not durable, startup MUST leave the entries
dirty and allow erase maintenance to repeat.
6. `RING-IMPL-FREE-006` If `erase_free_region_span` is durable,
startup MUST advance `ready_boundary` and make the span allocatable.
7. `RING-IMPL-FREE-007` If `allocate_region` is durable but the
enclosing transaction is not committed, rollback recovery MUST retain
the region as a transaction-owned allocation and return it to the dirty
range through ordered cleanup after the rollback marker is durable.
8. `RING-IMPL-FREE-008` If a transaction commit or rollback marker is
durable but cleanup is unfinished, startup MUST preserve the committed
or rolled-back transaction state, finish ordered cleanup frees, and then
write `transaction_finished`.
9. `RING-IMPL-FREE-009` Ready-region reserve pressure MUST stop
ordinary allocation while still allowing erase maintenance, recovery
terminals, WAL rotation, transaction-log growth, and allocator metadata
maintenance to make forward progress.
10. `RING-IMPL-FREE-010` Inline transactions MUST apply allocator and
collection effects atomically after `commit_inline_transaction` and
ignore body effects before commit.
11. `RING-IMPL-FREE-011` Full transactions MUST import collection
effects atomically at `commit_transaction`, while transaction-owned
allocation pops remain private until commit publishes them or rollback
cleanup frees them.
12. `RING-IMPL-FREE-012` Free-space metadata materialization MUST keep
allocator links and cursors out of freed data regions.
13. `RING-IMPL-FREE-013` Transaction-private frees MUST be recorded as
`free_intent` records before commit and MUST NOT append `free_region` or
advance `append_tail` before commit.
14. `RING-IMPL-FREE-014` Rollback recovery MUST derive its cleanup list
from transaction-owned `allocate_region` records and MUST append
`rollback_transaction` before cleanup frees.
15. `RING-IMPL-FREE-015` Transaction cleanup MUST append cleanup
`free_region` records in retained-list order and require each
`append_tail_after` to match the next cleanup slot.
16. `RING-IMPL-FREE-016` Erase maintenance MUST NOT advance
`ready_boundary` while a transaction owns cleanup.
17. `RING-IMPL-FREE-017` Rollback cleanup MUST finish with
`transaction_finished` after every transaction-owned allocation has been
returned to the dirty range.
