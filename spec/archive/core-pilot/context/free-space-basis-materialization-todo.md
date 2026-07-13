# TODO: Reconcile Free-Space Basis Materialization

> **Archive status:** Historical design gap from the v2 implementation and
> superseded core pilot. The active free-list and progress discussions replace
> the proposed protocol below.

## Discrepancy

The normative design treats the free-space collection as log-structured state:

- the current bootstrap, snapshot, or materialized head remains the retained
  durable basis;
- a materialized free_space_v2 chain becomes authoritative only when a durable
  head record publishes it;
- stable replay-visible state must not change before that publication;
- live committed collection state must not be rewritten in place.

The implementation currently follows a different protocol:

- allocator records are appended and synced;
- materialize_free_space_collection then erases and rewrites the metadata
  regions belonging to the current live chain;
- the chain is synced only after all of its regions have been rewritten;
- no replacement free_space_v2 head is published.

A power failure or I/O failure during that rewrite can therefore invalidate the
only basis that startup must load before it can replay the allocator WAL.

Relevant specification:

- spec/ring/01-theory.md:182-195 and 280-286
- spec/ring/02-state-machines.md:275-281 and 470-485
- spec/ring/04-wal-records.md:311-322 and 468-482
- spec/ring/06-startup-replay.md:135-144 and 261-271
- spec/ring/07-reclaim.md:227-240

Relevant implementation:

- src/storage.rs:4129-4154
- src/storage.rs:4236-4292
- src/startup.rs:1576-1684
- src/startup.rs:3457-3464
- src/startup.rs:3507-3518

## Required Design Clarification

Add a normative MaterializeFreeSpaceBasis procedure that explicitly defines:

1. how fresh metadata regions are reserved without recursive allocator
   exhaustion;
2. how a complete replacement chain is encoded, written, and synced;
3. how head(collection_id = 0, collection_type = free_space_v2, new_root)
   publishes the replacement;
4. when the previous basis becomes reclaimable;
5. how new-chain reservations and old-chain reclamation recover at every crash
   cut;
6. how a WAL snapshot may be used as an alternative when materializing a chain
   cannot make forward progress.

## Implementation TODO

- Implement replay support for collection-zero free_space_v2 snapshots and
  heads.
- Replace in-place metadata rewrites with a prepared replacement basis.
- Keep the previous basis intact until the replacement head is durable.
- Reclaim the old metadata chain through ordinary ordered dirty frees only
  after publication.
- Preflight bounded RAM, metadata-region, WAL-reserve, and cleanup capacity
  before beginning publication.
- Make blocking and Future reclaim use the same materialization state machine.

## Required Tests

- One-region and multi-region replacement-chain success cases.
- Crash or injected I/O failure after every erase, write, sync, head append,
  and head sync.
- Reopen before publication selects the old basis.
- Reopen after publication selects the new basis.
- Old metadata regions never enter the free queue before publication.
- New private metadata regions are recovered or reclaimed if publication does
  not complete.
- Repeated basis rollover keeps allocator memory and durable history bounded.
