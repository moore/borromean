# TODO

This list comes from the repository maturity and completeness review.
Design history and exploratory ideas live in [journal.md](journal.md).

## High Priority

- Generalize the `FlashIo` error surface so real hardware drivers are not forced
  through `MockError` and `MockFormatError`.
- Decide and document the production-readiness target for the storage core and
  durable map: alpha, beta, or release-candidate criteria.
- Keep channel explicitly experimental until it is durably integrated, or move
  it behind a feature flag if it should not be treated as supported API.

## Medium Priority

- Resolve or retire the Duvet `todo` requirements around storage ownership,
  startup scratch, collection buffers, shared-device synchronization, named
  transition edges, and transaction recovery ordering.
- Harden collection-scoped WAL transaction recovery with broader crash-injection
  coverage and foreground I/O-error tests.
- Fix the cached-frontier compaction `InvalidChecksum` exposed by
  `cargo test --all-features`, then add the all-features suite to
  `scripts/verify.sh`.
- Rewrite the forced WAL-rotation and WAL-head reclaim lifecycle stress tests so
  they exercise the transaction-era allocator model without relying on old
  cleanup counters.
- Refine the explicit state machine so durable transitions have named
  preconditions, durable effects, runtime effects, replay effects, and crash-cut
  outcomes.
- Continue reducing low-level public map APIs that require caller-provided
  frontier buffers in normal use.
- Add an auxiliary-region pointer index to ordinary object-log regions: object
  records grow from the top down while the auxiliary index grows from the bottom
  up, allowing region-freeing logic to scan only the index to find auxiliary
  regions that must also be freed.
- Add pre-erased regions. The current durable free list stores its own links
  inside the free regions, so those regions cannot be erased ahead of
  allocation. One possible replacement is a dedicated allocator journal whose
  entries point to already-erased regions, with explicit caller-driven cleanup
  erasing detached regions when the database user has no other I/O to perform.
- Move collection frontier buffers into values passed to typed collection
  open/load commands. The opened collection handle would own those buffers while
  resident and return them through an explicit `close()` operation, making
  checkpoint, close, and cleanup I/O visible to callers instead of hidden behind
  opening or reading another collection.
- Rename `wal_write_granule` to `flash_write_size` across code, specs, and tests
  so the term describes the hardware-alignment constraint instead of the WAL
  subsystem.
- Promote exact `MAP_MANIFEST_V1_FORMAT` and `MAP_RUN_V1_FORMAT` byte layouts
  into normative `MAP-` requirements when those layouts are stable enough for
  format review.

## Low Priority

- Clean up old exploratory comments in the map and channel modules once the
  current design is settled.
- Add a short release checklist for no-std target coverage, traceability, docs,
  and dependency policy.
