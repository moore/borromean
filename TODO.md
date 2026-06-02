# TODO

This list comes from the repository maturity and completeness review.
Design history and exploratory ideas live in [journal.md](journal.md).

## High Priority

- Generalize the `FlashIo` error surface so real hardware drivers are not forced through
  `MockError` and `MockFormatError`.
- Decide and document the production-readiness target for the storage core and durable map:
  alpha, beta, or release-candidate criteria.
- Keep channel explicitly experimental until it is durably integrated, or move it behind a feature
  flag if it should not be treated as supported API.

## Medium Priority

- Resolve or retire the Duvet `todo` requirements around storage ownership, startup scratch,
  collection buffers, shared-device synchronization, named transition edges, and transaction
  recovery ordering.
- Harden collection-scoped WAL transaction recovery with broader crash-injection coverage and
  foreground I/O-error tests.
- Rewrite the forced WAL-rotation and WAL-head reclaim lifecycle stress tests so they exercise the
  transaction-era allocator model without relying on old cleanup counters.
- Refine the explicit state machine so durable transitions have named preconditions, durable
  effects, runtime effects, replay effects, and crash-cut outcomes.
- Continue reducing low-level public map APIs that require caller-provided frontier buffers in
  normal use.
- Promote exact `MAP_MANIFEST_V1_FORMAT` and `MAP_RUN_V1_FORMAT` byte layouts into normative
  `MAP-` requirements when those layouts are stable enough for format review.

## Low Priority

- Fix package metadata polish, including the `embeded` and `nostd` keyword typos in
  `Cargo.toml`.
- Clean up old exploratory comments in the map and channel modules once the current design is
  settled.
- Add a short release checklist for no-std target coverage, traceability, docs, and dependency
  policy.
