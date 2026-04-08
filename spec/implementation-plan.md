# Implementation Plan

## Goal

Bring requirement traceability for the current supported storage scope
into a test-backed state.

For the storage core, WAL, startup/recovery, allocator/reclaim,
future-based operations, and the `map` collection, this plan has two
tracks:

1. Move requirements that are currently only cited from functional code
   into dedicated tests or dedicated compile-time harnesses.
2. Add new tests until every requirement in
   [`spec/ring.md`](ring.md) and
   [`spec/implementation.md`](implementation.md) that can be
   mechanically verified is covered.

Any remaining uncovered item must be either:

1. A spec-authoring or traceability-meta requirement enforced by Duvet
   or CI policy rather than by a runtime test.
2. A requirement whose wording must be normalized so Duvet extracts the
   intended single traceability target.
3. An explicit exception with written rationale.

## Supported Scope

This plan keeps the same supported implementation scope as the
completed implementation-closeout plan:

1. Persistent storage core behavior.
2. WAL append, rotation, recovery, and WAL-head reclaim.
3. Startup/replay/recovery.
4. Allocator and free-list behavior.
5. Persistent `map` collections.
6. Future-based externally polled operations.

Still out of scope:

1. Durable `channel` integration.
2. Channel startup/open/replay support.
3. Channel WAL payload and committed-region format support.

## Current Baseline

The implementation is functionally in place for the supported scope,
but the traceability story is not yet test-first.

Current observations from the repository state on April 3, 2026:

1. `cargo test` passes.
2. `cargo clippy --all-targets --all-features -- -D warnings`
   passes.
3. `duvet report --ci true --require-citations true --require-tests true`
   still fails with `Citation missing test`.
4. The recent core/I/O and memory/buffer-API traceability slices are
   migrated and test-backed, but Duvet still reports a broad remaining
   backlog of requirements that are cited only from functional
   modules.
5. Current examples now include implementation-structure requirements
   such as `RING-IMPL-CORE-004`,
   `RING-IMPL-EXEC-001` through `RING-IMPL-EXEC-004`,
   `RING-IMPL-ARCH-004`, `RING-IMPL-ARCH-005`,
   `RING-IMPL-OP-001`, `RING-IMPL-OP-004`, `RING-IMPL-OP-005`,
   `RING-IMPL-API-005`,
   `RING-IMPL-STARTUP-001` through `RING-IMPL-STARTUP-004`,
   `RING-IMPL-COLL-001`, and `RING-IMPL-COLL-003`, plus a
   remaining ring-side migration backlog in storage, reclaim, replay,
   and allocator/core requirements.
6. Several existing tests still claim multiple requirement identifiers
   in one top-level test entry point, which conflicts with
   `RING-IMPL-TEST-002`.
7. Inline test bodies have now been migrated out of the main core
   functional modules, and `RING-IMPL-TEST-005` is now covered by a
   dedicated repository-traceability test.
8. The current `RING-IMPL-TEST-001` repository audit is still looser
   than Duvet's `--require-tests` rule because it scans all `src/`
   traces rather than only dedicated test or harness entries; it should
   be treated as migration scaffolding rather than final closeout
   evidence.
9. Some "missing citation" items are really spec-format or
   multi-`MUST` wording problems rather than missing implementation
   coverage.

## Progress

Completed so far:

1. Split the reclaim/reopen regressions in
   [`src/tests/mod.rs`](/home/moore/devel/borromean/src/tests/mod.rs)
   into dedicated test entry points for:
   `RING-WAL-RECLAIM-SAFE-001`,
   `RING-WAL-RECLAIM-POST-002`,
   `RING-WAL-RECLAIM-POST-003`,
   `RING-WAL-RECLAIM-POST-005`,
   `RING-STARTUP-007`,
   `RING-STARTUP-020`,
   `RING-STARTUP-021`,
   `RING-REGION-RECLAIM-PRE-002`,
   `RING-REGION-RECLAIM-PRE-003`,
   `RING-REGION-RECLAIM-SEM-003`, and
   `RING-REGION-RECLAIM-ORDER-005`.
2. Added metadata/storage-layout coverage in
   [`src/mock.rs`](/home/moore/devel/borromean/src/mock.rs) and
   [`src/disk.rs`](/home/moore/devel/borromean/src/disk.rs) for
   `RING-META-005` and `RING-STORAGE-010`.
3. Extended `MockFlash` with a linear-storage read helper and encoded
   metadata-region backing so metadata-region tests can observe the
   reserved bytes and the metadata/data-region boundary directly.
4. Migrated the inline test bodies out of
   [`src/disk.rs`](/home/moore/devel/borromean/src/disk.rs),
   [`src/mock.rs`](/home/moore/devel/borromean/src/mock.rs),
   [`src/wal_record.rs`](/home/moore/devel/borromean/src/wal_record.rs),
   [`src/storage.rs`](/home/moore/devel/borromean/src/storage.rs), and
   [`src/startup.rs`](/home/moore/devel/borromean/src/startup.rs) into
   dedicated `tests.rs` files. The collection modules were already on
   that pattern.
5. Added a dedicated repository-traceability test in
   [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
   so `RING-IMPL-TEST-005` is satisfied by an automated test rather
   than by convention alone.
6. Split the remaining bundled WAL codec/layout tests in
   [`src/wal_record/tests.rs`](/home/moore/devel/borromean/src/wal_record/tests.rs)
   into dedicated one-requirement tests and aligned their annotation
   text with Duvet's extracted requirement wording. This cleared the
   `wal-record-types` backlog from the Duvet incomplete set.
7. Aligned the disk-side requirement annotations to Duvet's extracted
   wording and added dedicated tests for `RING-FREE-004`,
   `RING-FORMAT-STORAGE-PRE-001`, and `RING-FORMAT-STORAGE-POST-002`.
   This cleared the `storage-metadata`, `header`,
   `wal-region-prologue`, `free-pointer-footer`, and
   `format-storage-on-disk-initialization` families from the Duvet
   incomplete set.
8. `cargo test` now passes with 133 tests after the test-module
   migration, traceability harness, WAL test split, and disk/format
   cleanup.
9. Duvet still fails, but the remaining work is now concentrated in
   meta-verification requirements and in requirements that are still
   genuinely unimplemented (`RING-CORE-012`, `RING-CORE-015`,
   `RING-CORE-016`, `RING-CORE-017`, `RING-STARTUP-027`) or are down to
   exact-quote/spec-wording cleanup in a much smaller set of remaining
   areas. The total incomplete set is now down to 25 items.
10. Migrated the remaining low-friction functional-code claims for
    `RING-DISK-001`, `RING-DISK-005`, `RING-DISK-006`,
    `RING-DISK-007`, `RING-CORE-003`, `RING-CORE-004`,
    `RING-FORMAT-013`, and `RING-IMPL-ARCH-003` into dedicated test
    modules in
    [`src/disk/tests.rs`](/home/moore/devel/borromean/src/disk/tests.rs),
    [`src/wal_record/tests.rs`](/home/moore/devel/borromean/src/wal_record/tests.rs),
    [`src/storage/tests.rs`](/home/moore/devel/borromean/src/storage/tests.rs),
    [`src/collections/map/tests.rs`](/home/moore/devel/borromean/src/collections/map/tests.rs),
    and
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs).
11. Removed the corresponding temporary production-code citations from
    [`src/disk.rs`](/home/moore/devel/borromean/src/disk.rs),
    [`src/wal_record.rs`](/home/moore/devel/borromean/src/wal_record.rs),
    [`src/lib.rs`](/home/moore/devel/borromean/src/lib.rs), and
    [`src/collections/map/mod.rs`](/home/moore/devel/borromean/src/collections/map/mod.rs)
    so those requirements are now evidenced by tests instead of
    functional-module annotations.
12. `cargo test -q` now passes with 143 tests, and
    `duvet report --ci true --require-citations true --require-tests true`
    is down to 13 incomplete items. The remaining failures are now only
    the two requirements-format fragments in each specification,
    `RING-IMPL-TEST-001` through `RING-IMPL-TEST-004`,
    `RING-CORE-012`, `RING-CORE-015`, `RING-CORE-016`,
    `RING-CORE-017`, and `RING-STARTUP-027`.
13. Split the remaining bundled traced startup and reclaim tests in
    [`src/startup/tests.rs`](/home/moore/devel/borromean/src/startup/tests.rs)
    and
    [`src/tests/mod.rs`](/home/moore/devel/borromean/src/tests/mod.rs)
    so the old multi-claim coverage is now expressed as dedicated
    one-requirement test entry points for `RING-STARTUP-005`,
    `RING-STARTUP-006`, `RING-STARTUP-011`, `RING-STARTUP-013`,
    `RING-STARTUP-018`, `RING-STARTUP-020`, `RING-STARTUP-022`,
    `RING-STARTUP-023`, `RING-WAL-ENC-010`, `RING-WAL-VALID-022`,
    `RING-REGION-RECLAIM-POST-001`, and
    `RING-REGION-RECLAIM-POST-005`.
14. Extended
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    with repository-level enforcement for `RING-IMPL-TEST-002`,
    `RING-IMPL-TEST-003`, and `RING-IMPL-TEST-004`, plus spec-format
    checks that satisfy the requirements-format entries for
    [`spec/implementation.md`](/home/moore/devel/borromean/spec/implementation.md)
    and [`spec/ring.md`](/home/moore/devel/borromean/spec/ring.md).
15. `cargo test -q` now passes with 157 tests, and
    `duvet report --ci true --require-citations true --require-tests true`
    is down to 6 incomplete items. The only remaining non-runtime
    traceability item is now `RING-IMPL-TEST-001`; the rest are the
    real behavioral gaps `RING-CORE-012`, `RING-CORE-015`,
    `RING-CORE-016`, `RING-CORE-017`, and `RING-STARTUP-027`.
16. Added dedicated retained-state validation coverage in
    [`src/collections/map/tests.rs`](/home/moore/devel/borromean/src/collections/map/tests.rs)
    and
    [`src/tests/mod.rs`](/home/moore/devel/borromean/src/tests/mod.rs)
    for invalid retained committed-region bases, invalid retained
    `snapshot` payloads, and invalid retained post-basis `update`
    payloads. Those tests now satisfy `RING-FORMAT-016` and
    `RING-STARTUP-027`, and the temporary functional-code citation for
    `RING-FORMAT-016` has been removed from
    [`src/collections/map/mod.rs`](/home/moore/devel/borromean/src/collections/map/mod.rs).
17. `cargo test -q` now passes with 159 tests, and
    `duvet report --ci true --require-citations true --require-tests true`
    is down to 5 incomplete items. The only remaining non-runtime
    traceability item is still `RING-IMPL-TEST-001`; the remaining
    runtime gaps are now only `RING-CORE-012`, `RING-CORE-015`,
    `RING-CORE-016`, and `RING-CORE-017`.
18. Added an integrated map-frontier update path in
    [`src/lib.rs`](/home/moore/devel/borromean/src/lib.rs),
    a rotation-aware WAL append helper in
    [`src/storage.rs`](/home/moore/devel/borromean/src/storage.rs),
    and true live-entry snapshot compaction in
    [`src/collections/map/mod.rs`](/home/moore/devel/borromean/src/collections/map/mod.rs)
    so overflowing a bounded mutable map frontier now flushes the
    current logical state into a committed region, retries the update
    over a compacted fresh frontier, and continues appending later
    updates over that new durable head.
19. Added dedicated tests in
    [`src/collections/map/tests.rs`](/home/moore/devel/borromean/src/collections/map/tests.rs)
    and
    [`src/tests/mod.rs`](/home/moore/devel/borromean/src/tests/mod.rs)
    for `RING-CORE-015`, `RING-CORE-016`, and `RING-CORE-017`.
20. Added runtime-only dirty-frontier tracking in
    [`src/lib.rs`](/home/moore/devel/borromean/src/lib.rs) so
    `Storage::update_map_frontier(...)` now enforces the configured
    reserve implied by `RING-CORE-012`: a clean collection may not
    become a new dirty in-memory frontier when doing so would require
    more than `min_free_regions - 1` simultaneously dirty frontiers.
    Successful `flush_map`, `snapshot_map`, and `drop_map` calls now
    clear that runtime tracker.
21. Added dedicated tests in
    [`src/tests/mod.rs`](/home/moore/devel/borromean/src/tests/mod.rs)
    and
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    for `RING-CORE-012` and `RING-IMPL-TEST-001`, plus an explicit
    dedicated reclaim trace for `RING-REGION-RECLAIM-004`.
22. `cargo test -q` now passes with 164 tests.
23. `duvet report --ci true --require-citations true --require-tests true`
    still fails. The runtime gap for `RING-CORE-012` is closed, but the
    report is still flagging a broader "citation missing test" backlog:
    many requirements remain cited only in functional modules rather
    than by dedicated tests. The remaining work is therefore still a
    wide trace-migration pass, not just a final one-or-two-requirement
    cleanup.
24. Added dedicated static traceability tests in
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    for `RING-IMPL-CORE-001`, `RING-IMPL-CORE-002`,
    `RING-IMPL-CORE-003`, `RING-IMPL-NONGOAL-001`,
    `RING-IMPL-NONGOAL-002`, `RING-IMPL-ARCH-002`,
    `RING-IMPL-API-002`, and `RING-IMPL-IO-001` through
    `RING-IMPL-IO-005`.
25. Added a generic-driver forwarding harness and a blocking-vs-future
    equivalence regression in
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    so those implementation-shape requirements are now evidenced by
    dedicated test entry points instead of by production-only
    annotations.
26. Removed the corresponding temporary production-code citations from
    [`Cargo.toml`](/home/moore/devel/borromean/Cargo.toml),
    [`src/lib.rs`](/home/moore/devel/borromean/src/lib.rs),
    [`src/flash_io.rs`](/home/moore/devel/borromean/src/flash_io.rs),
    and [`src/mock.rs`](/home/moore/devel/borromean/src/mock.rs).
27. `cargo test -q` now passes with 176 tests.
28. `duvet report --ci true --require-citations true --require-tests true`
    still fails, but the migrated core/I/O/API slice is clean. The
    remaining Duvet backlog is now concentrated in the broader
    implementation-structure families (`EXEC`, `MEM`, `ARITH`,
    `PANIC`, `OP`, `STARTUP`, `COLL`, `ARCH-004/005`, `API-004/005`,
    `CORE-004/005`) plus ring-side storage/reclaim/replay/core
    migrations that are still only cited from functional code.
29. Added dedicated static traceability tests in
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    for `RING-IMPL-CORE-005`, `RING-IMPL-MEM-001`,
    `RING-IMPL-MEM-002`, `RING-IMPL-MEM-004`,
    `RING-IMPL-MEM-005`, `RING-IMPL-COLL-002`, and
    `RING-IMPL-API-004`.
30. Those tests verify the current implementation shape directly:
    fixed-capacity runtime state, caller-provided workspaces and
    payload buffers, borrowed map storage, and public disk-format size
    constants or workspace constructor contracts.
31. Removed the corresponding temporary production-code citations from
    [`src/storage.rs`](/home/moore/devel/borromean/src/storage.rs),
    [`src/workspace.rs`](/home/moore/devel/borromean/src/workspace.rs),
    [`src/disk.rs`](/home/moore/devel/borromean/src/disk.rs),
    [`src/collections/map/mod.rs`](/home/moore/devel/borromean/src/collections/map/mod.rs),
    and [`src/lib.rs`](/home/moore/devel/borromean/src/lib.rs).
32. `cargo test -q` now passes with 183 tests.
33. `duvet report --ci true --require-citations true --require-tests true`
    still fails, but the migrated memory/API slice is now out of the
    remaining backlog. The open implementation-side migration work is
    now concentrated in `CORE-004`, `EXEC`, `ARCH-004/005`, `ARITH`,
    `PANIC`, `OP`, `STARTUP`, `COLL-001`, `COLL-003`, and `API-005`,
    plus the remaining ring-side storage/reclaim/replay/core families.
34. Added dedicated traceability tests in
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    for `RING-IMPL-ARITH-001` through `RING-IMPL-ARITH-004` and
    `RING-IMPL-PANIC-001` through `RING-IMPL-PANIC-003`.
35. Those tests now cover the checked-arithmetic surface directly,
    verify explicit failure-type mapping for checked arithmetic and
    invalid input paths, and enforce that non-test code stays free of
    forbidden panic primitives under the crate-level non-test deny
    configuration.
36. Removed the corresponding temporary production-code citations from
    [`src/lib.rs`](/home/moore/devel/borromean/src/lib.rs),
    [`src/storage.rs`](/home/moore/devel/borromean/src/storage.rs), and
    [`src/collections/map/mod.rs`](/home/moore/devel/borromean/src/collections/map/mod.rs).
37. `cargo test -q` now passes with 190 tests.
38. `duvet report --ci true --require-citations true --require-tests true`
    still fails, but the implementation-side backlog is now narrower:
    `CORE-004`, `EXEC`, `ARCH-004/005`, `OP`, `STARTUP`,
    `COLL-001`, `COLL-003`, and `API-005`, plus the remaining
    ring-side storage/reclaim/replay/core families.
39. Split the repository-level traceability tests out of the monolithic
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    file into dedicated family modules under
    [`src/tests/traceability/`](/home/moore/devel/borromean/src/tests/traceability/):
    `audit.rs`, `format.rs`, `core.rs`, `memory.rs`,
    `arithmetic.rs`, `panic.rs`, `api.rs`, `io.rs`, and `arch.rs`.
40. Reduced
    [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
    to shared helpers plus module declarations so repository-level
    static checks are still centralized but no longer live in a single
    1400-line file.
41. Verified that the traceability split is behavior-neutral:
    `cargo test -q` still passes with 190 tests, and
    `cargo clippy --all-targets --all-features -- -D warnings`
    passes cleanly after the refactor.
42. Dropped the panic-canary experiment after review because it did not
    match the intended verification goal. The final plan keeps clippy-
    and traceability-based panic-policy checks, but does not currently
    include a dedicated freestanding panic-handler fixture.

Still open from the traceability/spec-cleanup backlog:

1. Broader migration of production-only requirement traces into
   dedicated tests remains open across multiple requirement families.
   Current examples include:
   `RING-CORE-001`, `RING-CORE-002`, `RING-CORE-005` through
   `RING-CORE-011`, `RING-CORE-013`,
   `RING-STORAGE-001` through `RING-STORAGE-009`,
   `RING-WAL-ENC-002`, `RING-WAL-ENC-007`,
   `RING-WAL-VALID-026`,
   `RING-STARTUP-026`, `RING-STARTUP-028`,
   `RING-WAL-RECLAIM-PRE-001` through `RING-WAL-RECLAIM-PRE-003`,
   `RING-WAL-RECLAIM-POST-001`, `RING-WAL-RECLAIM-POST-006`,
   `RING-WAL-RECLAIM-POST-007`,
   `RING-REGION-RECLAIM-SEM-002`,
   `RING-REGION-RECLAIM-PRE-001`,
   `RING-REGION-RECLAIM-POST-002`,
   `RING-REGION-RECLAIM-POST-003`,
   `RING-REGION-RECLAIM-POST-004`,
   `RING-REGION-RECLAIM-ORDER-001` through
   `RING-REGION-RECLAIM-ORDER-004`,
   `RING-IMPL-CORE-004`,
   `RING-IMPL-EXEC-001` through `RING-IMPL-EXEC-004`,
   `RING-IMPL-ARCH-004`, `RING-IMPL-ARCH-005`,
   `RING-IMPL-OP-001`, `RING-IMPL-OP-004`, `RING-IMPL-OP-005`,
   `RING-IMPL-API-005`,
   `RING-IMPL-STARTUP-001` through `RING-IMPL-STARTUP-004`, and
   `RING-IMPL-COLL-001` and `RING-IMPL-COLL-003`.
2. The meta traceability tests are now present, but they should be
   treated as guardrails for the migration, not as evidence that the
   repository has already reached a fully test-backed end state.
3. The `RING-IMPL-TEST-001` repository audit in
   [`src/tests/traceability.rs`](/home/moore/devel/borromean/src/tests/traceability.rs)
   should be tightened before closeout so it counts only dedicated test
   or harness entries rather than any `src/` trace.

## Working Rules

These rules govern the whole plan:

1. Production-code `//#` annotations are temporary scaffolding, not the
   final form of evidence for testable behavior.
2. Once a requirement has a dedicated test or dedicated compile-time
   harness, the production-code Duvet claim for that requirement should
   be removed or converted to a plain non-Duvet comment if local design
   context is still useful.
3. Each top-level traced test function or compile-time harness entry
   must claim exactly one requirement identifier.
4. Shared helpers, fixtures, setup routines, poll helpers, and data
   generators may be reused freely, but they should remain untraced
   unless they are themselves the final dedicated harness entry.
5. Runtime tests and compile-time harnesses should live in dedicated
   test modules or files rather than inline in the functional module
   they exercise.
6. Repository-level traceability tests should also be split by
   requirement family once a single helper file starts accumulating
   unrelated static checks.
7. Prefer the narrowest harness that actually proves the requirement:
   a pure unit test for codecs, a storage regression test for replay or
   recovery rules, a property test for boundary-heavy formats, or a
   compile-time harness for crate-shape and API-shape constraints.
8. Where clippy can soundly enforce the same policy, use it as
   defense in depth, but keep a dedicated traced test or harness entry
   for Duvet-facing evidence.
9. If a requirement is not meaningfully testable, classify it
   explicitly as Duvet-policy enforced, spec-cleanup required, or an
   explicit exception. Do not leave it as an implied gap.

## Workstreams

### Phase 1: Build The Requirement Matrix

First create a complete requirement inventory for the supported scope.

1. Enumerate every Duvet-extracted requirement from
   [`spec/ring.md`](ring.md) and
   [`spec/implementation.md`](implementation.md).
2. Classify each requirement into one of these buckets:
   runtime test, compile-time test, existing-test migration,
   spec-cleanup required, or explicit exception.
3. Record the current evidence location for each requirement:
   uncited, cited only in functional code, cited by an existing test,
   or split across a multi-claim test.
4. Use that matrix as the execution backlog for the remaining phases.

### Phase 2: Migrate Existing Functional-Code Claims Into Tests

This is the first major track.

1. Audit requirement claims in functional modules such as
   `src/lib.rs`, `src/storage.rs`, `src/startup.rs`,
   `src/op_future.rs`, `src/flash_io.rs`, `src/workspace.rs`, and
   `src/collections/map/mod.rs`.
2. For each requirement that is already behaviorally implemented, write
   a dedicated test before removing the functional-code claim.
3. Migrate inline `#[cfg(test)]` modules and inline test bodies out of
   functional source files into dedicated test modules or files so the
   verification surface satisfies `RING-IMPL-TEST-005`.
   Status: completed for `disk`, `mock`, `wal_record`, `storage`, and
   `startup`; collection modules were already using dedicated test
   files.
4. Split existing bundled tests so each requirement gets its own entry
   point, especially in replay/reclaim regressions and WAL encoding
   tests.

### Phase 3: Close The Runtime-Test Gaps

This is the second major track.

Prioritize uncited and weakly covered requirements by domain.

1. Allocator and in-memory frontier rules:
   add focused tests for `RING-CORE-012`, `RING-CORE-015`,
   `RING-CORE-016`, and `RING-CORE-017`.
   Status: completed.
2. Metadata and storage-layout rules:
   add tests for `RING-META-005` and `RING-STORAGE-010`.
3. Reclaim safety and reopen behavior:
   add tests for `RING-REGION-RECLAIM-PRE-002`,
   `RING-REGION-RECLAIM-PRE-003`,
   `RING-REGION-RECLAIM-ORDER-005`,
   `RING-WAL-RECLAIM-POST-002`, and
   `RING-WAL-RECLAIM-POST-003`.
4. Startup validation:
   add tests for `RING-STARTUP-027` and any remaining unsupported or
   invalid retained-basis reopen paths.
   Status: completed for the current `map`-backed supported scope.
5. WAL layout and codec edge cases:
   split or add tests so each remaining WAL encoding/layout rule has a
   dedicated entry point.

### Phase 4: Add Compile-Time And Static Harnesses

Some requirements are mechanical but not best proven by runtime tests.

1. Add compile-time or static harnesses for crate-shape and API-shape
   requirements such as `#![no_std]`, no `alloc`, no required runtime
   or framework, and explicit workspace/I/O ownership in public entry
   points.
   Status: first slice completed for `RING-IMPL-CORE-001` through
   `RING-IMPL-CORE-003`, `RING-IMPL-NONGOAL-001`,
   `RING-IMPL-NONGOAL-002`, `RING-IMPL-ARCH-002`,
   `RING-IMPL-API-002`, and `RING-IMPL-IO-001` through
   `RING-IMPL-IO-005`.
   Second slice completed for `RING-IMPL-CORE-005`,
   `RING-IMPL-MEM-001`, `RING-IMPL-MEM-002`,
   `RING-IMPL-MEM-004`, `RING-IMPL-MEM-005`,
   `RING-IMPL-COLL-002`, and `RING-IMPL-API-004`.
   Third slice completed for `RING-IMPL-ARITH-001` through
   `RING-IMPL-ARITH-004` and `RING-IMPL-PANIC-001` through
   `RING-IMPL-PANIC-003`.
2. Keep each compile-pass, compile-fail, or static-check entry focused
   on one requirement identifier.
3. Integrate these harnesses into the normal verification flow rather
   than leaving them as one-off scripts.

### Phase 5: Normalize Spec Wording For Duvet

Some current Duvet failures are extraction problems rather than test
gaps.

1. Normalize requirement text that currently contains multiple
   normative `MUST` clauses inside one numbered item when that causes
   Duvet to extract extra uncited fragments.
2. Review the requirement-format sections in both specs, the extra
   sentence attached to `RING-WAL-LAYOUT-003`, and other duplicated
   uncited fragments such as `RING-META-005` and `RING-STORAGE-010`.
3. Ensure the wording preserves the intended semantics while producing
   one stable traceability target per requirement.

### Phase 6: Handle Meta Requirements Explicitly

Some requirements are about traceability structure itself.

1. Treat `RING-IMPL-TEST-001` through `RING-IMPL-TEST-004` as plan
   acceptance criteria and repository policy, not as ordinary storage
   behavior tests.
   Status: explicit audit tests now exist for these requirements, but
   `RING-IMPL-TEST-001` is not "done" in practice until the broader
   production-trace migration is complete and Duvet passes cleanly.
2. Treat `RING-IMPL-TEST-005` as both a repository-policy requirement
   and a concrete refactor task to move tests out of functional source
   modules.
3. Decide which meta requirements should be enforced by Duvet report
   shape, CI checks, or explicit exceptions.
4. Document every such decision so the final report does not rely on
   guesswork.

### Phase 7: Verification Closeout

The plan is not complete until the full verification surface is clean.

1. Run `cargo test`.
2. Run any compile-time or static harnesses added by this plan.
3. Run `cargo clippy --all-targets --all-features -- -D warnings`.
4. Run
   `duvet report --ci true --require-citations true --require-tests true`.
5. Fix any remaining uncited, multi-claim, or exception bookkeeping
   issues before declaring the plan complete.

## Exit Criteria

This plan is complete only when all of the following are true:

1. Every requirement in [`spec/ring.md`](ring.md) and
   [`spec/implementation.md`](implementation.md) that can be
   mechanically verified for the supported scope has at least one
   dedicated runtime test or dedicated compile-time harness entry.
2. No top-level traced test or compile harness entry claims more than
   one requirement identifier.
3. No testable requirement relies on functional code as its only Duvet
   evidence.
4. Any remaining uncovered requirement is explicitly classified as
   Duvet-policy enforced, spec-cleanup only, or a documented exception.
5. `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`,
   and
   `duvet report --ci true --require-citations true --require-tests true`
   all pass.
