# Borromean Development Audit

> **Archive status:** Historical checkpoint reviewed on 2026-07-09. Paths,
> test results, and severity assessments describe that revision and are not
> current design authority.

Date: 2026-07-09

Revision reviewed: 287dac18a7d55f038ed040fb4570779afc6a7c02

Project version: 0.2.0

## Executive Assessment

Borromean is a substantial alpha-stage storage engine with a strong default test
suite, careful bounded-memory design, and unusually detailed local
specifications. The default map path, strict linting, the RISC-V no-std build,
the Linux file-backed tests, and the checked Quint model all provide useful
positive evidence.

This checkpoint is nevertheless **red**. The project should not yet be treated
as beta, release-candidate, or as having a stable version-2 media format.

The highest-risk issues are:

1. The live free-space basis is erased and rewritten in place. A power cut can
   destroy the only allocator baseline before startup can replay the intact WAL.
2. Transaction-log rotation publishes a durable link before initializing the
   linked region, and startup does not recover that crash cut.
3. Main-WAL rotation allocations can be mistaken for transaction-owned data
   allocations and freed during rollback recovery.
4. The free-space queue retains all historical entries in a fixed 4096-entry
   vector. Overflow happens after the corresponding WAL record is durable,
   potentially making the store permanently unopenable.
5. Durable collection type 0x0003 means free_space_v2 in the normative disk
   specification but means ObjectLog in the implementation.
6. A deterministic map compaction sequence produces a store for which
   Storage::open succeeds but LsmMap::open fails with InvalidChecksum.

In addition, the normative free-space layout and the implemented layout are
different formats under the same STORAGE_VERSION = 2, the Future reclaim path
omits durability work performed by the blocking path, transaction failures are
not safely resumable after a durable commit marker, and the advertised
verification command currently fails.

Recommended status:

- Continue to describe the project as alpha/prototype.
- Treat current media as disposable and reformat-only.
- Do not promise cross-version media compatibility.
- Stop adding durable formats until the allocator, transaction, and format
  identity issues below are resolved.

## Scope And Method

This review covered:

- public architecture and status documentation;
- normative ring, map, object-log, and implementation specifications;
- disk encoding, startup, WAL, allocator, transaction, reclaim, and future
  implementations;
- map, object-log, channel, backing, and common collection APIs;
- traceability machinery, repository verification, packaging, and release
  reproducibility;
- targeted runtime reproduction of the feature-gated map failure;
- local model typechecking and randomized invariant simulation.

The review distinguishes:

- **Reproduced**: observed by running the code or repository tooling.
- **Direct static evidence**: the unsafe result follows directly from the
  shown write/replay ordering, but the audit did not add a permanent
  crash-injection test.
- **Contract inconsistency**: implementation, specification, or public status
  claims disagree.

No production source was changed. This report is the only audit-created file
left in the worktree. The pre-existing untracked .alloy.tmp,
_apalache-out/, and allocator-v2-gap-closure-plan.md artifacts were not
modified.

## Verification Snapshot

| Check | Result | Notes |
| --- | --- | --- |
| scripts/verify.sh | **Failed** | All earlier stages passed; final Duvet report exited with 70 errors. |
| cargo fmt --all -- --check | Passed | No formatting drift before this report. |
| rumdl check . --respect-gitignore | Passed | Existing Markdown passed. |
| Default cargo test | Passed | 484 library tests, 31 traceability tests, 2 CLI integration tests, and 1 doctest passed. |
| cargo test --all-features | **Failed** | 520 passed and 3 failed. |
| cargo test --features file-backing | Passed | 507 tests passed. |
| RISC-V no-std build | Passed | riscv32imac-unknown-none-elf library build passed. |
| All-target, all-feature Clippy | Passed | Repository warning policy passed. |
| Strict library Clippy | Passed | Panic, unwrap, expect, todo, and related policy lints passed. |
| Python tooling tests | Passed | 21 tests passed. |
| Custom traceability audit | Passed misleadingly | Reported 531 requirement tests and 0 todo tests before Duvet found 70 errors. |
| Quint typecheck | Passed | models/transaction_free_recovery.qnt typechecked. |
| Quint randomized safety run | Passed within sample | No violation in 10,000 traces with at most 40 steps; this is not exhaustive proof. |
| cargo package --list --allow-dirty | **Hygiene failure** | Generated and local untracked artifacts are included in the package list. |

The three all-feature failures are:

1. collections::map::tests::
   requirement_object_lsm_map_compaction_reuses_cached_frontier at
   src/collections/map/tests.rs:1755, returning
   Storage(Startup(Disk(InvalidChecksum))).
2. embedded_storage::tests::
   requirement_embedded_storage_uses_configured_erased_byte at
   src/embedded_storage/tests.rs:212.
3. embedded_storage::tests::
   requirement_embedded_storage_format_writes_wal_prefix_contiguously at
   src/embedded_storage/tests.rs:324.

The embedded tests still use a 64-byte region after the minimum grew to 67
bytes. Those are stale fixtures rather than evidence that rejecting the
geometry is wrong. The map failure is a real storage/reopen defect.

## Critical Findings

### AUD-C01: Free-Space Checkpointing Is Not Crash-Safe

Classification: direct static evidence; release blocker.

Evidence:

- src/storage.rs:4113-4125 writes and syncs the allocator WAL record before
  applying it and refreshing runtime state.
- src/storage.rs:4129-4154 invokes
  materialize_free_space_collection after allocate, free, and erase records.
- src/storage.rs:4236-4292 erases each region in the currently live metadata
  chain, rewrites it in place, and performs a single sync only after the whole
  chain has been rewritten.
- src/startup.rs:1576-1684 requires the canonical free-space chain rooted at
  region 1 to decode successfully before WAL replay can construct runtime
  state.

Impact:

A cut after erase, during a partial write, between regions, or before the final
sync can invalidate the only allocator basis. The WAL may contain everything
needed to advance an older basis, but startup cannot reach WAL replay because
loading that basis fails first. A foreground I/O error has the same structural
problem. Rewriting the same metadata on every allocator record also
concentrates erase wear in the metadata chain.

Recommendation:

Use copy-on-write allocator metadata. Write and sync a complete new chain, then
publish it with a durable free_space_v2 head record. Retain the previous basis
until publication is durable, and reclaim it only afterward. Add crash cuts
after every erase, write, sync, and head publication for both one-region and
multi-region bases.

### AUD-C02: The Free-Space Queue Has A Finite Durable Lifetime

Classification: direct static evidence; release blocker.

Evidence:

- src/free_space.rs:5 fixes MAX_FREE_QUEUE_ENTRIES at 4096.
- src/free_space.rs:29-38 stores the entire queue in a heapless vector.
- src/free_space.rs:272-291 appends every free and never discards consumed
  prefix entries.
- src/storage.rs:4244-4268 sizes and materializes metadata from the total
  historical vector, not only the retained interval.
- src/storage.rs:4119-4125 writes and syncs a FreeRegion before
  apply_synced_record can return QueueOverflow.
- The normative design already provides free_space_v2 snapshot/head compaction
  in spec/ring/01-theory.md:182-195 and
  spec/ring/07-reclaim.md:231-235, but startup rejects a collection-zero
  snapshot and only accepts a WAL head for collection zero at
  src/startup.rs:3457-3464 and src/startup.rs:3507-3518.

Impact:

A long-lived store eventually exhausts allocator metadata even if it has
plenty of reusable physical regions. The worst failure occurs after the WAL
record is durable: the caller receives an error, and reboot replays the same
unrepresentable record into the same fixed capacity. The store can then become
permanently unopenable.

Recommendation:

Implement logical monotonic positions and bounded prefix compaction using the
specified free-space snapshot/head mechanism. Preflight all bounded runtime
bookkeeping before any durable append. Add a lifecycle test that performs well
over 4096 allocate/free cycles with a small physical geometry and repeated
reopens.

### AUD-C03: Transaction-Log Rotation Publishes An Uninitialized Target

Classification: direct static evidence; release blocker.

Evidence:

- src/storage.rs:2925-2990 grows a transaction log by durably recording the
  allocation, materializing the old segment seal, and then initializing the
  next segment.
- src/storage.rs:2993-3041 writes and syncs the old segment seal.
- src/storage.rs:2967-2972 initializes the target only after that sync.
- src/startup.rs:2680-2728 decodes a seal, follows next_region_index, and
  immediately validates the target transaction-log region.

Impact:

A power cut after the seal sync and before target initialization leaves a
durable link to erased bytes. Startup follows the link and fails instead of
recovering the old tail or completing initialization. This is a classic
publish-before-prepare ordering error.

Recommendation:

Prepare and sync the target region before publishing the old segment seal.
Include enough identity and sequence information to distinguish a prepared
target from stale media. Add crash tests at allocation, target erase, target
header write, target sync, seal write, and seal sync.

### AUD-C04: Rollback Recovery Can Free A Live Main-WAL Region

Classification: direct static evidence; release blocker.

Evidence:

- While a transaction is open, src/startup.rs:1325-1354 adds every replayed
  main-WAL AllocateRegion record to transaction_allocations.
- Transaction terminal-room preparation may rotate the main WAL before a
  commit marker; see src/storage.rs:3214 and src/storage.rs:3961 onward.
- src/startup.rs:1745-1774 rolls an unfinished transaction back and frees
  collected transaction allocations.
- src/startup.rs:1790 onward does not filter current main-WAL chain regions from
  those frees.

Impact:

If a transaction forces WAL rotation and power fails before commit, recovery
can append the current WAL continuation region to the free queue. A later erase
or allocation can overwrite live WAL state.

Recommendation:

Record allocation ownership explicitly. Privileged WAL/free-space metadata
allocations must never enter the transaction-owned data-allocation set. During
recovery, also reject freeing any region reachable from the current WAL,
free-space metadata, or retained collection basis.

### AUD-C05: ObjectLog Collides With A Reserved Durable Type Code

Classification: contract inconsistency confirmed in source; release blocker.

Evidence:

- spec/ring/05-disk-format.md:147-154 reserves global collection_type
  0x0003 for free_space_v2.
- src/lib.rs:295-313 defines OBJECT_LOG_CODE as 3.
- src/collections/object_log.rs:378-405 persists that value in NewCollection.
- src/storage.rs:612-627, src/lib.rs:1493-1506, and
  src/startup.rs:1925-1941 accept it as an ordinary object-log type.
- src/wal_record/tests.rs:273-302 quotes an older namespace and never asserts
  the ObjectLog assignment.

Impact:

Current object-log media conflicts with the normative global namespace. A
spec-conforming implementation can reject or misinterpret it. Keeping
STORAGE_VERSION = 2 while changing this assignment makes existing version-2
media ambiguous.

Recommendation:

Assign ObjectLog a documented, unused core code. Decide whether alpha media
will be explicitly rejected or migrated, and bump the storage format version
for incompatible media. Add golden cross-module tests that assert every
collection type and region format from one authoritative registry.

### AUD-C06: Map Compaction Can Leave The Typed Collection Unopenable

Classification: reproduced deterministically; release blocker.

Evidence:

- cargo test --all-features consistently fails
  requirement_object_lsm_map_compaction_reuses_cached_frontier at
  src/collections/map/tests.rs:1700-1755.
- The audit reproduced the same sequence through public APIs without relying
  on performance counters. compact_and_report returned success.
- After compaction, Storage::open succeeded and reconstructed the same WAL
  head and tail, but LsmMap::open returned
  Storage(Startup(Disk(InvalidChecksum))).
- The final manifest and both referenced run regions decoded successfully.
  The invalid region was an erased/retired transaction-log region.
- src/storage.rs:6014-6056 expands every encountered CommitTransaction by
  unconditionally rereading its transaction-log range. It does not first
  account for a later TransactionFinished marker or retired-log state.

Impact:

A successful, ordinary map operation sequence can leave durable user data
unavailable after reopen. The low-level store looks open, which can defer the
failure until the application opens the typed collection.

Recommendation:

Make WAL visitation retention-aware. Finished transaction bodies must either
be copied/inlined into the retained WAL basis or omitted once their effects are
represented elsewhere, and visitors must not reread retired transaction-log
regions. Preserve this exact 12-region sequence as a default-feature
regression and assert both Storage::open and LsmMap::open.

## High-Severity Findings

### AUD-H01: Normative And Implemented Free-Space Formats Are Different

Classification: contract inconsistency.

Evidence:

- spec/ring/05-disk-format.md:137-154 defines free_queue_position as one u64.
- spec/ring/05-disk-format.md:293-316 defines a logical monotonic queue_index
  that does not name physical storage.
- src/disk.rs:614-665 encodes region_index:u32 plus entry_index:u32 and
  validates the region as a physical index.
- spec/ring/05-disk-format.md:298-307 requires first_queue_position in every
  FreeSpaceRegionPrologue.
- src/disk.rs:700-785 omits that field and encodes a different prologue.
- spec/ring/05-disk-format.md:325-372 requires a materialized basis to cover
  exactly the retained interval [allocation_head, append_tail).
- src/storage.rs:4236-4286 serializes the full historical vector from index
  zero.
- src/disk.rs:8-15 still describes STORAGE_VERSION = 2 and its format IDs as
  stable.

Impact:

Specification-based tools, golden vectors, independent readers, and current
code do not agree on the bytes or cursor semantics of the allocator. Tests can
pass against the implementation while proving a different contract than the
normative text.

Recommendation:

Choose one design, update all encoders, WAL fields, transaction entries,
startup, docs, and tests together, and assign a new storage version. Do not
edit the meaning of already-written version-2 media in place.

### AUD-H02: Blocking And Future WAL Reclaim Have Different Durability

Classification: direct static evidence.

Evidence:

- Blocking reclaim ensures free-space metadata capacity and materializes the
  allocator before planning at src/storage.rs:2048-2054.
- It also retires completed transaction logs before reopening at
  src/storage.rs:2096-2101.
- ReclaimWalHeadFuture begins directly with
  prepare_wal_head_reclaim at src/op_future.rs:459-476.
- Its completion path at src/op_future.rs:625-653 frees source regions and
  reopens without either blocking-path step.

Impact:

The two advertised execution styles do not implement the same transition.
The Future path can discard the only retained allocator history for
transaction-owned pops during WAL-head replacement and can retain stale
transaction-log state. This also makes correctness tests for the blocking path
insufficient evidence for the Future path.

Recommendation:

Implement one shared phase engine and make blocking execution drive it to
completion. Each phase should define its precondition, durable effects,
runtime effects, and crash outcome. Add state-equivalence and crash-cut tests
for both entry points.

### AUD-H03: Futures Can Stall A Conventional Executor

Classification: reproduced by inspection of the Future contract.

Evidence:

- YieldingFlushMapFuture, ReclaimWalHeadFuture, and OpenStorageFuture return
  Poll::Pending at internal phase boundaries in src/op_future.rs:248-275,
  src/op_future.rs:454-675, and src/op_future.rs:713-828.
- Their poll methods ignore the Context and neither register nor invoke the
  waker.
- Tests repeatedly busy-poll with a no-op waker at
  src/tests/mod.rs:231-285.
- FlashIo itself is synchronous at src/flash_io.rs:70-110, and several future
  phases perform multiple complete synchronous I/O operations per poll.

Impact:

A standards-based executor can poll once, observe Pending, and wait forever.
The surface also cannot actually suspend on DMA- or interrupt-backed I/O, so
the specification's runtime-agnostic asynchronous claims overstate the
implementation.

Recommendation:

Either call wake_by_ref for deliberate cooperative yields and document that
device I/O remains blocking, or redesign the I/O boundary around real
poll/future operations. Add an executor test whose progress depends on wakeups,
not manual repolling.

### AUD-H04: Transaction Commit Errors Are Not Safely Resumable

Classification: direct static evidence.

Evidence:

- src/lib.rs:1628-1642 writes the commit marker and then the finish marker,
  clearing TransactionMemory only if both succeed.
- The public TransactionWriter::commit consumes the writer at
  src/lib.rs:525-540 even when the call returns an error.
- Cleanup frees and transaction finish are fallible after the durable commit.
- Runtime rollback does not have an explicit guard that rejects an already
  durable committed outcome.
- ObjectLog repeats the pattern at
  src/collections/object_log.rs:328-351, with in-memory staged state changed
  between commit and finish.

Impact:

After a durable commit but failed cleanup/finish, retrying commit can repeat
the wrong phase and rollback can restore old RAM state even though reboot will
import the commit. The caller cannot tell whether the operation is safely
retryable, committed with cleanup pending, or rolled back.

Recommendation:

Represent explicit Prepared, CommittedCleanupPending, and Finished runtime
states. Reserve all bounded bookkeeping before the commit marker. Make retry
resume cleanup by durable cursor, reject rollback after commit, and return an
error type that communicates durable outcome.

### AUD-H05: Object-Log Failure Cleanup Can Be Hidden Or Abandoned

Classification: direct static evidence.

Evidence:

- On explicit transaction append failure,
  src/collections/object_log.rs:303-305 discards rollback_open's result.
- rollback_open stores a possible error but unconditionally restores tracking,
  clears memory, and closes the writer at
  src/collections/object_log.rs:309-325.
- The separate automatic append path correctly prefers cleanup errors at
  src/collections/object_log.rs:731-735.
- Truncation begins a transaction and uses immediate error propagation through
  update, commit, free, and finish at
  src/collections/object_log.rs:1746-1794, with no pre-commit rollback or
  post-commit resume path.

Impact:

A rollback I/O error can be concealed while the durable transaction remains
unfinished, and the bookkeeping needed for retry is erased. Truncation can
leave either an inaccessible open transaction or committed cleanup pending.

Recommendation:

Use one transaction guard/state machine for automatic append, explicit append,
flush, and truncation. Preserve cleanup state on error, surface cleanup failure
to the caller, and make Drop behavior and recovery explicit.

### AUD-H06: Object-Log Flush Can Permanently Skip A Failed Snapshot

Classification: direct static evidence.

Evidence:

- ObjectLogMemory defaults to 16 regions and 64 metadata bytes at
  src/collections/object_log.rs:200-204.
- Snapshot encoding uses 16 header bytes plus 35 bytes per region plus
  metadata at src/collections/object_log.rs:3537-3605.
- A 512-byte region therefore cannot encode all 16 default region entries:
  the minimum is 576 bytes without metadata and 640 with the default maximum.
- src/collections/object_log.rs:1898-1952 writes the committed data region and
  sets region.flushed = true before snapshot encoding and WAL append.
- A later call returns early when flushed is true at
  src/collections/object_log.rs:1917-1918.

Impact:

Capacity or I/O failure after the flag mutation can make a later flush report
success without ever publishing the corresponding snapshot. The public
generic defaults also permit an in-memory state that cannot be represented by
the provided scratch geometry.

Recommendation:

Validate representability at construction and before allocation. Encode the
prospective snapshot before the committed write, and mutate the in-memory
flushed flag only after the snapshot is durable. Add exact-boundary and
retry-after-each-I/O-failure tests.

### AUD-H07: Object-Log Creation Can Leave An Orphan Live Collection

Classification: direct static evidence.

Evidence:

- src/collections/object_log.rs:378-405 appends NewCollection before encoding
  and appending required log metadata.
- Encoding, rotation, sync, or validation can fail after the live collection
  record is durable.
- There is no typed object-log drop operation that repairs this partial
  creation.

Impact:

Reopen can discover a live object-log collection with missing initialization
state and reject it. A caller-visible creation error can therefore leave
persistent damage.

Recommendation:

Pre-encode and validate all initialization data, then publish creation and
metadata atomically in a transaction. Provide a supported drop/reclaim path.

### AUD-H08: A Pending Recovery Boundary Does Not Guard Appends

Classification: direct static evidence.

Evidence:

- Startup can finish with pending_wal_recovery_boundary set after a torn tail;
  later replay requires WalRecovery to be the next valid record. See
  src/startup.rs:955-1010 and src/storage.rs:5983-6006.
- src/storage.rs:4346-4370 writes ordinary records without checking that flag.
- src/storage.rs:3518-3534 can write an AllocateRegion during rotation and
  clear the pending flag without first appending WalRecovery.

Impact:

A normal update after opening a torn tail can make the next open fail with
UnexpectedRecordAfterCorruption.

Recommendation:

Reject every non-recovery record while the boundary is pending, or
automatically append and sync WalRecovery before returning the opened store to
the caller.

### AUD-H09: Malformed Transaction Positions Can Panic Startup

Classification: direct static evidence.

Evidence:

- Startup converts on-disk LogPosition offsets but does not consistently
  validate them against the region size, record-area start, or granule.
- src/startup.rs:2687-2724 can slice region_bytes[offset..] when start.offset
  is greater than the buffer length.
- src/startup.rs:2515-2582 has the equivalent risk while replaying a committed
  multi-segment range.

Impact:

A checksum-valid malformed WAL record can panic rather than return
StartupError, contradicting the crate's corrupt-input and panic-free policy.

Recommendation:

Centralize validated LogPosition decoding. Check region bounds, lower record
area bound, upper bound, ordering, and alignment before every indexed access.
Add property tests over arbitrary checksum-valid positions.

### AUD-H10: Public Committed-Region Writes Can Erase Live Storage

Classification: direct static evidence.

Evidence:

- src/storage.rs:828-880 accepts any region index and erases it immediately.
- src/lib.rs:1890-1910 exposes that helper directly on Storage.
- No reservation token, collection ownership, free/live check, or rejection
  of region zero is performed.

Impact:

A caller can erase the WAL by passing region zero. A caller can also write a
head into an unallocated ready region while that region remains in the
allocator, allowing later overwrite of live collection data.

Recommendation:

Require an affine reservation token returned by allocation. Validate owner,
purpose, and generation at commit, and reject WAL, allocator metadata, free,
or already-live regions.

### AUD-H11: FlashIo Is Extensible In Type But Closed In Error Semantics

Classification: API/contract inconsistency.

Evidence:

- README.md:27-29 tells real targets to implement FlashIo directly.
- src/flash_io.rs:4-68 defines closed StorageIoError and
  StorageFormatError enums containing only repository-owned backends.
- src/flash_io.rs:70-110 hard-codes those enums into every trait method.
- TODO.md:8-9 already acknowledges the issue.

Impact:

A downstream hardware driver cannot preserve its native error or context
without masquerading as a built-in backend or collapsing useful information.
The advertised primary integration path is therefore incomplete.

Recommendation:

Use associated I/O and format error types, with top-level generic conversion,
or provide a deliberate opaque caller-error adapter with stable
classification.

### AUD-H12: Durable Collection Byte Contracts Are Incomplete

Classification: specification gap.

Evidence:

- spec/ring/03-collection-lifecycle.md:252-265 requires exact committed-region,
  snapshot, and update encodings for every durable collection.
- spec/map.md:549-578 defers exact manifest/run ordering and scalar widths,
  while src/collections/map/mod.rs:327-332 labels live formats stable.
- spec/object-log.md:47-52 provides no numeric collection-type or region-format
  assignments.
- Object-log update and snapshot bytes exist only in private code at
  src/collections/object_log.rs:3393-3664.
- TODO.md:42-43 still requests V1 map formats although implementation names
  the live manifest and run formats V2.

Impact:

Independent readers cannot reproduce the media contract, compatibility review
has no authoritative byte schema, and implementation changes can silently
invalidate stored data.

Recommendation:

Specify exact V2 field order, width, endian, checksum, reserved-byte,
validation, type, and format identifiers. Add checked-in golden vectors
decoded by both implementation and independent test code.

### AUD-H13: Traceability Is Red And Requirement IDs Are Ambiguous

Classification: reproduced tooling failure and specification defect.

Evidence:

- The final Duvet command in scripts/verify.sh:85-86 exits with 70 stale quote
  and missing-anchor errors across disk, free-space, WAL, startup, reclaim,
  and state-machine tests.
- The custom checker reports 531 valid tests first because
  src/bin/traceability_audit.rs:343-366 checks only that an ID exists
  somewhere, not that the cited anchor and exact text match.
- RING-CORE-016 is defined twice for different requirements at
  spec/ring/01-theory.md:342-349.
- RING-IMPL-REGRESSION-107 through 111 and 135 through 136 are reused for
  unrelated requirements across spec/implementation.md, spec/map.md, and
  spec/ring/09-implementation-coverage.md.
- src/bin/traceability_audit.rs:417-430 loads IDs into a HashSet, erasing
  duplicate-definition evidence.

Impact:

A green custom precheck does not mean tests match the current specification,
and one requirement ID no longer identifies one behavior. Coverage can be
misattributed or concealed.

Recommendation:

Make configured-spec requirement IDs globally unique, repair all Duvet
citations, and make the custom checker validate document, anchor, exact quote,
and uniqueness. Duvet must be a blocking CI signal.

### AUD-H14: Formatting Publishes Metadata Before The Store Is Ready

Classification: contract inconsistency with crash-safety implications.

Evidence:

- Mock writes metadata before erasing data regions at src/mock.rs:352-378.
- Embedded writes and syncs metadata before erasing regions at
  src/embedded_storage.rs:405-426.
- File backing writes metadata before erasing regions at
  src/file_backing.rs:702-730.
- All three initialize the WAL prefix before the free-space metadata chain.
- The normative formatting order requires erase-all, metadata, free-space
  basis, and then WAL publication.

Impact:

An interrupted reformat can expose valid new metadata over stale or partially
initialized regions. Publishing WAL before its required allocator basis also
creates an open-visible intermediate state.

Recommendation:

Define an explicit format-in-progress marker or publish metadata last. Test
every interruption point on both erased media and media containing a previous
valid store.

### AUD-H15: Multi-Segment Allocator Replay Uses The Wrong Ordering

Classification: direct static evidence.

Evidence:

- src/startup.rs:3407-3412 considers one FreeQueuePosition at or before another
  only when both name the same metadata region.
- The implemented position is a physical region/entry pair, and valid logical
  order spans multiple metadata regions.
- src/startup.rs:3498-3505 uses that comparison to skip stale AllocateRegion
  records.

Impact:

After a cursor crosses a metadata-segment boundary, an older allocator record
from the preceding segment is not recognized as stale and is applied against
the current head. Startup can fail on otherwise replayable history.

Recommendation:

Convert positions to checked logical indexes using the retained metadata
chain, or adopt the normative u64 logical position. Add replay tests with the
basis in segment two and stale records from segment one.

## Medium-Severity Findings

### AUD-M01: Feature Tests Are Compiled But Not Executed By Verification

scripts/verify.sh:39-40 runs only default cargo test. All-feature Clippy
compiles feature code but does not execute its tests. This hides the current
map corruption failure and two embedded fixture failures. Add cargo test
--all-features or an explicit non-overlapping feature matrix.

There is also no tracked CI configuration, so even the failing Duvet command
is not automatically enforced on main. The 21 passing Python tooling tests are
not part of scripts/verify.sh.

### AUD-M02: Object-Log Transaction API Contradicts Its Specification

spec/object-log.md:429-438 promises a closure-scoped transaction that avoids
an object callers must remember to close. The implementation returns
ObjectLogTransactionWriter at src/collections/object_log.rs:575-605 and
requires explicit commit or rollback. Dropping the underlying writer
intentionally does not roll back at src/lib.rs:491-501. Either implement the
scoped API or specify a safe explicit cancellation and recovery contract.

### AUD-M03: Object-Log Snapshot Decoding Is Not Canonical

The encoder writes a zero reserved u16 at
src/collections/object_log.rs:3587-3590, but the decoder accepts and ignores
any value at src/collections/object_log.rs:3620-3625. This contradicts the
canonical encoding requirement in spec/object-log.md:260-265 and makes future
field use ambiguous. Reject nonzero reserved values and add a corruption test.

### AUD-M04: Unknown-Collection Updates Are Silently Ignored

src/startup.rs:3442-3448 returns success for an Update whose collection is not
known. Runtime behavior and the replay contract treat this as malformed state.
Silently dropping a durable update hides corruption and loses data. Return an
UnknownCollection error unless a narrowly specified reclaim rule proves the
record irrelevant.

### AUD-M05: VecLikeSlice::get Ignores Logical Length

src/vec_like.rs:49-51 calls get on the full backing array rather than the
active slice. A new or cleared vector therefore returns Some for inactive
slots. Existing tests do not cover index >= len. Implement
self.as_slice().get(index) and add empty, cleared, and capacity-boundary tests.

### AUD-M06: Experimental Channel Mutations Are Not Error-Atomic

src/collections/channel/mod.rs:343-347 consumes the next sequence before
apply_command can fail. Lines 370-377 mutate the author's sequence before a
pending-buffer push can return PendingLimitReached, and lines 399-416 update
last_sequence before the update tracker can return NeedsCheckpoint. The API
can report failure after partial mutation.

In addition, ChannelSequence, MemberId, and MessageId have private fields and
no public constructors at src/collections/channel/mod.rs:31-45, even though
public constructors and commands require them. Keep the module gated as
experimental or make the types constructible and operations transactional.

### AUD-M07: Public Collection Abstractions Are Internally Inconsistent

- The public Collection trait at src/lib.rs:317-323 is implemented by
  ObjectLog but not by LsmMap or Channel.
- LsmValue is documented as a validation extension point at
  src/collections/map/mod.rs:265-269, but its blanket implementation covers
  every eligible type and prevents downstream custom implementations.
- ObjectLogHandle is advertised as stable/opaque, but its checked 16-byte
  codec is private at src/collections/object_log.rs:4502-4526, so callers
  cannot safely persist and reconstruct handles.

Decide which abstractions are truly public extension points and make the
implementations, visibility, and documentation agree.

### AUD-M08: Named Transition Edges Are Not Connected To Execution

StateMachineOperation and DurableTransitionEdge are hard-coded descriptive
tables in src/mode.rs:121-182 and src/mode.rs:382-690. Production storage paths
do not consume them; traceability tests only inspect the prose table. The table
can therefore drift from actual write/sync ordering while tests remain green.
Generate the descriptions from executable phase plans or make typed edges part
of operation execution.

### AUD-M09: Status, Support Tiers, And Roadmap Are Stale

- README.md:17-23 says map is implemented and log-style collections are
  planned/experimental, while src/collections.rs:9-11 publicly exports a
  durable ObjectLog with extensive implementation and tests.
- docs/architecture-and-api.md:17-30 omits ObjectLog from Tier 1 without
  assigning it another clear tier.
- docs/implementing-a-collection.md still calls map the only complete example.
- TODO.md:31-35 describes free-list links stored in free regions, contradicting
  the current free_space_v2 metadata collection.
- The untracked allocator-v2-gap-closure-plan.md marks verification tasks
  complete even though the current verification command fails.

Choose an explicit support tier for ObjectLog, Channel, advanced runtime APIs,
and media compatibility. Update status documents from the implementation, not
from older design stages.

### AUD-M10: Advanced Public APIs Defeat The Documented Ownership Boundary

The normative API requires media-touching operations to go through Storage,
but low-level runtime and workspace types are publicly re-exported and
StorageRuntime methods accept separate flash/workspace arguments. MapFrontier
also has public low-level access paths. Documentation calls these advanced,
but Rust visibility still makes them compatibility commitments and invariant
bypass paths. Make them crate-private, seal/feature-gate them, or specify the
exception.

### AUD-M11: File-Backing Flush Overreach Is Always Reported As Zero

src/file_backing.rs:873-885 sets requested_mmap_flush_bytes equal to
aligned_dirty_bytes and then subtracts aligned_dirty_bytes from the same
value. flush_overreach_bytes is therefore always zero. It should compare the
page-aligned flush range with the unaligned dirty range. Also note that
mark_dirty_range coalesces disjoint writes into one bounding interval at
src/file_backing.rs:927-934, so its byte count includes clean gaps.

### AUD-M12: Release And Package Reproducibility Are Weak

- Cargo.toml has no rust-version, and no rust-toolchain file is tracked.
- The audit ran on rustc 1.94.0-nightly from 2025-12-30, so the actual compiler
  contract is not reproducible.
- No CI workflow is tracked.
- Cargo.lock is ignored.
- External verification and model-checking tool versions are not pinned.
- .gitignore does not cover .alloy.tmp or _apalache-out/.
- cargo package --list --allow-dirty includes all 19 Apalache output files
  (about 1.6 MiB), .alloy.tmp, and the local allocator plan.

Pin the compiler and verification tools, define the library MSRV, add CI, and
use a Cargo package include whitelist. A full package build was not completed
during this audit because restricted network access prevented registry
resolution.

### AUD-M13: The Recovery Model Is Outside The Verification Lane

models/transaction_free_recovery.qnt typechecks and its safety invariant
survived the audit's 10,000 randomized traces, which is positive evidence.
However, scripts/verify.sh and tasks.sh do not run Quint or Apalache, tool
versions are not pinned, and the model's fixed two-slot abstraction is not
mapped to the implementation's fixed one-slot runtime. Add a reproducible,
bounded formal check and document the refinement boundary. Pre-existing
_apalache-out logs alone are not proof of a completed invariant check.

## Design Consistency Matrix

| Area | Normative claim | Current implementation | Assessment |
| --- | --- | --- | --- |
| Collection type 0x0003 | free_space_v2 | ObjectLog | Direct collision |
| FreeQueuePosition | Logical monotonic u64 | Physical region/entry u32 pair | Different format and ordering |
| FreeSpaceRegionPrologue | Includes first_queue_position | Field absent | Different bytes |
| Materialized free-space basis | Exactly retained cursor interval | Entire historical vector | Different state model |
| Allocator compaction | Snapshot or head for collection zero | Snapshot rejected; only WAL head accepted | Missing design mechanism |
| Storage format version | Stable version 2 | Mutually incompatible version-2 contracts | Unsafe compatibility claim |
| Blocking/Future operations | Same state transition | Reclaim phases differ | Behavioral divergence |
| Runtime-agnostic Future | Suspendable and caller-executor friendly | Synchronous I/O and unwoken Pending | Contract not met |
| Durable map/object-log bytes | Exact collection-owned schema | Important layouts live only in code | Incomplete specification |
| State-machine edges | Named executable durability transitions | Descriptive table used by tests only | Drift risk |
| Supported collections | Map is current supported collection | ObjectLog is public and durable but untiered | Status ambiguity |

## Recommended Remediation Order

### P0: Restore Crash And Media Safety

1. Freeze the current media contract and decide that incompatible fixes will
   use a new storage version.
2. Replace in-place allocator checkpoint rewrites with copy-on-write
   publication.
3. Implement bounded allocator prefix compaction and preflight capacity before
   durable appends.
4. Correct transaction-segment rotation ordering.
5. Separate privileged WAL/metadata allocation ownership from transaction data
   ownership.
6. Fix retained transaction-log visitation so map compaction reopens.
7. Add deterministic fault injection for every durable edge in these paths.

Exit criterion: repeated lifecycle and crash-cut tests cannot make a formatted
store unopenable or place a live infrastructure region in the free queue.

### P1: Make One Authoritative Format And State Machine

1. Resolve the ObjectLog/free_space_v2 type collision.
2. Reconcile logical free-space positions, prologue bytes, WAL fields, and
   snapshot/head semantics.
3. Publish exact map and object-log byte layouts with golden vectors.
4. Unify blocking and Future reclaim through one phase engine.
5. Make commit/cleanup outcomes explicitly resumable.
6. Harden all on-disk positions before slicing or arithmetic.

Exit criterion: one versioned specification, encoder, decoder, replay path, and
crash model agree on every persisted field and transition.

### P2: Recover A Trustworthy Development Gate

1. Repair all 70 Duvet errors and renumber duplicate requirements.
2. Execute all-feature or per-feature tests in verification.
3. Add Python tooling and model checks.
4. Add CI that blocks on the complete lane.
5. Pin Rust and external verification tools.
6. Exclude generated artifacts from packages.

Exit criterion: a clean checkout can reproduce a green, version-pinned gate
without local artifacts or manual steps.

### P3: Clarify And Harden Public APIs

1. Generalize FlashIo errors.
2. Decide whether the asynchronous surface is truly asynchronous or a manual
   step interface.
3. Require ownership-bearing reservation tokens for committed-region writes.
4. Make ObjectLog transactions and failure cleanup safe and retryable.
5. Repair or gate Channel, advanced runtime, Collection, LsmValue, and handle
   persistence APIs.
6. Update README, architecture, contributor guidance, TODO, and collection
   support tiers.

## Positive Evidence

The following parts are in comparatively good condition:

- The project is candidly labeled alpha in README.md.
- The default suite is broad and fast, with strong coverage of WAL, replay,
  map lifecycle, transaction records, disk checksums, and corruption errors.
- Strict Clippy policy, no-std compilation, and bounded caller-owned memory are
  actively enforced.
- The Linux file-backed implementation passes its feature test suite.
- The repository contains unusually detailed normative material and hundreds
  of traceability annotations, even though they currently need reconciliation.
- The Quint model typechecks and did not violate its safety invariant in the
  audit sample.
- The existing lifecycle and crash-oriented tests provide a good foundation
  for the missing edge-specific regressions.

These strengths justify continuing the design, but they do not compensate for
the crash-consistency and format-identity blockers.

## Audit Limitations

- The direct code-path findings were not all converted into permanent
  fault-injection tests during this review.
- No physical NOR device, controller, DMA driver, power-fail rig, or endurance
  test was available.
- The Quint run was randomized simulation, not exhaustive proof.
- Existing Apalache artifacts were not treated as proof of a successful model
  check.
- Network restrictions prevented a full registry-backed package build and a
  fresh dependency vulnerability/license audit.
- Performance and wear were reviewed structurally, not benchmarked across
  representative hardware.

## Checkpoint Conclusion

Borromean has moved beyond a sketch: its core, map, object log, tests, specs,
and tooling form a serious storage-engine prototype. The checkpoint also shows
that several recently introduced allocator and transaction concepts have not
yet converged across specification, implementation, recovery, and
verification.

The next milestone should be **crash-safe, format-consistent alpha**, not more
surface area. Once P0 and P1 are complete and the full gate is green, the
project will have a credible basis for defining beta criteria.
