# Implementation Strategy

## Purpose

This specification defines the Rust implementation strategy for the
low-level storage design in [spec/ring.md](ring.md).
[spec/ring.md](ring.md) remains the source of truth for storage
semantics, crash ordering, and on-disk format. This document defines
how those rules are to be realized in a `no_std`, `no_alloc`,
runtime-agnostic Rust implementation.

Repository traceability policy and specification-format rules live in
[spec/implementation-policy.md](implementation-policy.md).

## Design Goals

The implementation must fit small embedded systems and still express
multi-step storage operations clearly. The current prototype already
shows a few useful directions: a thin `Storage` facade, explicit WAL
and collection layers, and caller-provided byte buffers for
serialization and decode scratch. Those ideas should be kept.

At the same time, the prototype also exposes a few constraints that do
not fit the target design. In particular, binding the backend into the
`Storage` value for its full lifetime makes async borrowing awkward,
and treating I/O as direct synchronous calls mixes durable state
transitions with device scheduling. The new implementation should keep
the storage state machine pure at the architectural level and treat
device progress as an externally driven concern.

### Core Requirements

1. `RING-IMPL-CORE-001` The core library crate MUST compile with
`#![no_std]`.
2. `RING-IMPL-CORE-002` The core library crate MUST NOT depend on the
Rust `alloc` crate.
3. `RING-IMPL-CORE-003` The core library crate MUST NOT depend on an
async runtime, executor, scheduler, or timer facility.
4. `RING-IMPL-CORE-004` The implementation MUST preserve the durable
behavior defined by [spec/ring.md](ring.md); this specification MAY
constrain implementation structure but MUST NOT weaken ring-level
correctness requirements.
5. `RING-IMPL-CORE-005` All memory required for normal operation MUST
come from caller-owned values, fixed-capacity fields, or stack frames
whose size is statically bounded by type parameters or API contracts.

## Execution Model

The reason to use Rust `async` here is not to require an async runtime.
It is to express suspendable coroutines for long-running storage
procedures while keeping control of I/O outside borromean core.
Formatting, opening, replay, WAL append, snapshot, reclaim, region
flush, and collection operations all naturally decompose into a short
sequence of state transitions separated by device actions such as
read, write, erase, and sync.

The public API should therefore expose operations as futures created by
Rust `async` functions or equivalent handwritten `Future`
implementations. Each such future should behave like a single
coroutine that advances one storage operation from start to finish.
The caller is responsible for polling that future and for supplying an
I/O object whose own completion model integrates with the platform.

### Execution Requirements

1. `RING-IMPL-EXEC-001` Every fallible storage operation that may
require one or more device interactions MUST be expressible as a
single future.
2. `RING-IMPL-EXEC-002` Borromean futures MUST make progress only when
polled by the caller and when the caller-provided I/O object becomes
ready; they MUST NOT rely on background tasks internal to borromean.
3. `RING-IMPL-EXEC-003` A simple single-threaded poll-to-completion
executor MUST be sufficient to drive any borromean operation future to
completion.
4. `RING-IMPL-EXEC-004` Borromean operations on a given `Storage`
instance MUST require exclusive mutable access to that instance unless
and until a separate concurrency specification defines stronger
sharing rules.
5. `RING-IMPL-EXEC-005` Await boundaries inside borromean operations
MUST align only with externally visible I/O steps or with pure
in-memory decision points that preserve the ring ordering rules.

## Separation of Responsibilities

The implementation should separate three concerns:

1. Durable-state logic: deciding what record, region write, reclaim
step, or recovery step must happen next according to
[spec/ring.md](ring.md).
2. Encoding and decoding: translating between in-memory structures and
the exact bytes defined by the storage spec.
3. I/O execution: actually reading, writing, erasing, and syncing the
backing device.

The current prototype already has the start of this split in `Storage`,
`Io`, WAL, and collection code. The next revision should make the
boundary stricter by ensuring the `Storage` state machine does not own
the backend for its full lifetime.

### Architecture Requirements

1. `RING-IMPL-ARCH-001` `Storage` MUST own logical storage state and
configuration, but MUST NOT require long-lived ownership of the
backing I/O object.
2. `RING-IMPL-ARCH-002` The backing I/O object MUST instead be passed
into operation entry points or operation builders so the same
`Storage` value can participate in externally driven async execution.
3. `RING-IMPL-ARCH-003` WAL handling, region-management logic, and
collection-specific logic MUST remain separable modules with explicit
interfaces.
4. `RING-IMPL-ARCH-004` Encoding and decoding code MUST be usable from
pure tests without requiring live device I/O.
5. `RING-IMPL-ARCH-005` The implementation SHOULD model complex
multi-step procedures such as startup replay and reclaim as explicit
phase machines so that each durable transition is inspectable in code
review and testable in isolation.

## Async I/O Interface

The I/O boundary should remain minimal. Borromean only needs a small
set of primitive device actions: read bytes, program bytes, erase
regions, and ensure durability where the medium requires an explicit
sync or flush. Platform-specific interrupt handling, DMA completion,
transport ownership, and scheduling belong outside this crate.

This does not require a single concrete Rust surface. An
implementation may use trait methods returning `impl Future`,
associated future types, or handwritten future objects. The important
constraint is that these futures are non-allocating and statically
dispatched by default.

### I/O Requirements

1. `RING-IMPL-IO-001` The borromean I/O abstraction MUST expose only
the primitive operations needed to satisfy [spec/ring.md](ring.md):
region or metadata reads, writes, erases, and durability barriers.
2. `RING-IMPL-IO-002` The borromean I/O abstraction MUST be generic
over the caller's concrete transport or flash driver type.
3. `RING-IMPL-IO-003` The borromean I/O abstraction MUST be usable
without dynamic dispatch and without heap allocation.
4. `RING-IMPL-IO-004` If the target medium does not require an
explicit durability barrier, the I/O abstraction MAY implement sync as
a zero-cost completed operation.
5. `RING-IMPL-IO-005` Borromean MUST treat wakeups, DMA completion, or
interrupt delivery as an external concern of the caller-provided I/O
implementation rather than as an internal runtime service.

## Memory Model

The implementation must be able to run on targets where RAM budgets are
tight and allocation is unavailable or forbidden. That means every
capacity that can grow in principle must be made explicit in the API or
in type-level configuration. This includes collection registries,
startup replay tables, reclaim bookkeeping, decode scratch, and
per-operation staging buffers.

The prototype already trends in this direction with `heapless::Vec`
usage and caller-supplied buffers in WAL and map code. The new design
should make those capacities part of the external contract rather than
an incidental implementation detail.

### Memory Requirements

1. `RING-IMPL-MEM-001` The maximum number of tracked collections,
heads, replay entries, and other bounded in-memory items MUST be an
explicit compile-time or constructor-time capacity.
2. `RING-IMPL-MEM-002` Any operation that needs scratch space for
encoding, decoding, or staging MUST accept caller-provided buffers or
borrow dedicated storage from a caller-provided workspace object.
3. `RING-IMPL-MEM-003` If the configured capacities are insufficient to
open the store or complete an operation, the implementation MUST fail
explicitly with a capacity-related error rather than silently allocate
or truncate state.
4. `RING-IMPL-MEM-004` The implementation SHOULD avoid keeping
duplicate copies of large record payloads in memory when a borrowed
buffer or streaming decode is sufficient.
5. `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk
format constants MUST be derivable from public constants, associated
constants, or documented constructor contracts.

## Arithmetic Discipline

Storage code is full of offset, length, sequence, index, and capacity
math. In this domain, arithmetic bugs are not minor defects; they can
turn into silent truncation, out-of-bounds access, broken recovery, or
incorrect durability decisions. The implementation should therefore
default to checked arithmetic and treat arithmetic failure as an
explicit error path.

This applies both to data-path code and to test helpers. A future
optimization may justify proving that a particular operation cannot
overflow, but that proof should be reflected in the code structure
rather than by relying on unchecked integer behavior.

### Arithmetic Requirements

1. `RING-IMPL-ARITH-001` Integer arithmetic that can affect storage
layout, region addressing, WAL offsets, lengths, indexes, capacities,
or sequence advancement MUST use checked arithmetic or an equivalent
construction that makes overflow and underflow impossible by
construction.
2. `RING-IMPL-ARITH-002` If such arithmetic cannot be proven safe by
construction and a checked operation fails, the implementation MUST
return an explicit error rather than wrap, saturate, or silently
truncate.
3. `RING-IMPL-ARITH-003` The implementation MUST NOT rely on wrapping
integer behavior for correctness unless a future disk-format
requirement explicitly defines modulo arithmetic for that field.
4. `RING-IMPL-ARITH-004` Conversions between integer widths that may
lose information MUST be checked and MUST fail explicitly if the value
is out of range for the destination type.

## Panic Discipline

Embedded storage code should not treat malformed input, corrupt media,
capacity exhaustion, or internal state conflicts as reasons to abort
execution. Those are operational conditions that must surface as
explicit failures. For borromean core, "panic free" means the library's
non-test implementation must not rely on panics for correctness or for
ordinary error handling.

This requirement is stronger than merely compiling with `panic=abort`.
The implementation should be written so that production code paths
return explicit errors instead of panicking regardless of panic
strategy.

### Panic Requirements

1. `RING-IMPL-PANIC-001` The borromean core library and its non-test
support code MUST be panic free for all input data, including invalid
API inputs, corrupt on-storage state, exhausted capacities, and device
errors.
2. `RING-IMPL-PANIC-002` Recoverable failures and invariant violations
that can be caused by external input or storage state MUST be reported
through explicit error results rather than by panicking.
3. `RING-IMPL-PANIC-003` Non-test code MUST NOT use `panic!`,
`unwrap()`, `expect()`, `todo!()`, `unimplemented!()`, or
`unreachable!()` in any path that can be reached from public APIs or
from storage data under validation.
4. `RING-IMPL-PANIC-004` If a condition is believed to be impossible by
construction, the implementation SHOULD encode that proof in types,
control flow, or checked validation before the point of use rather than
relying on a panic as a backstop.

## Operation Futures

An operation future should be short-lived and linear: it borrows the
`Storage` state, borrows the I/O object, borrows the workspace it
needs, and finishes in a well-defined terminal result. It should not
register itself globally, spawn helper tasks, or require that another
borromean future be polled concurrently in order to finish.

This keeps the operational model simple: the caller decides when to
poll, the I/O layer decides when device actions are ready, and the
borromean future is just the coroutine joining those two.

### Operation Requirements

1. `RING-IMPL-OP-001` A borromean future MUST NOT require spawning
another borromean future in order to complete.
2. `RING-IMPL-OP-002` A borromean future MUST either complete with a
terminal result or remain safely resumable by further polling after
any `Poll::Pending`.
3. `RING-IMPL-OP-003` If an operation future is dropped before
completion, any already-issued durable writes MUST still satisfy the
crash-safety rules from [spec/ring.md](ring.md).
4. `RING-IMPL-OP-004` Pure in-memory state mutations that make a later
durable step mandatory MUST occur in an order that allows the same
operation to be retried or reconstructed after reset.
5. `RING-IMPL-OP-005` Public operations SHOULD minimize the duration of
mutable borrows of large caller workspaces so embedded callers can
reuse buffers across sequential operations.

## API Shape

The API should make the operational ownership model obvious. A caller
opens or formats storage, obtains a `Storage` state object, and then
passes `&mut Storage`, `&mut impl Io`, and a workspace into async
operations. Collection-specific code should fit the same pattern.

The important point is not the exact naming but the ownership
direction: borromean owns logical invariants, while callers own the
device, executor, and temporary memory.

### API Requirements

1. `RING-IMPL-API-001` Public entry points for format, open, replay,
and mutating collection operations MUST make their workspace and I/O
dependencies explicit in the function signature.
2. `RING-IMPL-API-002` The public API MUST allow a caller to drive the
same storage engine from either blocking test shims or asynchronous
device adapters without changing borromean correctness logic.
3. `RING-IMPL-API-003` Collection implementations MUST define their
opaque payload semantics above the shared storage primitives rather
than bypassing WAL and region-management invariants.
4. `RING-IMPL-API-004` The implementation SHOULD keep collection
operation APIs close to the prototype's explicit buffer-passing style
where that style avoids hidden allocation.
5. `RING-IMPL-API-005` The implementation MAY provide optional helper
adapters for common executors or embedded frameworks, but the core
crate MUST remain usable without them.

## Startup and Recovery Strategy

Startup is likely the most complex borromean operation. It must read
metadata, locate the effective WAL tail and WAL head, replay the live
collection state, rebuild bounded in-memory indexes, and detect any
incomplete reclaim or rotation that must be resumed. That is exactly
the kind of logic that benefits from an explicit async phase machine:
each phase can request reads, parse the result, update bounded replay
state, and continue.

### Startup Requirements

1. `RING-IMPL-STARTUP-001` Opening storage MUST be implemented as an
operation that can suspend between device interactions without losing
its replay context.
2. `RING-IMPL-STARTUP-002` Startup replay state MUST itself obey the
same no-allocation rule as steady-state operation.
3. `RING-IMPL-STARTUP-003` If startup needs temporary decode storage,
that storage MUST come from a caller-provided workspace or other
bounded static storage.
4. `RING-IMPL-STARTUP-004` Recovery of incomplete WAL rotation,
allocation, or reclaim state MUST be expressible through the same
operation framework used for normal foreground work.

## Collection Strategy

Collections should remain layered over shared storage machinery rather
than each one implementing its own device protocol. The prototype's WAL
and map modules are a useful direction here: collection code defines
payload semantics and local indexing, while shared infrastructure
handles region addressing, sequences, and persistence ordering.

### Collection Requirements

1. `RING-IMPL-COLL-001` Collection implementations MUST depend on the
shared storage engine for durability, ordering, and recovery rather
than duplicating those mechanisms ad hoc.
2. `RING-IMPL-COLL-002` Collection-specific in-memory state MUST obey
the same explicit-capacity and no-allocation rules as borromean core.
3. `RING-IMPL-COLL-003` A collection operation that needs I/O MUST be
drivable through the same runtime-agnostic future model as core
storage operations.

## Functional Regression Coverage

This section records regression behaviors that are exercised by
functional library tests. These requirements give mutation and
boundary tests stable Duvet trace targets when the behavior is not
already covered by a narrower storage, implementation, or map
requirement.

### Functional Regression Requirements

1. `RING-IMPL-REGRESSION-001` Channel construction MUST initialize a channel with the requested
   collection id, first member, next sequence 0, and first member last sequence 0.
2. `RING-IMPL-REGRESSION-002` Adding a new channel member MUST succeed when member storage has
   capacity and MUST retain both existing and added members.
3. `RING-IMPL-REGRESSION-003` Adding a channel member beyond configured member capacity MUST fail
   with UserLimitReached after filling available slots.
4. `RING-IMPL-REGRESSION-004` Channel last-sequence lookup MUST return the stored sequence for an
   existing member and MemberNotFound for an unknown member.
5. `RING-IMPL-REGRESSION-005` Channel next-sequence allocation MUST return the current sequence and
   increment subsequent next_sequence monotonically.
6. `RING-IMPL-REGRESSION-006` Adding an already-present channel member MUST be idempotent and MUST
   NOT create duplicate member entries.
7. `RING-IMPL-REGRESSION-007` Checkpoint channel commands MUST retain the previous checkpoint
   address, exact command count, and member sequence snapshot.
8. `RING-IMPL-REGRESSION-008` Recording a used channel sequence MUST update the member last
   sequence, track that member only once for checkpoint pressure, and reject unknown members.
9. `RING-IMPL-REGRESSION-009` Map run descriptors MUST use inclusive lower and upper key bounds for
   may_contain, integer helpers MUST advance offsets and reject short buffers, and manifest capacity
   checks MUST reject excess runs.
10. `RING-IMPL-REGRESSION-010` Snapshot helpers MUST validate snapshot layout, preserve
    set/delete/not-found lookup semantics, encode exact subranges, and reject out-of-bounds or
    undersized buffers.
11. `RING-IMPL-REGRESSION-011` Snapshot and frontier search helpers MUST find even-window keys and
    return the correct insertion position for missing keys.
12. `RING-IMPL-REGRESSION-012` Loading a snapshot MUST use entry reference offsets rather than
    physical entry byte order so reversed adjacent entry storage still loads sorted keys.
13. `RING-IMPL-REGRESSION-013` Snapshot encoding MUST accept exact empty snapshot capacity and
    snapshot decoding MUST reject invalid entry references.
14. `RING-IMPL-REGRESSION-014` Run cursors MUST advance segment positions correctly for ascending
    and descending run chains, and compaction writers MUST report segment-fit and state-count
    overflow errors.
15. `RING-IMPL-REGRESSION-015` Entry reference and entry count helpers MUST preserve exact
    serialized offsets and counts, and map checkpoints MUST restore prior frontier state while
    rejecting undersized buffers.
16. `RING-IMPL-REGRESSION-016` Run segment payloads MUST round-trip generation, next-region link,
    key bounds, and snapshot lookup semantics, and reject undersized or truncated payloads.
17. `RING-IMPL-REGRESSION-017` Committed-region helpers MUST accept boundary-sized payload regions
    and legacy snapshot helpers MUST decode exact empty-snapshot payloads.
18. `RING-IMPL-REGRESSION-018` Loading an empty snapshot MUST fit in a frontier buffer containing
    only the entry-count header and MUST leave lookups empty.
19. `RING-IMPL-REGRESSION-019` Map run selection and generation helpers MUST count only run-chain
    regions for live region totals, compaction selection, and next generation calculations.
20. `RING-IMPL-REGRESSION-020` Frontier range, region encoding, and checkpoint helpers MUST accept
    exact-size buffers, preserve lookup state, and reject undersized or malformed inputs.
21. `RING-IMPL-REGRESSION-021` Manifest descriptor loading MUST preserve run metadata and reject too
    many runs, zero-length run chains, and truncated descriptor payloads.
22. `RING-IMPL-REGRESSION-022` Snapshot run segment helpers MUST plan at least one region and encode
    requested snapshot subranges with generation, next-region link, bounds, and lookup semantics.
23. `RING-IMPL-REGRESSION-023` Snapshot run planning and storage writes MUST split snapshots that
    exceed one committed run payload across multiple run regions, return a descriptor with the exact
    state count and lower and upper keys, and return no descriptor for an empty snapshot.
24. `RING-IMPL-REGRESSION-024` Frontier run planning MUST count every committed run payload segment
    required for frontier contents that exceed one run-region payload.
25. `RING-IMPL-REGRESSION-025` Reclaiming map run regions MUST move all tracked run-chain regions to
    the storage free-list tail.
26. `RING-IMPL-REGRESSION-026` Committing a map manifest MUST reclaim the previous manifest region
    and retain only run-chain descriptors in the manifest state.
27. `RING-IMPL-REGRESSION-027` Flushing a map to storage MUST convert valid legacy region bases into
    run-chain descriptors and reject flushes that exceed configured run capacity.
28. `RING-IMPL-REGRESSION-028` Committed run storage helpers MUST read run segment bounds and next
    links only from matching map-run regions and reject non-run region headers.
29. `RING-IMPL-REGRESSION-029` Map lookup helpers MUST read both legacy region snapshots and
    manifest run chains, and head-reference checks MUST report manifest and run regions as
    reachable.
30. `RING-IMPL-REGRESSION-030` Opening a map from storage MUST replay only WAL records for the
    requested collection and ignore updates and drop records for other collections.
31. `RING-IMPL-REGRESSION-031` Entry reference serialization MUST preserve independent start and end
    offsets for distinct record indexes.
32. `RING-IMPL-REGRESSION-032` Storage WAL record visitation for maps MUST expose typed
    new-collection and snapshot records for map collections in durable order.
33. `RING-IMPL-REGRESSION-033` Map read/write operations MUST return the latest inserted values for
    generated key/value workloads.
34. `RING-IMPL-REGRESSION-034` Map write/delete operations MUST remove deleted keys while preserving
    non-deleted entries for generated workloads.
35. `RING-IMPL-REGRESSION-035` Disk byte helpers MUST advance offsets on reads and writes and return
    BufferTooSmall with needed and available sizes for short buffers.
36. `RING-IMPL-REGRESSION-036` The WAL record area offset MUST be aligned to the configured WAL
    write granule and follow the region header and prologue area.
37. `RING-IMPL-REGRESSION-037` Mock flash metadata read/write operations MUST persist metadata and
    log write/read metadata operations in order.
38. `RING-IMPL-REGRESSION-038` Mock flash storage reads MUST span metadata and data regions by
    absolute offset and reject out-of-bounds reads.
39. `RING-IMPL-REGRESSION-039` Mock flash metadata writes MUST fail without changing metadata when
    the metadata region is smaller than encoded StorageMetadata.
40. `RING-IMPL-REGRESSION-040` Mock flash metadata writes MUST succeed when the metadata region
    exactly matches encoded StorageMetadata and persist decodable metadata.
41. `RING-IMPL-REGRESSION-041` FlashIo metadata operations on MockFlash MUST delegate to mock
    metadata storage and return the persisted metadata.
42. `RING-IMPL-REGRESSION-042` Mock flash erase/write/read/sync operations MUST perform the
    operation and log each operation with region, offset, and length details.
43. `RING-IMPL-REGRESSION-043` Erasing a mock flash region MUST restore every byte in that region to
    the erased byte.
44. `RING-IMPL-REGRESSION-044` Formatting an empty mock store MUST accept the exact minimum region
    count and persist matching metadata.
45. `RING-IMPL-REGRESSION-045` Formatting an empty mock store MUST leave reserved bytes after
    encoded StorageMetadata erased.
46. `RING-IMPL-REGRESSION-046` Startup tail selection MUST ignore regions with nonzero collection_id
    even when their format is wal_v1 while still tracking max seen sequence.
47. `RING-IMPL-REGRESSION-047` Startup replay MUST preserve staged regions when a WAL head-control
    record is replayed.
48. `RING-IMPL-REGRESSION-048` Startup replay MUST preserve staged regions when non-map collection
    head and drop records are replayed.
49. `RING-IMPL-REGRESSION-049` Startup replay MUST count multiple live collections independently.
50. `RING-IMPL-REGRESSION-050` Startup replay MUST accept a committed-region head basis and recover
    the collection basis, collection type, and max seen sequence from that region.
51. `RING-IMPL-REGRESSION-051` Startup replay MUST accept a reclaimed historical head after
    replacement and recover the live replacement head with no pending reclaim.
52. `RING-IMPL-REGRESSION-052` Startup replay MUST track pending updates on an empty collection
    basis and preserve their count.
53. `RING-IMPL-REGRESSION-053` Startup replay MUST reject update records that appear after a
    collection drop tombstone for the same collection.
54. `RING-IMPL-REGRESSION-054` Strict WAL-region reads MUST reject regions whose collection_id is
    nonzero even if collection_format is wal_v1.
55. `RING-IMPL-REGRESSION-055` WAL target validation MUST require both collection_id 0 and
    collection_format wal_v1.
56. `RING-IMPL-REGRESSION-056` Live committed-region basis validation MUST reject a region whose
    header belongs to a different collection.
57. `RING-IMPL-REGRESSION-057` Region index validation MUST reject a region_index equal to
    region_count.
58. `RING-IMPL-REGRESSION-058` Startup replay MUST recover a WAL rotation after a durable link by
    selecting the linked tail, resetting tail append offset, updating allocator state, and advancing
    max sequence.
59. `RING-IMPL-REGRESSION-059` Startup replay MUST recover a WAL rotation when alloc_begin is
    durable but link is absent and only rotation reserve remains.
60. `RING-IMPL-REGRESSION-060` Startup replay MUST recover a WAL rotation when only the link record
    fits after alloc_begin at the tail boundary.
61. `RING-IMPL-REGRESSION-061` Startup replay MUST reject an unrecovered corrupt boundary in a
    non-tail WAL region as a broken WAL chain.
62. `RING-IMPL-REGRESSION-062` Opening a freshly formatted store MUST initialize allocator free-list
    head and tail from the formatted free-list chain.
63. `RING-IMPL-REGRESSION-063` Committed region writes MUST accept a payload that exactly fills
    committed payload capacity and persist the full payload bytes.
64. `RING-IMPL-REGRESSION-064` Formatting storage MUST return fresh runtime state with metadata, WAL
    head/tail, allocator, collection, and reclaim fields initialized.
65. `RING-IMPL-REGRESSION-065` WAL record visitation MUST report snapshot and update records after a
    new collection in durable WAL order.
66. `RING-IMPL-REGRESSION-066` Opening storage MUST return replayed runtime state with append
    offset, max sequence, collection type, committed basis, and pending update count.
67. `RING-IMPL-REGRESSION-067` Opening storage MUST complete reclaims for regions already on the
    free list and clear pending reclaim state.
68. `RING-IMPL-REGRESSION-068` Opening storage MUST discard pending reclaim records for regions
    still reachable from live collection state.
69. `RING-IMPL-REGRESSION-069` Appending a new collection and update MUST refresh runtime collection
    state and pending update count.
70. `RING-IMPL-REGRESSION-070` Appending a snapshot MUST move the collection to WAL snapshot basis
    and clear prior pending updates.
71. `RING-IMPL-REGRESSION-071` Appending head and drop records MUST refresh runtime basis to
    committed region and then dropped tombstone while reducing tracked live collection count.
72. `RING-IMPL-REGRESSION-072` Appending WAL recovery MUST clear pending recovery boundary and
    advance append offset; appending free-list-head MUST refresh allocator head and tail.
73. `RING-IMPL-REGRESSION-073` WAL rotation start/finish appends MUST reserve the next free region,
    advance allocator state, then move WAL tail to the new region and clear ready_region.
74. `RING-IMPL-REGRESSION-074` WAL rotation MUST initialize the new WAL region at max_seen_sequence
    + 1 and update runtime max_seen_sequence.
75. `RING-IMPL-REGRESSION-075` Reopening with uncommitted staged regions MUST reclaim staged regions
    and leave no ready or staged regions live.
76. `RING-IMPL-REGRESSION-076` Staging a region MUST reject region indexes that do not match the
    current ready_region.
77. `RING-IMPL-REGRESSION-077` Normal WAL appends MUST reject writes that would consume rotation
    reserve until WAL rotation completes, after which appends may continue.
78. `RING-IMPL-REGRESSION-078` WAL rotation start MUST reject calls made before the WAL tail has
    entered the rotation window.
79. `RING-IMPL-REGRESSION-079` Head append room checks MUST perform WAL rotation when the current
    tail lacks room for a head record.
80. `RING-IMPL-REGRESSION-080` Stage-region append room checks MUST reject staging when allocator
    state no longer matches the target region.
81. `RING-IMPL-REGRESSION-081` Encoded append reserve checks for alloc_begin MUST require a free
    region and return WalRotationRequired when none remains.
82. `RING-IMPL-REGRESSION-082` Encoded append reserve checks MUST allow alloc_begin when the tail
    has exactly the rotation reserve plus encoded record length remaining.
83. `RING-IMPL-REGRESSION-083` WAL-head reclaim classification MUST copy only head records that
    still reference the retained live region and skip stale head records.
84. `RING-IMPL-REGRESSION-084` WAL-head reclaim classification MUST copy drop tombstones only for
    collections that remain dropped and skip drops for live collections.
85. `RING-IMPL-REGRESSION-085` Foreground allocation headroom checks MUST reject allocations that
    would consume the configured minimum free-region reserve.
86. `RING-IMPL-REGRESSION-086` WAL-head reclaim copying MUST stop cleanly when a copied tail record
    ends exactly at the region end.
87. `RING-IMPL-REGRESSION-087` Live-state reachability checks MUST NOT parse non-map collection
    heads as maps.
88. `RING-IMPL-REGRESSION-088` Live-state reachability checks MUST follow live map manifest heads to
    referenced run regions.
89. `RING-IMPL-REGRESSION-089` Dropping a staged region in memory MUST remove only the matching
    staged region and preserve other staged regions.
90. `RING-IMPL-REGRESSION-090` WAL record visitation MUST process a tail record that ends exactly at
    the append limit and then stop.
91. `RING-IMPL-REGRESSION-091` WAL-chain membership checks MUST follow durable link targets to
    determine whether a region belongs to the chain.
92. `RING-IMPL-REGRESSION-092` CollectionId helpers MUST expose little-endian bytes and checked
    increment semantics, returning none on u64 overflow.
93. `RING-IMPL-REGRESSION-093` Storage facade accessors MUST reflect underlying runtime state and
    tracked collection metadata.
94. `RING-IMPL-REGRESSION-094` Storage facade raw WAL wrapper methods MUST update runtime
    collection, allocator, free-list, and reclaim state.
95. `RING-IMPL-REGRESSION-095` Storage facade WAL recovery append MUST reject recovery records when
    no recovery boundary is pending.
96. `RING-IMPL-REGRESSION-096` Storage facade recovery status MUST report pending WAL recovery
    boundaries and clear them after appending wal_recovery.
97. `RING-IMPL-REGRESSION-097` Storage format futures MUST poll to completion and return initialized
    storage state.
98. `RING-IMPL-REGRESSION-098` Storage open futures MUST poll to completion and replay collection
    pending update state.
99. `RING-IMPL-REGRESSION-099` Storage open futures MUST yield pending between startup phases before
    completing with recovered WAL head and tail.
100. `RING-IMPL-REGRESSION-100` Dropping a partially polled storage open future MUST leave the store
     openable with unchanged recovered state.
101. `RING-IMPL-REGRESSION-101` Storage WAL-head reclaim futures MUST poll to completion, update WAL
     head to the reclaimed successor, and append the old head to the free-list tail.
102. `RING-IMPL-REGRESSION-102` Storage WAL-head reclaim futures MUST yield between reclaim phases
     before completing with updated WAL head.
103. `RING-IMPL-REGRESSION-103` Dropping a WAL-head reclaim future after reclaim begins MUST leave
     the store recoverable with original WAL head and live collection basis.
104. `RING-IMPL-REGRESSION-104` Storage append operations MUST persist new collection and update
     records so reopening through flash restores the collection and pending update state.
105. `RING-IMPL-REGRESSION-105` WAL-head reclaim MUST update runtime WAL head and tail to the next
     region.
106. `RING-IMPL-REGRESSION-106` WAL-head reclaim MUST rewrite a live empty-head map as a WAL
     snapshot basis while preserving pending updates.
107. `RING-IMPL-REGRESSION-107` Storage operations MUST work through any FlashIo backend that
     implements the trait, including delegating backends.
108. `RING-IMPL-REGRESSION-108` Storage map APIs MUST restore snapshot basis values and later typed
     updates when opening a map.
109. `RING-IMPL-REGRESSION-109` Storage map flush API MUST write a committed region basis, clear
     ready_region, and preserve flushed key/value lookups.
110. `RING-IMPL-REGRESSION-110` Targeted then greedy map compaction MUST reduce selected runs while
     preserving unselected runs and all visible key/value lookups.
111. `RING-IMPL-REGRESSION-111` Map compaction MUST preserve tombstone masking so deleted keys
     remain absent and later live keys remain visible.
112. `RING-IMPL-REGRESSION-112` Map compaction MUST stream replacements larger than frontier
     capacity into a single run while preserving all visible key/value lookups across repeated
     compaction.
113. `RING-IMPL-REGRESSION-113` Reopening after a map replacement flush MUST complete pending
     reclaim of the replaced region and preserve the replacement map value.
114. `RING-IMPL-REGRESSION-114` Reopening after replacement with an empty free list MUST initialize
     free-list head from the recovered reclaimed region.
115. `RING-IMPL-REGRESSION-115` Reopening after replacement with an empty free list MUST reconstruct
     free-list tail from the recovered reclaimed region.
116. `RING-IMPL-REGRESSION-116` Map flush MUST complete detached pending reclaims before allocating
     from the minimum free-region reserve.
117. `RING-IMPL-REGRESSION-117` Reopening after a premature reclaim_begin before replacement
     detaches the old head MUST discard the pending reclaim and preserve the old map basis and
     value.
118. `RING-IMPL-REGRESSION-118` Dropping a map with committed-region basis MUST start reclaim for
     that region, tombstone the collection, complete reclaim on reopen, and reject reopening the
     dropped map.
119. `RING-IMPL-REGRESSION-119` Reopening after a premature reclaim_begin before drop detaches the
     live region MUST discard the pending reclaim and preserve the live map basis and value.
120. `RING-IMPL-REGRESSION-120` Dropping a map whose basis is a WAL snapshot MUST tombstone the
     collection without starting a region reclaim.
121. `RING-IMPL-REGRESSION-121` VecLikeSlice MUST report empty state, length, capacity, and slice
     contents from its logical items.
122. `RING-IMPL-REGRESSION-122` VecLikeSlice clear MUST remove only logical items, restore empty
     length, and allow reuse of underlying capacity.
123. `RING-IMPL-REGRESSION-123` WAL byte helpers MUST advance offsets for byte and byte-slice reads
     and writes and report BufferTooSmall with needed and available sizes on short buffers.
124. `RING-IMPL-REGRESSION-124` Logical WAL byte encoding MUST escape erased byte, record magic, and
     escape byte with distinct derived escape codes.
125. `RING-IMPL-REGRESSION-125` WAL record decoding MUST consume all encoded physical bytes and
     report encoded and logical lengths for decoded records.
126. `RING-IMPL-REGRESSION-126` WAL record decoding MUST wait until all payload-header bytes are
     available before reading payload metadata.
127. `RING-IMPL-REGRESSION-127` WAL record decoding MUST reject an empty logical scratch buffer
     before writing the first decoded logical byte.
128. `RING-IMPL-REGRESSION-128` Logical WAL record encoding MUST serialize fixed-width fields
     little-endian in canonical order.
129. `RING-IMPL-REGRESSION-129` Logical WAL record checksums MUST use CRC-32C over logical prefix
     bytes and store the checksum little-endian.
130. `RING-IMPL-REGRESSION-130` Update WAL records MUST round-trip through physical escaping,
     padding, and decoding without changing payload bytes.
131. `RING-IMPL-REGRESSION-131` Free-list-head WAL records with no region index MUST round-trip
     through physical encoding and decoding.
132. `RING-IMPL-REGRESSION-132` Alloc-begin WAL records MUST round-trip free_list_head_after through
     physical encoding and decoding.

## Non-Goals

This specification intentionally does not define a mandatory executor,
interrupt model, DMA abstraction, or collection API for every future
collection type. Those choices can remain platform-specific or can be
specified later, as long as they preserve the constraints above.

### Non-Goal Requirements

1. `RING-IMPL-NONGOAL-001` Borromean core MUST NOT require a specific
embedded framework, RTOS, or async executor.
2. `RING-IMPL-NONGOAL-002` Borromean core MUST NOT assume thread
support, background workers, or heap-backed task scheduling.
