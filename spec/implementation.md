# Implementation Strategy

## Purpose

This specification defines the Rust implementation strategy for the
low-level storage design in [spec/ring.md](ring.md).
[spec/ring.md](ring.md) remains the source of truth for storage
semantics, crash ordering, and on-disk format. This document defines
how those rules are to be realized in a `no_std`, `no_alloc`,
runtime-agnostic Rust implementation.

## Requirements Format

This specification keeps normative requirements adjacent to the text
that motivates them. Each normative requirement starts with a stable
identifier such as `RING-IMPL-CORE-001` and uses explicit normative
language such as `MUST`, `MUST NOT`, `SHOULD`, or `MAY`.

These identifiers are intended to be Duvet traceability targets for
implementation-architecture decisions that are not themselves on-disk
format requirements.

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

## Verification Strategy

Requirement traceability should stay mechanically simple. If a single
test body tries to verify many independent requirements at once, the
trace becomes noisy and failure diagnosis gets worse. A tighter rule is
better: one requirement, one dedicated test function, with any shared
setup moved into helpers.

This does not forbid helper code, fixtures, macros, or shared property
generators. It only constrains how normative requirements are claimed
by top-level tests.

### Verification Requirements

1. `RING-IMPL-TEST-001` Every normative requirement in
[spec/ring.md](ring.md) or this specification MUST have at least one
dedicated automated test function or dedicated compile-time test case
whose primary purpose is to verify that single requirement.
2. `RING-IMPL-TEST-002` A top-level automated test function MUST NOT
claim to verify multiple normative requirement identifiers.
3. `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions,
macros, and data generators MAY be reused across requirement-specific
tests, but the final traced test entry point MUST remain specific to
one requirement identifier.
4. `RING-IMPL-TEST-004` When a requirement is verified by a
compile-fail, compile-pass, or other non-runtime harness, that harness
entry MUST still be dedicated to a single requirement identifier.

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
