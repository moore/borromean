# Blocking API And Memory Bounds

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

The API should expose the actual storage mechanics without forcing callers to
maintain two execution models. V3 therefore has one blocking interface. Long
logical activities are represented by small synchronous step machines and
explicit maintenance calls, which preserves crash injection and inspectable
I/O without exposing `Future`-returning duplicates.

The design targets `no_std` and devices where hidden allocation is
unacceptable. Runtime state and scratch space are caller-owned and sized from
declared constants, so memory exhaustion is a preflight outcome rather than a
mid-publication surprise.

## Interface layers

The raw device trait is intentionally mechanical. It exposes
geometry plus blocking metadata/region reads, aligned media writes, region erase,
and sync, with an associated backend error. It does not encode v3 headers or
provide a separate formatting implementation. `Storage::format` and
`Storage::open` own the on-media format and drive raw I/O.

Above raw I/O, the generic core exposes:

- storage format/open and retained-state recovery;
- bounded inline publication and region reservation operations;
- a fixed-capacity registry of open transactions and collection enrollments;
- committed catalog/root access without collection payload interpretation; and
- explicit `maintain_once(task)` transitions.

Typed collections layer map, channel, and object-log semantics above those
operations. They validate their own manifests and payloads when opened and
provide explicit flush or compaction steps, each with collection-specific
resource budgets. Those budgets limit measurable resource use, such as I/O
operations, bytes, search steps, or memory; the flush and compaction actions
themselves are part of the collection's operation set.

### Interface requirements

1. `CORE-API-001` The public storage interface MUST be blocking and MUST expose
   no Future-returning duplicate operations.
2. `CORE-API-002` The raw device trait MUST have an associated backend error
   type and expose geometry.
3. `CORE-API-003` Formatting MUST be implemented by the storage core using raw
   device operations.
4. `CORE-API-010` Typed collection validation MUST occur when that collection
   type is opened, not while the generic storage core scans unrelated data.

## Result and error model

A successful mutation returns `OperationResult<T>`: the requested logical
value plus compact maintenance-pressure flags. Success means the operation's
publication is durable and its runtime transition has been applied. Pressure
flags are advisory work that may be performed later; success does not imply
that maintenance was hidden in the call.

If required prepared capacity is absent, the operation returns
`MaintenanceRequired(flags)` before any media write, erase, or sync. If an enrolled collection is
write-reserved, a competing write returns
`CollectionWriteLocked(collection_id)` before any raw-device call. Neither
condition waits for a scheduler or OS lock.

Device failures retain the backend's associated error. Format corruption,
unsupported versions, geometry mismatch, ownership violations, capacity
pressure, lock conflicts, and typed collection errors remain distinguishable.

### Result and error requirements

1. `CORE-API-004` Successful mutations MUST return their logical value together
   with compact maintenance-pressure flags.
2. `CORE-API-005` An operation without sufficient prepared capacity MUST return
   `MaintenanceRequired(flags)` before issuing any media write, erase, or sync.
3. `CORE-API-006` A competing write to an enrolled collection MUST return
   `CollectionWriteLocked(collection_id)` before I/O.
4. `CORE-API-007` `maintain_once(task)` MUST perform a bounded blocking step and
   report whether more work remains.

## Caller-owned memory

The caller supplies fixed-capacity storage for:

- one fixed header per region during startup;
- the runtime ownership table and free-queue frontier;
- WAL encoding and region read scratch;
- collection catalog and retained roots;
- configured transaction slots containing identity, enrollment, private roots,
  and cleanup obligations, plus the single decision-to-finish WAL lock; and
- each typed collection's committed memory frontier and the sorting, indexing,
  packing, flush, or compaction workspace for one configured materialization
  unit.

Frontier memory is a configured pool rather than an assumption that every live
collection remains resident. A clean immutable-region basis needs no dirty
frontier slot. A clean WAL-snapshot basis can be decoded into a slot on demand;
later WAL updates are then applied to reconstruct its current frontier. When
the resident limit is reached, the caller explicitly snapshots or materializes
a selected dirty frontier before reusing its slot.

Every operation states which object supplies its memory and which constant
bounds iteration over it. An internal step machine may retain progress across
blocking calls, but it cannot borrow an OS executor as implicit state storage.

### Memory requirements

1. `CORE-API-008` Core runtime, transaction, recovery, and operation scratch
   memory MUST be caller-owned and bounded by declared configuration constants.
2. `CORE-API-009` Transaction memory MUST declare a maximum open-transaction
   count and retain each transaction's identity, enrolled collection
   generations, private roots, and ordered cleanup state together with the
   single decision-to-finish WAL lock.
3. `CORE-API-011` Before durably appending a collection mutation, the core MUST
   preflight space to represent its logical effect in the bounded collection
   memory frontier and MUST return `MaintenanceRequired(FLUSH_COLLECTION)`
   before media I/O when that space is unavailable.
4. `CORE-API-012` Typed collection maintenance MUST expose explicit snapshot and
   materialization steps so a caller can release a resident frontier slot
   without hidden I/O in the mutation that encountered memory pressure.

## Mechanical call sequence

A mutating call validates logical state and all capacity first. It then records
any required reservation, performs the format's declared physical writes and syncs,
publishes the durable decision, applies the corresponding pure runtime
transition, and returns the result with remaining pressure. Tests can drive
the same synchronous steps one at a time, inspect raw-I/O traces, and inject a
crash before any primitive operation or sync.

Dropping a handle or caller memory performs no implicit write. Work that must
survive handle loss has already been recorded durably and is completed by
explicit recovery or maintenance.
