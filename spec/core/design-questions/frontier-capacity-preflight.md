# Frontier Capacity Preflight Design Question

Status: active, non-normative input for D39. D14 and D41 own the related
residency and pressure semantics.

## Current specified design

Before a collection mutation is written to the WAL, the core verifies that the
mutation's logical effect fits in the collection's caller-owned memory frontier.
If it fits, the operation writes and syncs the WAL record, applies the mutation
to the frontier, and returns success. If it does not fit, the operation performs
no media I/O and returns collection-checkpoint pressure. The caller snapshots
or materializes the current frontier and retries.

This rule appeared in the archived pilot's
[goals chapter](../../archive/core-pilot/00-goals-locality.md) and was enforced
by `CORE-API-011` in its
[API chapter](../../archive/core-pilot/07-api-memory.md).

The design is acceptable and mechanically simple, but it is not yet accepted as
the best long-term choice.

## Why the current rule is attractive

- A successful durable mutation is immediately representable by runtime state.
- Committed reads do not need an emergency path that searches raw WAL records.
- Capacity failure occurs before media I/O, so retry behavior is unambiguous.
- Foreground writes do not hide snapshot or materialization work.
- Memory usage and query cost remain bounded by declared frontier capacity.
- Foreground apply and recovery can use the same collection transition.

## Alternatives to compare

### Permit a durable WAL-only mutation

Accept and sync a mutation even when no resident frontier slot can represent it.
The collection would retain a bounded WAL-resident delta state until memory is
available.

This could reduce write backpressure, but it needs answers for immediate read
visibility, bounded lookup of WAL-only deltas, ordering, transaction overlays,
memory-allocation failure after durability, and the point at which further
mutations must still be rejected.

### Reserve frontier capacity during admission

Represent mutation admission with a memory reservation token obtained before
the WAL append. This is close to the current rule but may separate capacity
planning from mutation execution and make multi-step or transactional writes
clearer.

### Maintain a shared spill frontier

Use a small bounded global structure for mutations whose collection frontier is
not resident. This may support more simultaneously active collections, but adds
another read overlay, eviction order, and recovery state machine.

### Snapshot or materialize inside the mutation

Automatically create room before appending the mutation. This avoids a caller
retry but conflicts with the explicit-maintenance rule and makes foreground
latency, sync count, and write amplification less predictable.

## Questions for the later decision

1. Must every successful write be immediately queryable without additional
   media reads?
2. Can a bounded WAL-only lookup structure preserve the required search and
   memory bounds?
3. Is frontier capacity owned per collection, drawn from a shared resident-slot
   pool, or a combination of both?
4. Does a WAL snapshot reliably release enough frontier capacity for the next
   mutation, or can decoding the snapshot require the same memory again?
5. How should one capacity decision cover a multi-collection transaction before
   its atomic commit record is written?
6. Which alternative gives the clearest correspondence between foreground
   apply, startup replay, and collection read semantics?
7. What API communicates backpressure without introducing hidden maintenance or
   unbounded retry behavior?

## Decision gate

Resolve this question through D39 before freezing a frontier-admission rule or
adapting typed collections to it. Feed the result back into D14's retained-delta
semantics and D41's pressure contract. The eventual decision should update the
core specification, transaction model, memory bounds, recovery model, and I/O
tests together.
