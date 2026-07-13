# Region Ownership

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

Region ownership prevents the most damaging class of storage errors: two
structures believing they own the same erase region, reusing a region while it
is still reachable, or interpreting a system region as collection data. It is
kept as a small pure state machine so foreground code, replay, tests, and model
checking share one definition of a legal transition.

## Ownership identities and lifecycle

The ownership table assigns one lifecycle state to every physical region. The
state machine itself performs no I/O; foreground operations and recovery feed
it facts after the required durability conditions have been met.

Three identities give each transition its meaning:

- **purpose** identifies the physical format being built, such as main WAL,
  transaction log, free-space basis, bootstrap storage, or user collection
  data;
- **operation identity** identifies the particular construction attempt that
  reserved the region; and
- **owner** identifies the retained system structure or collection allowed to
  reach a published region.

A system purpose is compatible only with the matching system owner. A user
collection-data purpose carries its collection type and is compatible only with
a collection owner of that type. A transaction-log purpose additionally names
the transaction that may retain the log, preventing publication to another open
transaction. The operation identity connects that compatible owner to the
construction attempt named by the durable reservation.

Purpose is fixed at reservation time because the region has no authoritative
owner while its bytes are being constructed. Recording it in the durable
reservation and token commits that construction attempt to one physical format,
so publication cannot reinterpret completed bytes as a different system role or
collection type. Operation identity distinguishes construction attempts, but
does not identify their encoding.

The recyclable lifecycle is:

`ErasedPrepared -> Reserved -> Published -> Dirty -> ErasedPrepared`

The states mean:

- `ErasedPrepared`: the region is unowned, physically erased, and covered by a
  durable readiness fact, so FIFO allocation may select it;
- `Reserved(purpose, operation)`: the named operation has claimed the region for
  the named format while it builds and syncs the content;
- `Published(owner)`: a durable publication makes the completed region reachable
  by exactly one retained owner; and
- `Dirty`: the previous owner has durably released the region, while its old
  bytes remain until erase maintenance.

### State and identity requirements

1. `CORE-OWN-001` Every region MUST have exactly one lifecycle state.
2. `CORE-OWN-002` A region MUST have at most one logical owner, and only a
   `Published` region may have one.
3. `CORE-OWN-003` A `Reserved` state MUST contain the reservation's purpose and
   operation identity.
4. `CORE-OWN-004` Encoded system purposes MUST distinguish main WAL, transaction
   log, free-space basis, and format bootstrap storage from user collection
   data.

## Reservation and publication

Allocation preflights the capacity needed to complete its durable operation and
chooses the region at the FIFO allocation position. It appends and syncs a
reservation fact before applying `ErasedPrepared -> Reserved` in memory. The
transition yields a non-copyable token containing the region, purpose, and
operation identity.

The caller writes the reserved region using the physical layout declared by its
format and syncs the complete content. It then appends and syncs the publication
record. The publication API validates the proposed owner against the token,
consumes that token, and applies `Reserved -> Published`. The token connects the
publication to the exact reservation and prevents a raw region index from
publishing unrelated or stale content.

### Reservation and publication requirements

1. `CORE-OWN-005` A reservation transition MUST accept only an
   `ErasedPrepared` region; any other source state MUST return a typed ownership
   error without changing the state.
2. `CORE-OWN-006` A publication transition MUST accept only a matching
   `Reserved` state after its publication record is durable, and MUST produce
   `Published(owner)`.
3. `CORE-OWN-010` The low-level publication API MUST consume a non-copyable
   reservation token and MUST NOT accept an arbitrary region index in its place.
4. `CORE-OWN-011` Publication MUST reject a token whose region, purpose, or
   operation identity does not match the durable reservation, or whose proposed
   owner is incompatible with that purpose, without changing ownership state.

## Release, erase, and reuse

A logical free record removes the retained owner's reachability. After that
record is synced, ownership apply moves the region from `Published` to `Dirty`.
The dirty state keeps the region out of the prepared FIFO range while its old
bytes remain.

Erase maintenance selects the next ordered dirty region, erases it, and appends
and syncs a readiness fact. Applying that durable fact moves the region from
`Dirty` to `ErasedPrepared`, where FIFO allocation can consume it again.

### Release and reuse requirements

1. `CORE-OWN-007` A logical release transition MUST accept only a `Published`
   region and, after its free fact is durable, transition it to `Dirty`; the
   released region MUST remain unavailable to allocation.
2. `CORE-OWN-008` A readiness transition MUST accept only a `Dirty` region and
   transition it to `ErasedPrepared` only after successful explicit erase and a
   durable readiness fact.

## Crash recovery and permanent ownership

A crash can leave a durable fact ahead of the in-memory ownership table.
Startup replays reservation, publication, free, and readiness facts to rebuild
the same state that foreground apply would have produced. A reserved region
whose content was synced but never published remains reserved and unreachable;
recovery or maintenance can later reclaim it through its operation identity.

Metadata-named bootstrap regions begin in permanent published system ownership.
The initial WAL selects their free-space basis, and those regions remain outside
the recyclable lifecycle.

### Recovery-equivalence requirement

1. `CORE-OWN-009` For every valid durable ownership-event sequence, foreground
   application and startup replay MUST produce identical ownership tables; for
   an invalid event, both paths MUST return the same typed ownership error at
   the same event without applying it.

## Formal refinement evidence

The ownership design has two independent Quint representations. The
[abstract ownership model](../../../models/archive/core-pilot/region_ownership.qnt)
defines this chapter's lifecycle, identities, linear reservation authority,
transition errors, and replay fold without device state. The shared
[mechanical storage model](../../../models/archive/core-pilot/storage_mechanical.qnt)
represents issued work, durable records, content and erase completion, runtime
apply, crash outcomes, and recovery.

The
[ownership refinement bridge](../../../models/archive/core-pilot/ownership_refinement.qnt)
imports both models and maps recoverable mechanical state to the abstract
ownership table. It checks forward refinement: each mechanical step must produce
one permitted abstract ownership batch or leave abstract ownership unchanged.
The abstract model does not import the mechanical model, and the mechanical
model does not import this chapter's abstraction.

This bounded model supplies evidence for the logical transition requirements.
The encoded namespace portion of `CORE-OWN-004` and Rust's compile-time
non-copyability for `CORE-OWN-010` remain concrete implementation obligations;
the model represents their logical counterparts with disjoint variants and a
uniqueness-checked linear token registry.
