# Channel Collection Specification

## Purpose

This specification defines the current behavior of the experimental channel collection surface. The
channel module is exported, but it is not yet integrated as a durable storage collection. Shared
storage ordering and committed-region mechanics remain defined by
[spec/ring/00-introduction.md](ring/00-introduction.md).

## Channel State And Member Sequences

The channel tracks a stable collection id, a bounded member set, the next channel sequence, and
member-local last-used sequences.

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
