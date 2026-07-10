# Mock Flash Specification

## Purpose

This specification defines the behavior of the in-repository mock flash backend used by tests and
examples. The mock is not a storage format, but its observable behavior is a traceability target
because it is the primary backend used to verify storage semantics in this repository.

## Mock Flash Requirements

Mock flash exposes the same primitive operations expected by the Borromean storage I/O boundary and
keeps an operation log for tests.

1. `RING-IMPL-REGRESSION-037` Mock flash metadata read/write operations MUST persist metadata and
   log write/read metadata operations in order.
2. `RING-IMPL-REGRESSION-038` Mock flash storage reads MUST span metadata and data regions by
   absolute offset and reject out-of-bounds reads.
3. `RING-IMPL-REGRESSION-039` Mock flash metadata writes MUST fail without changing metadata when
   the metadata region is smaller than encoded StorageMetadata.
4. `RING-IMPL-REGRESSION-040` Mock flash metadata writes MUST succeed when the metadata region
   exactly matches encoded StorageMetadata and persist decodable metadata.
5. `RING-IMPL-REGRESSION-041` FlashIo metadata operations on MockFlash MUST delegate to mock
   metadata storage and return the persisted metadata.
6. `RING-IMPL-REGRESSION-042` Mock flash erase/write/read/sync operations MUST perform the
   operation and log each operation with region, offset, and length details.
7. `RING-IMPL-REGRESSION-043` Erasing a mock flash region MUST restore every byte in that region to
   the erased byte.
8. `RING-IMPL-REGRESSION-044` Formatting an empty mock store MUST accept the exact minimum region
   count and persist matching metadata.
9. `RING-IMPL-REGRESSION-045` Formatting an empty mock store MUST leave reserved bytes after
   encoded StorageMetadata erased.
10. `RING-IMPL-REGRESSION-157` Mock flash operation logging controls MUST clear the existing log
    when disabled, suppress new operation log entries while disabled, and resume logging when
    re-enabled without suppressing the underlying flash operation.
