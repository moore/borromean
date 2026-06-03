# Embedded-Storage Backend

## Purpose

`EmbeddedStorageFlash` adapts `embedded-storage` NOR flash drivers to the
Borromean `FlashIo` boundary. It is intended for real embedded targets and
for tests that exercise hardware-like read, write, and erase granularity.

## Public API

1. `RING-EMBEDDED-001` The crate MUST expose an `embedded-storage`
feature that enables `EmbeddedStorageFlash`, `EmbeddedStorageOptions`,
`EmbeddedStorageMetadataField`, `EmbeddedStorageError`, and
`EmbeddedStorageFormatError` without enabling the `std` feature.
`EmbeddedStorageFlash` MUST expose `new`, `options`, `inner`,
`inner_mut`, and `into_inner` accessors for constructing the adapter,
inspecting its options, and recovering the wrapped flash object.

## Backend Behavior

1. `RING-EMBEDDED-002` `EmbeddedStorageFlash` MUST use the configured
`erased_byte` for metadata empty checks, metadata padding, erase
verification, strict write padding, and formatted `StorageMetadata`.
2. `RING-EMBEDDED-003` `EmbeddedStorageFlash` MUST map one metadata
region followed immediately by all data regions into the wrapped flash
address space.
3. `RING-EMBEDDED-004` `EmbeddedStorageFlash` MUST reject capacity,
region-alignment, and WAL write-granule configurations that cannot be
represented safely by the wrapped `NorFlash`.
4. `RING-EMBEDDED-005` Strict pad-only writes MUST read the aligned
hardware write span first and reject the write if any byte in that span
is not the configured erased byte.
5. `RING-EMBEDDED-006` Formatting through `EmbeddedStorageFlash` MUST
initialize WAL region prefixes as one contiguous write containing
`Header`, `WalRegionPrologue`, and erased bytes up to
`wal_record_area_offset`.
6. `RING-EMBEDDED-007` Formatted `EmbeddedStorageFlash` storage MUST be
usable through the generic Borromean storage API with a non-`0xff`
erased byte.
