# FileBacking mmap Backend

## Purpose

`FileBacking` is a Linux host-file storage backing. It uses one mutable
memory mapping over one database file and exposes the same primitive
storage operations as other Borromean backings.

`FileBacking` is not flash media. It simulates erased-byte state for
Borromean storage operations, but it does not simulate flash-only
programming restrictions such as one-way bit transitions.

## Public API

1. `RING-FILE-001` The crate MUST expose a `file-backing` feature that
enables the Linux `FileBacking` backend without making the default build
depend on `std`.
2. `RING-FILE-002` The file-backed API MUST expose
`FileBacking`, `FileBackingOptions`, `AllocationPolicy`, and
`MadvisePolicy`.

## File Geometry

1. `RING-FILE-003` A `FileBacking` database file MUST contain one
metadata region followed immediately by all data regions.
2. `RING-FILE-004` Data region `n` MUST start at byte offset
`(n + 1) * REGION_SIZE`.
3. `RING-FILE-005` On create and open, `FileBacking` MUST discover the
OS mmap page size and filesystem allocation block size through libc/POSIX
APIs.
4. `RING-FILE-006` `FileBacking` MUST define its required alignment unit
as the least common multiple of the OS mmap page size and filesystem
allocation block size.
5. `RING-FILE-007` Create and open MUST fail when `REGION_SIZE` or the
computed file length is not a multiple of the required alignment unit.
6. `RING-FILE-008` Opening an existing database file MUST fail when the
file length is not exactly `(REGION_COUNT + 1) * REGION_SIZE`.

## Allocation and mmap Advice

1. `RING-FILE-009` Creating a new database file MUST use exclusive file
creation and reject an already-existing path.
2. `RING-FILE-010` Creating a new database file MUST call
`fallocate(fd, 0, 0, file_len)` before creating the mmap.
3. `RING-FILE-011` The `FileBacking` specification MUST state that
`fallocate()` preallocates storage but does not guarantee physically
contiguous storage.
4. `RING-FILE-012` Under `AllocationPolicy::Strict`, any `fallocate()`
failure MUST fail database-file creation.
5. `RING-FILE-013` Under `AllocationPolicy::FallbackOnUnsupported`,
`FileBacking` MAY fall back to setting the file length only for
unsupported `fallocate()` failures such as `ENOSYS` or `EOPNOTSUPP`.
Capacity and quota failures such as `ENOSPC` MUST still fail creation.
6. `RING-FILE-014` After creating an mmap, `FileBacking` MUST apply
`madvise()` according to the configured `MadvisePolicy`. `madvise()`
MUST NOT replace `fallocate()`, mmap creation, page-size discovery,
filesystem block-size discovery, or durability sync.

## Backend Behavior

1. `RING-FILE-015` New database files MUST be initialized to the
configured erased byte before use.
2. `RING-FILE-016` Region reads, writes, and erases MUST reject region
indexes, offsets, or lengths outside the configured geometry.
3. `RING-FILE-017` Erasing a data region MUST fill the entire region with
the configured erased byte.
4. `RING-FILE-018` `FileBacking::sync()` MUST flush mmap changes and sync
the underlying file.
5. `RING-FILE-019` Formatted `FileBacking` storage MUST be usable through
the generic Borromean storage API.
