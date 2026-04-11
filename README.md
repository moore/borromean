# Borromean

Borromean is a `no_std` flash-storage engine built around an append-only ring and durable collection heads. The repository currently contains a working storage core, a durable map collection, a mock flash backend for tests and examples, and a traceability setup that links code and tests back to local specifications.

## Supported Today

- The durable storage engine is implemented by [`Storage`](src/lib.rs).
- The caller supplies device access through [`FlashIo`](src/flash_io.rs) and temporary buffers through [`StorageWorkspace`](src/workspace.rs).
- The only user collection type that is supported durably today is the map collection implemented by [`LsmMap`](src/collections/map/mod.rs).
- The exported channel module is still experimental. It is documented as a public API surface, but it is not yet a durably integrated storage collection.

## Operating Model

Borromean keeps logical storage state in `Storage` and keeps device ownership outside the crate. Callers can drive the same operations in two styles:

- Blocking entry points such as `Storage::format`, `Storage::open`, `Storage::create_map`, and `Storage::flush_map`.
- Future-returning entry points such as `Storage::format_future`, `Storage::open_future`, `Storage::create_map_future`, and `Storage::flush_map_future`.

In both styles the ownership model stays the same:

- Borromean owns storage invariants and on-disk ordering rules.
- The caller owns the flash driver, executor, and scratch memory.
- Map values are materialized in caller-owned buffers and serialized without heap allocation inside the core crate.

## Documentation Map

- Storage format and crash semantics: [spec/ring.md](spec/ring.md)
- Implementation architecture and API constraints: [spec/implementation.md](spec/implementation.md)
- Durable map collection format and validation rules: [spec/map.md](spec/map.md)
- Narrative architecture and API guide: [docs/architecture-and-api.md](docs/architecture-and-api.md)
- Contributor tutorial for adding a durable collection: [docs/implementing-a-collection.md](docs/implementing-a-collection.md)
- Documentation work tracker: [docs/implementation-docs-plan.md](docs/implementation-docs-plan.md)

## Requirement Traceability

The storage spec in [spec/ring.md](spec/ring.md) now keeps normative requirements next to the motivating text. Each requirement uses a stable identifier such as `RING-WAL-ENC-001` so Duvet annotations can point at local spec text instead of a requirements appendix.

Example Rust annotation:

```rust
// = spec/ring.md#startup-replay-algorithm
// # RING-STARTUP-003 Select WAL tail as the unique candidate WAL region with the largest valid sequence.
```

The implementation docs follow the same pattern. `spec/implementation.md` captures architecture and API constraints, while collection-specific normative behavior lives in collection specifications such as [spec/map.md](spec/map.md).
