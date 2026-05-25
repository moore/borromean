# Borromean

Borromean is a `no_std` flash-storage engine built around an append-only ring and durable collection
heads. It is designed for durable, wear-leveling storage directly on MCU flash, with caller-provided
device I/O and bounded operation scratch. The long-term goal is to host many collection instances
and support multiple collection types; durable maps are implemented today, while channel, queue, and
log-style collections remain experimental or planned.

This repository is alpha-quality engineering code. It contains a working storage core, a durable map
collection, a mock flash backend for tests and examples, a Linux file-backed backend for host
testing and benchmarking, and a traceability setup that links code and tests back to local
specifications.

The main remaining work is a deeper review of the durability state machines, continued cleanup of
the specs for readability, and simplification of implementation code where the design has become
clearer.

## Performance

Borromean aims to offer competitive performance for its durability model and target devices. The
current perf matrix compares file-backed Borromean with [redb](https://github.com/cberner/redb) and
[Fjall](https://github.com/fjall-rs/fjall), using the same deterministic workloads for each engine.
The full generated summary lives in [BENCHMARKS.md](BENCHMARKS.md).

### Relative Throughput

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 1.00x | 0.46x | 0.71x | **1.01x** |
| update_hot | **1.00x** | 0.48x | 0.76x | 0.99x |
| read_hits | 1.00x | 0.27x | 0.73x | **1.15x** |
| read_misses | 1.00x | 1.16x | 0.52x | **1.33x** |
| mixed_update | 1.00x | 0.48x | 0.74x | **1.01x** |

In the current local results, Borromean is close to Fjall on durable insert, hot-update, and mixed
read/update throughput; faster than redb on those write-heavy scenarios; and fastest on read-hit
throughput. Fjall is substantially faster on read misses. The IO tables show Borromean and Fjall
write similar byte counts in the write workloads, so the current write-side optimization focus is
durability sync cost rather than raw write volume.

## Supported Today

- The durable storage engine is implemented by [`Storage`](src/lib.rs).
- The caller supplies device access through [`FlashIo`](src/flash_io.rs); `Storage` binds that
  backing by exclusive mutable reference and owns the bounded scratch used by normal operations.
- The only user collection type that is supported durably today is the map collection implemented by
  [`LsmMap`](src/collections/map/mod.rs).
- The exported channel module is still experimental. It is documented as a public API surface, but
  it is not yet a durably integrated storage collection.

## Operating Model

Borromean keeps logical storage state and bounded operation scratch in `Storage`, while the concrete
device driver remains caller-provided through the `FlashIo` trait.
Callers can drive the same operations in two styles:

- Blocking entry points such as `Storage::format`, `Storage::open`, `Storage::create_map`, and
  `Storage::flush_map`.
- Future-returning entry points such as `Storage::format_future`, `Storage::open_future`,
  `Storage::create_map_future`, and `Storage::flush_map_future`.

In both styles the ownership model stays the same:

- Borromean owns storage invariants and on-disk ordering rules.
- The caller owns the flash driver and executor; `Storage` owns reusable operation scratch while it
  holds the backing reference.
- Map values are materialized in caller-owned buffers and serialized without heap allocation inside
  the core crate.

## Documentation Map

- Storage format and crash semantics: [spec/ring.md](spec/ring.md)
- Implementation architecture and API constraints: [spec/implementation.md](spec/implementation.md)
- Durable map collection format, current validation rules, and implemented
  whole-run LSM design: [spec/map.md](spec/map.md)
- Experimental channel collection behavior: [spec/channel.md](spec/channel.md)
- Mock flash backend behavior: [spec/mock.md](spec/mock.md)
- Narrative architecture and API guide: [docs/architecture-and-api.md](docs/architecture-and-api.md)
- Contributor tutorial for adding a durable collection:
  [docs/implementing-a-collection.md](docs/implementing-a-collection.md)

## Requirement Traceability

The storage spec in [spec/ring.md](spec/ring.md) now keeps normative requirements next to the
motivating text. Each requirement uses a stable identifier such as `RING-WAL-ENC-001` so Duvet
annotations can point at local spec text instead of a requirements appendix.

Example Rust annotation:

```rust
// = spec/ring.md#startup-replay-algorithm
// # RING-STARTUP-003 Select WAL tail as the unique candidate WAL region with the largest valid sequence.
```

The implementation docs follow the same pattern. `spec/implementation.md` captures architecture and
API constraints, while concrete functional behavior lives in storage, collection, or support
specifications such as [spec/ring.md](spec/ring.md), [spec/map.md](spec/map.md),
[spec/channel.md](spec/channel.md), and [spec/mock.md](spec/mock.md).
