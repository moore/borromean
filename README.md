# Borromean

Borromean is `no_std` durable flash storage with bounded caller-owned memory.

The core is log-structured. It records durable transitions in a write-ahead log, replays them after
reset, allocates storage in erase-aligned regions, and exposes durable collections above those
storage rules. The implemented collection today is an LSM-style durable map.

## Why Borromean?

- **Recoverable state after reset**: committed operations are replayed from WAL and region metadata.
- **Bounded memory**: storage state and operation scratch live in caller-owned buffers; the core
  crate does not depend on `alloc`.
- **Flash-aware layout**: erase-region alignment, FIFO free-list allocation, and log-structured
  updates support wear leveling to prolong flash lifespan.

## Status

Borromean is alpha-quality engineering code. The storage core and durable map are working, covered
by local specs and traceability tests, and suitable for experiments and prototypes. Channel, queue,
and log-style collections remain experimental or planned. `MockFlash` supports tests and examples,
while the Linux file-backed backend is for host testing and benchmarking.

## Quick Start

This example uses `MockFlash` for the backing store. Real targets provide a flash driver by
implementing [`FlashIo`](src/flash_io.rs).

```rust
use borromean::{
    LsmMap, LsmMapMemory, MockFlash, Storage, StorageFormatConfig, StorageMemory,
};

const REGION_SIZE: usize = 512;
const REGION_COUNT: usize = 8;
const MAX_COLLECTIONS: usize = 8;

let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);

let collection_id = {
    let mut storage_memory = StorageMemory::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        &mut storage_memory,
    )
    .unwrap();

    let mut map_memory = LsmMapMemory::<u16, u16>::new();
    let mut map = LsmMap::<u16, u16>::new(&mut storage, &mut map_memory).unwrap();

    map.set(&mut storage, 7, 70).unwrap();
    assert_eq!(map.get(&mut storage, &7, |_, value| *value).unwrap(), Some(70));

    map.collection_id()
};

let mut reopen_memory = StorageMemory::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::new();
let mut reopened = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::open(
    &mut flash,
    &mut reopen_memory,
)
.unwrap();

let mut reopened_map_memory = LsmMapMemory::<u16, u16>::new();
let mut reopened_map =
    LsmMap::<u16, u16>::open(collection_id, &mut reopened, &mut reopened_map_memory).unwrap();

assert_eq!(
    reopened_map
        .get(&mut reopened, &7, |_, value| *value)
        .unwrap(),
    Some(70)
);
```

## Performance

Throughput is normalized to file-backed Borromean with 1 MiB regions for each scenario; higher is
better.

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 1.00x | 0.42x | 0.73x | **1.03x** |
| update_hot | 1.00x | 0.48x | 0.78x | **1.01x** |
| read_hits | **1.00x** | 0.21x | 0.50x | 0.76x |
| read_misses | 1.00x | **1.09x** | 0.33x | 0.82x |
| mixed_update | 1.00x | 0.51x | 0.77x | **1.05x** |

The current benchmark suite compares file-backed Borromean with
[redb](https://github.com/cberner/redb) and [Fjall](https://github.com/fjall-rs/fjall), using the
same deterministic workloads for each engine. The full generated report lives in
[BENCHMARKS.md](BENCHMARKS.md).

## Documentation

- Storage format and crash semantics: [spec/ring/00-introduction.md](spec/ring/00-introduction.md)
- Architecture and API guide: [docs/architecture-and-api.md](docs/architecture-and-api.md)
- Durable map format and behavior: [spec/map.md](spec/map.md)
- Mock flash behavior: [spec/mock.md](spec/mock.md)
- Contributor tutorial for adding collections: [docs/implementing-a-collection.md](docs/implementing-a-collection.md)
- Traceability and contribution notes: [CONTRIBUTING.md](CONTRIBUTING.md) and
  [spec/implementation-policy.md](spec/implementation-policy.md)
