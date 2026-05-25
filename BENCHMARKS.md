# Benchmarks

This document records the current local performance matrix for Borromean and the comparison
engines used by the perf runner. The numbers are not intended to be universal claims about the
engines. They are a repeatable engineering signal for this repository: same workload generator,
same operation sequence, same value sizes, and the same reporting format.

## Running The Matrix

Run the full comparison matrix with:

```bash
./tasks.sh perf-matrix
```

That command runs the configured scenarios and then prints a Markdown summary. It also writes the
same summary to:

```text
target/perf/perf_matrix_summary.md
```

To regenerate the Markdown summary from existing JSON reports without rerunning the benchmarks:

```bash
./tasks.sh perf-matrix-summary
```

## Why These Benchmarks Exist

The matrix is designed to separate the performance questions we are actively investigating:

- **Insert** measures durable WAL append cost and active frontier mutation for new keys.
- **Hot update** preloads keys and then overwrites them, which is the workload where an LSM-like
  design should plausibly do well.
- **Read hits** measures successful lookups against preloaded keys.
- **Read misses** measures negative lookup cost without writes.
- **Mixed update** combines mostly reads with durable updates, approximating a small hot working
  set.

The comparison engines are chosen for different reasons:

- **borromean** is the file-backed implementation with the current durability policy.
- **redb** is a mature Rust embedded B-tree comparison point.
- **fjall** is a Rust LSM-style comparison point and is closer to Borromean architecturally.

The file-backed write results should be read together with the durability and IO tables. Borromean
and Fjall write similar process byte counts in these workloads, while redb often writes more.

Borromean's memory backend is intentionally omitted from this document because it is an internal
upper-bound diagnostic. To include it in ad hoc summaries, run the formatter with
`--include-memory`.

The durability table reports engine diagnostics from the full run. For read-only measured
workloads, redb may still show commit time/count from preload because preload writes are durable,
while measured read throughput excludes preload time.

## Current Local Results

The tables below were generated with:

```bash
./tasks.sh perf-matrix-summary
```

Bold values mark the best result in each comparable performance row.
All engines executed identical operation counts for each scenario; the JSON reports contain the
full count breakdown.

### Throughput (ops/s, higher is better)

| scenario | borromean | redb | fjall |
| --- | --- | --- | --- |
| insert | 391.2 | 285.9 | **400.9** |
| update_hot | **417.9** | 326.8 | 412.3 |
| read_hits | **529,483.3** | 258,738.1 | 443,332.6 |
| read_misses | 549,950.1 | 289,932.8 | **1,961,747.2** |
| mixed_update | 1,832.0 | 1,510.0 | **1,899.7** |

### Relative Throughput (x, engine / borromean, higher is better)

| scenario | borromean | redb | fjall |
| --- | --- | --- | --- |
| insert | 1.00x | 0.73x | **1.02x** |
| update_hot | **1.00x** | 0.78x | 0.99x |
| read_hits | **1.00x** | 0.49x | 0.84x |
| read_misses | 1.00x | 0.53x | **3.57x** |
| mixed_update | 1.00x | 0.82x | **1.04x** |

### Latency P50 (time/op, lower is better)

| scenario | borromean | redb | fjall |
| --- | --- | --- | --- |
| insert | 2.206ms | 2.532ms | **2.176ms** |
| update_hot | 2.166ms | 2.465ms | **2.158ms** |
| read_hits | 2.207us | 3.098us | **1.979us** |
| read_misses | 1.924us | 3.818us | **534ns** |
| mixed_update | 4.812us | 4.498us | **4.277us** |

### Latency P95 (time/op, lower is better)

| scenario | borromean | redb | fjall |
| --- | --- | --- | --- |
| insert | **2.359ms** | 4.876ms | 5.182ms |
| update_hot | **2.509ms** | 4.880ms | 2.629ms |
| read_hits | 40.126us | 44.871us | **11.954us** |
| read_misses | 57.558us | 16.423us | **3.989us** |
| mixed_update | 2.379ms | 2.726ms | **647.387us** |

### Latency P99 (time/op, lower is better)

| scenario | borromean | redb | fjall |
| --- | --- | --- | --- |
| insert | **2.359ms** | 4.876ms | 5.182ms |
| update_hot | **2.519ms** | 101.959ms | 3.685ms |
| read_hits | 40.126us | 44.871us | **11.954us** |
| read_misses | 57.558us | 16.423us | **3.989us** |
| mixed_update | 2.379ms | 2.726ms | **647.387us** |

### Disk And IO (bytes)

| scenario | engine | logical | file_len | path_size | process_read | process_write |
| --- | --- | --- | --- | --- | --- | --- |
| insert | borromean | 65.00MiB | 65.00MiB | - | 0B | 11.99MiB |
| insert | redb | 436.00KiB | 436.00KiB | - | 0B | 70.41MiB |
| insert | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 12.06MiB |
| update_hot | borromean | 65.00MiB | 65.00MiB | - | 0B | 40.97MiB |
| update_hot | redb | 292.00KiB | 292.00KiB | - | 0B | 234.38MiB |
| update_hot | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 40.19MiB |
| read_hits | borromean | 65.00MiB | 65.00MiB | - | 0B | 0B |
| read_hits | redb | 292.00KiB | 292.00KiB | - | 0B | 0B |
| read_hits | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 0B |
| read_misses | borromean | 65.00MiB | 65.00MiB | - | 0B | 0B |
| read_misses | redb | 292.00KiB | 292.00KiB | - | 0B | 0B |
| read_misses | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 0B |
| mixed_update | borromean | 65.00MiB | 65.00MiB | - | 0B | 2.46MiB |
| mixed_update | redb | 292.00KiB | 292.00KiB | - | 0B | 14.44MiB |
| mixed_update | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 2.47MiB |

### Durability Cost (time and bytes)

| scenario | engine | mode | count | time | time/op | wal_bytes |
| --- | --- | --- | --- | --- | --- | --- |
| insert | borromean | wal-sync | 3,000 | 7.511s | 2.504ms | 304.68KiB |
| insert | redb | commit | 3,000 | 10.338s | 3.446ms | - |
| insert | fjall | sync-data | 3,000 | 7.419s | 2.473ms | - |
| update_hot | borromean | wal-sync | 10,000 | 23.359s | 2.336ms | 1010.85KiB |
| update_hot | redb | commit | 11,000 | 33.651s | 3.059ms | - |
| update_hot | fjall | sync-data | 10,000 | 24.021s | 2.402ms | - |
| read_hits | borromean | wal-sync | 0 | 0ns | - | 0B |
| read_hits | redb | commit | 1,000 | 3.288s | 3.288ms | - |
| read_hits | fjall | sync-data | 0 | 0ns | - | - |
| read_misses | borromean | wal-sync | 0 | 0ns | - | 0B |
| read_misses | redb | commit | 1,000 | 3.059s | 3.059ms | - |
| read_misses | fjall | sync-data | 0 | 0ns | - | - |
| mixed_update | borromean | wal-sync | 616 | 1.594s | 2.587ms | 62.30KiB |
| mixed_update | redb | commit | 1,616 | 5.017s | 3.105ms | - |
| mixed_update | fjall | sync-data | 616 | 1.559s | 2.530ms | - |

### Borromean Internals (counts, bytes, and time)

| scenario | engine | cache_hits | cache_misses | reloads | wal_bytes | wal_sync | mmap_flush | compactions | undo_records | checkpoint_fallbacks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| insert | borromean | 2,999 | 1 | 1 | 304.68KiB | 7.511s | 7.504s | 0 | 3,000 | 0 |
| update_hot | borromean | 10,000 | 0 | 0 | 1010.85KiB | 23.359s | 23.351s | 0 | 10,000 | 0 |
| read_hits | borromean | 3,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| read_misses | borromean | 3,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| mixed_update | borromean | 3,000 | 0 | 0 | 62.30KiB | 1.594s | 1.592s | 0 | 616 | 0 |
