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

## Calibrating Run Size

The matrix should use enough measured operations that one scheduler pause or cache-state accident
does not dominate throughput. To find that point, run the calibration task:

```bash
./tasks.sh perf-calibrate
```

That task builds the release perf binary once, runs each matrix config repeatedly at increasing
operation counts, and writes a stability summary to:

```text
target/perf/perf_calibration_summary.md
```

The calibration summary reports relative MAD: median absolute deviation divided by median
throughput. The default stability thresholds are 3% for read-only workloads and 5% for write or
mixed workloads. redb and Fjall are run once per workload during calibration; additional Borromean
geometry configs run only the Borromean engines. The default write counts are intentionally
conservative because durable writes are slow; raise them when needed with:

```bash
BORROMEAN_PERF_CALIBRATION_WRITE_COUNTS=3000,10000,30000 ./tasks.sh perf-calibrate
```

The checked-in perf matrix uses the current stable run sizes from calibration:

- Insert, hot-update, and mixed-update scenarios use 10,000 measured operations.
- Read-hit and read-miss scenarios use 300,000 measured operations.

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

The matrix intentionally runs both 1 MiB and 4 KiB Borromean regions. Region size affects WAL
rotation frequency, frontier flush granularity, compaction granularity, mmap flush ranges, and
committed-read locality, so the two geometries are reported as separate Borromean columns. redb
and Fjall do not use Borromean region geometry, so they run and are shown once per workload.

## Current Local Results

The tables below were generated with:

```bash
./tasks.sh perf-matrix-summary
```

Bold values mark the best result in each comparable performance row.
Borromean geometry is shown as separate Borromean columns; redb and Fjall are shown once per
workload because Borromean region size does not apply to those engines.
All engines executed identical operation counts for each scenario; the JSON reports contain the
full count breakdown.

### Throughput (ops/s, higher is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 407.1 | 185.8 | 288.7 | **409.5** |
| update_hot | **406.3** | 196.9 | 307.5 | 402.4 |
| read_hits | 1,501,800.3 | 406,768.6 | 1,099,080.7 | **1,730,534.6** |
| read_misses | 2,222,884.9 | 2,575,030.7 | 1,150,544.4 | **2,964,721.3** |
| mixed_update | 1,975.7 | 956.7 | 1,464.9 | **2,003.3** |

### Relative Throughput (x, engine / borromean 1MiB when present, higher is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 1.00x | 0.46x | 0.71x | **1.01x** |
| update_hot | **1.00x** | 0.48x | 0.76x | 0.99x |
| read_hits | 1.00x | 0.27x | 0.73x | **1.15x** |
| read_misses | 1.00x | 1.16x | 0.52x | **1.33x** |
| mixed_update | 1.00x | 0.48x | 0.74x | **1.01x** |

### Latency P50 (time/op, lower is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 2.300ms | 2.328ms | 2.721ms | **2.234ms** |
| update_hot | **2.264ms** | 2.295ms | 2.511ms | 2.277ms |
| read_hits | 587ns | 2.297us | 822ns | **521ns** |
| read_misses | 394ns | 328ns | 767ns | **286ns** |
| mixed_update | **3.726us** | 30.751us | 4.635us | 4.330us |

### Latency P95 (time/op, lower is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | **3.001ms** | 6.565ms | 7.866ms | 3.314ms |
| update_hot | 3.821ms | 22.969ms | **2.905ms** | 4.187ms |
| read_hits | **838ns** | 3.957us | 1.369us | 859ns |
| read_misses | 856ns | **633ns** | 1.043us | 681ns |
| mixed_update | **2.309ms** | 2.367ms | 2.946ms | 2.336ms |

### Latency P99 (time/op, lower is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 54.715ms | 25.169ms | 51.157ms | **5.165ms** |
| update_hot | 5.672ms | 55.387ms | **3.183ms** | 5.544ms |
| read_hits | 2.112us | 43.984us | 3.341us | **2.008us** |
| read_misses | 3.611us | 1.200us | 10.877us | **988ns** |
| mixed_update | **2.603ms** | 2.834ms | 3.091ms | 2.669ms |

### Disk And IO (bytes)

| scenario | target | logical | file_len | path_size | process_read | process_write |
| --- | --- | --- | --- | --- | --- | --- |
| insert | borromean 1MiB | 65.00MiB | 65.00MiB | - | 0B | 39.98MiB |
| insert | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 74.60MiB |
| insert | redb | 1.43MiB | 1.43MiB | - | 0B | 255.21MiB |
| insert | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 40.19MiB |
| update_hot | borromean 1MiB | 65.00MiB | 65.00MiB | - | 0B | 40.97MiB |
| update_hot | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 69.62MiB |
| update_hot | redb | 292.00KiB | 292.00KiB | - | 0B | 234.38MiB |
| update_hot | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 40.19MiB |
| read_hits | borromean 1MiB | 65.00MiB | 65.00MiB | - | 0B | 0B |
| read_hits | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 0B |
| read_hits | redb | 292.00KiB | 292.00KiB | - | 0B | 0B |
| read_hits | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 0B |
| read_misses | borromean 1MiB | 65.00MiB | 65.00MiB | - | 0B | 0B |
| read_misses | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 0B |
| read_misses | redb | 292.00KiB | 292.00KiB | - | 0B | 0B |
| read_misses | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 0B |
| mixed_update | borromean 1MiB | 65.00MiB | 65.00MiB | - | 0B | 8.18MiB |
| mixed_update | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 14.30MiB |
| mixed_update | redb | 292.00KiB | 292.00KiB | - | 0B | 47.98MiB |
| mixed_update | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 8.23MiB |

### Durability Cost (time and bytes)

| scenario | target | mode | count | time | time/op | wal_bytes |
| --- | --- | --- | --- | --- | --- | --- |
| insert | borromean 1MiB | wal-sync | 10,000 | 23.889s | 2.389ms | 1015.59KiB |
| insert | borromean 4KiB | wal-sync | 10,000 | 24.514s | 2.451ms | 1015.59KiB |
| insert | redb | commit | 10,000 | 34.014s | 3.401ms | - |
| insert | fjall | sync-data | 10,000 | 24.174s | 2.417ms | - |
| update_hot | borromean 1MiB | wal-sync | 10,000 | 23.946s | 2.395ms | 1010.85KiB |
| update_hot | borromean 4KiB | wal-sync | 10,000 | 24.258s | 2.426ms | 1010.85KiB |
| update_hot | redb | commit | 11,000 | 35.383s | 3.217ms | - |
| update_hot | fjall | sync-data | 10,000 | 24.579s | 2.458ms | - |
| read_hits | borromean 1MiB | wal-sync | 0 | 0ns | - | 0B |
| read_hits | borromean 4KiB | wal-sync | 0 | 0ns | - | 0B |
| read_hits | redb | commit | 1,000 | 3.301s | 3.301ms | - |
| read_hits | fjall | sync-data | 0 | 0ns | - | - |
| read_misses | borromean 1MiB | wal-sync | 0 | 0ns | - | 0B |
| read_misses | borromean 4KiB | wal-sync | 0 | 0ns | - | 0B |
| read_misses | redb | commit | 1,000 | 3.121s | 3.121ms | - |
| read_misses | fjall | sync-data | 0 | 0ns | - | - |
| mixed_update | borromean 1MiB | wal-sync | 2,047 | 4.900s | 2.394ms | 207.05KiB |
| mixed_update | borromean 4KiB | wal-sync | 2,047 | 5.042s | 2.463ms | 207.05KiB |
| mixed_update | redb | commit | 3,047 | 9.849s | 3.232ms | - |
| mixed_update | fjall | sync-data | 2,047 | 4.908s | 2.398ms | - |

### Borromean Internals (counts, bytes, and time)

| scenario | target | cache_hits | cache_misses | reloads | wal_bytes | wal_sync | mmap_flush | compactions | undo_records | checkpoint_fallbacks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| insert | borromean 1MiB | 9,999 | 1 | 1 | 1015.59KiB | 23.889s | 23.860s | 0 | 10,000 | 0 |
| insert | borromean 4KiB | 10,014 | 1 | 1 | 1015.59KiB | 24.514s | 43.243s | 15 | 10,000 | 221 |
| update_hot | borromean 1MiB | 10,000 | 0 | 0 | 1010.85KiB | 23.946s | 23.937s | 0 | 10,000 | 0 |
| update_hot | borromean 4KiB | 10,015 | 0 | 0 | 1010.85KiB | 24.258s | 40.253s | 15 | 10,000 | 222 |
| read_hits | borromean 1MiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| read_hits | borromean 4KiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| read_misses | borromean 1MiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| read_misses | borromean 4KiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| mixed_update | borromean 1MiB | 10,000 | 0 | 0 | 207.05KiB | 4.900s | 4.894s | 0 | 2,047 | 0 |
| mixed_update | borromean 4KiB | 10,003 | 0 | 0 | 207.05KiB | 5.042s | 8.284s | 3 | 2,047 | 45 |
