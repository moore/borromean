# Benchmarks

This document records the current local performance matrix for Borromean and the
comparison engines used by the perf runner. The numbers are not intended to be
universal claims about the engines. They are a repeatable engineering signal for
this repository: same workload generator, same operation sequence, same value
sizes, and the same reporting format.

## Running The Matrix

Run the full comparison matrix with:

```bash
./tasks.sh perf-matrix
```

That command runs the configured scenarios and then prints a Markdown summary.
It also writes the same summary to:

```text
target/perf/perf_matrix_summary.md
```

To regenerate the Markdown summary from existing JSON reports without rerunning
the benchmarks:

```bash
./tasks.sh perf-matrix-summary
```

## Calibrating Run Size

The matrix should use enough measured operations that one scheduler pause or
cache-state accident does not dominate throughput. To find that point, run the
calibration task:

```bash
./tasks.sh perf-calibrate
```

That task builds the release perf binary once, runs each matrix config
repeatedly at increasing operation counts, and writes a stability summary to:

```text
target/perf/perf_calibration_summary.md
```

The calibration summary reports relative MAD: median absolute deviation divided
by median throughput. The default stability thresholds are 3% for read-only
workloads and 5% for write or mixed workloads. redb and Fjall are run once per
workload during calibration; additional Borromean geometry configs run only the
Borromean engines. The default write counts are intentionally conservative
because durable writes are slow; raise them when needed with:

```bash
BORROMEAN_PERF_CALIBRATION_WRITE_COUNTS=3000,10000,30000 ./tasks.sh perf-calibrate
```

The checked-in perf matrix uses the current stable run sizes from calibration:

- Insert, hot-update, and mixed-update scenarios use 10,000 measured operations.
- Read-hit and read-miss scenarios use 300,000 measured operations.

## Why These Benchmarks Exist

The matrix is designed to separate the performance questions we are actively
investigating:

- **Insert** measures durable WAL append cost and active frontier mutation for
  new keys.
- **Hot update** preloads keys and then overwrites them, which is the workload
  where an LSM-like design should plausibly do well.
- **Read hits** measures successful lookups against preloaded keys.
- **Read misses** measures negative lookup cost without writes.
- **Mixed update** combines mostly reads with durable updates, approximating a
  small hot working set.

The comparison engines are chosen for different reasons:

- **borromean** is the file-backed implementation with the current durability
  policy.
- **redb** is a mature Rust embedded B-tree comparison point.
- **fjall** is a Rust LSM-style comparison point and is closer to Borromean
  architecturally.

The file-backed write results should be read together with the durability and IO
tables. Borromean and Fjall write similar process byte counts in these
workloads, while redb often writes more.

Borromean's memory backend is intentionally omitted from this document because
it is an internal upper-bound diagnostic. To include it in ad hoc summaries, run
the formatter with `--include-memory`.

The durability table reports engine diagnostics from the full run. For read-only
measured workloads, redb may still show commit time/count from preload because
preload writes are durable, while measured read throughput excludes preload
time.

The matrix intentionally runs both 1 MiB and 4 KiB Borromean regions. Region
size affects WAL rotation frequency, frontier flush granularity, compaction
granularity, mmap flush ranges, and committed-read locality, so the two
geometries are reported as separate Borromean columns. redb and Fjall do not use
Borromean region geometry, so they run and are shown once per workload.

## Current Local Results

The tables below were generated with:

```bash
./tasks.sh perf-matrix-summary
```

Bold values mark the best result in each comparable performance row. Borromean
geometry is shown as separate Borromean columns; redb and Fjall are shown once
per workload because Borromean region size does not apply to those engines. All
engines executed identical operation counts for each scenario; the JSON reports
contain the full count breakdown.

### Throughput (ops/s, higher is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 397.1 | 168.7 | 291.6 | **409.1** |
| update_hot | 405.5 | 194.3 | 316.1 | **409.9** |
| read_hits | **2,154,156.4** | 460,095.2 | 1,069,172.8 | 1,636,149.8 |
| read_misses | 3,537,493.2 | **3,848,877.1** | 1,160,014.5 | 2,883,817.3 |
| mixed_update | 1,934.2 | 979.3 | 1,481.1 | **2,031.7** |

### Relative Throughput (x, engine / borromean 1MiB when present, higher is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 1.00x | 0.42x | 0.73x | **1.03x** |
| update_hot | 1.00x | 0.48x | 0.78x | **1.01x** |
| read_hits | **1.00x** | 0.21x | 0.50x | 0.76x |
| read_misses | 1.00x | **1.09x** | 0.33x | 0.82x |
| mixed_update | 1.00x | 0.51x | 0.77x | **1.05x** |

### Latency P50 (time/op, lower is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 2.275ms | 2.289ms | 2.639ms | **2.241ms** |
| update_hot | **2.205ms** | 2.287ms | 2.599ms | 2.243ms |
| read_hits | **411ns** | 1.953us | 820ns | 529ns |
| read_misses | 246ns | **200ns** | 764ns | 296ns |
| mixed_update | **2.718us** | 32.172us | 5.571us | 4.322us |

### Latency P95 (time/op, lower is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | 2.863ms | 2.896ms | 5.200ms | **2.639ms** |
| update_hot | **2.483ms** | 21.083ms | 3.415ms | 2.546ms |
| read_hits | **607ns** | 3.546us | 1.319us | 1.126us |
| read_misses | **573ns** | 680ns | 1.028us | 752ns |
| mixed_update | **2.287ms** | 2.402ms | 2.632ms | 2.470ms |

### Latency P99 (time/op, lower is better)

| scenario | borromean 1MiB | borromean 4KiB | redb | fjall |
| --- | --- | --- | --- | --- |
| insert | **4.919ms** | 6.709ms | 5.389ms | 4.946ms |
| update_hot | **2.548ms** | 22.494ms | 7.356ms | 2.726ms |
| read_hits | **1.539us** | 37.494us | 5.012us | 5.979us |
| read_misses | **987ns** | 1.103us | 17.334us | 1.077us |
| mixed_update | **2.490ms** | 2.604ms | 3.005ms | 2.690ms |

### Disk And IO (bytes)

| scenario | target | logical | file_len | path_size | process_read | process_write |
| --- | --- | --- | --- | --- | --- | --- |
| insert | borromean 1MiB | 65.00MiB | 65.00MiB | - | 0B | 39.98MiB |
| insert | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 79.53MiB |
| insert | redb | 1.43MiB | 1.43MiB | - | 0B | 255.21MiB |
| insert | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 40.19MiB |
| update_hot | borromean 1MiB | 65.00MiB | 65.00MiB | - | 0B | 40.97MiB |
| update_hot | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 70.41MiB |
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
| mixed_update | borromean 4KiB | 64.00MiB | 64.00MiB | - | 0B | 14.24MiB |
| mixed_update | redb | 292.00KiB | 292.00KiB | - | 0B | 47.98MiB |
| mixed_update | fjall | 64.00MiB | 64.00MiB | 64.00MiB | 0B | 8.23MiB |

### Durability Cost (time and bytes)

| scenario | target | mode | count | time | time/op | wal_bytes |
| --- | --- | --- | --- | --- | --- | --- |
| insert | borromean 1MiB | wal-sync | 10,000 | 24.516s | 2.452ms | 1015.59KiB |
| insert | borromean 4KiB | wal-sync | 10,000 | 24.873s | 2.487ms | 1015.59KiB |
| insert | redb | commit | 10,000 | 33.687s | 3.369ms | - |
| insert | fjall | sync-data | 10,000 | 24.203s | 2.420ms | - |
| update_hot | borromean 1MiB | wal-sync | 10,000 | 24.025s | 2.403ms | 1010.85KiB |
| update_hot | borromean 4KiB | wal-sync | 10,000 | 24.459s | 2.446ms | 1010.85KiB |
| update_hot | redb | commit | 11,000 | 34.444s | 3.131ms | - |
| update_hot | fjall | sync-data | 10,000 | 24.145s | 2.414ms | - |
| read_hits | borromean 1MiB | wal-sync | 0 | 0ns | - | 0B |
| read_hits | borromean 4KiB | wal-sync | 0 | 0ns | - | 0B |
| read_hits | redb | commit | 1,000 | 3.378s | 3.378ms | - |
| read_hits | fjall | sync-data | 0 | 0ns | - | - |
| read_misses | borromean 1MiB | wal-sync | 0 | 0ns | - | 0B |
| read_misses | borromean 4KiB | wal-sync | 0 | 0ns | - | 0B |
| read_misses | redb | commit | 1,000 | 3.442s | 3.442ms | - |
| read_misses | fjall | sync-data | 0 | 0ns | - | - |
| mixed_update | borromean 1MiB | wal-sync | 2,047 | 5.019s | 2.452ms | 207.05KiB |
| mixed_update | borromean 4KiB | wal-sync | 2,047 | 4.733s | 2.312ms | 207.05KiB |
| mixed_update | redb | commit | 3,047 | 9.938s | 3.262ms | - |
| mixed_update | fjall | sync-data | 2,047 | 4.839s | 2.364ms | - |

### Borromean Internals (counts, bytes, and time)

| scenario | target | cache_hits | cache_misses | reloads | wal_bytes | wal_sync | mmap_flush | compactions | undo_records | checkpoint_fallbacks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| insert | borromean 1MiB | 9,999 | 1 | 1 | 1015.59KiB | 24.516s | 24.486s | 0 | 10,000 | 0 |
| insert | borromean 4KiB | 10,015 | 1 | 1 | 1015.59KiB | 24.873s | 47.081s | 16 | 10,000 | 221 |
| update_hot | borromean 1MiB | 10,000 | 0 | 0 | 1010.85KiB | 24.025s | 24.016s | 0 | 10,000 | 0 |
| update_hot | borromean 4KiB | 10,016 | 0 | 0 | 1010.85KiB | 24.459s | 40.914s | 16 | 10,000 | 222 |
| read_hits | borromean 1MiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| read_hits | borromean 4KiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| read_misses | borromean 1MiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| read_misses | borromean 4KiB | 300,000 | 0 | 0 | 0B | 0ns | 0ns | 0 | 0 | 0 |
| mixed_update | borromean 1MiB | 10,000 | 0 | 0 | 207.05KiB | 5.019s | 5.014s | 0 | 2,047 | 0 |
| mixed_update | borromean 4KiB | 10,003 | 0 | 0 | 207.05KiB | 4.733s | 8.062s | 3 | 2,047 | 45 |
