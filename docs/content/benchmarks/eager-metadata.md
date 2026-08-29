# Lazy and eager scan metadata

Use the provider-execution benchmark to compare repeated queries against one
loaded table. Run a matched pair in which only the scan metadata mode and output
path differ:

```bash
cargo bench --locked -p delta-arrow-reader --bench reader --all-features -- \
  --mode provider-exec \
  --seed 0 \
  --output target/eager-metadata-lazy.csv \
  --provider-exec-temp-dir target/eager-metadata-bench \
  --provider-exec-storage-profile local \
  --provider-exec-workload provider_many_unequal_files \
  --provider-exec-query filter_tail_ids \
  --provider-exec-backend direct-parquet \
  --provider-exec-scan-metadata-mode lazy \
  --provider-exec-queries-per-table 3 \
  --provider-exec-scheduling-profile prefetch_2_ap_target_scan_3x \
  --provider-exec-parquet-metadata-size-hint-bytes 65536 \
  --provider-exec-parquet-full-file-read-threshold-bytes disabled \
  --provider-exec-repetitions 5

cargo bench --locked -p delta-arrow-reader --bench reader --all-features -- \
  --mode provider-exec \
  --seed 0 \
  --output target/eager-metadata-eager.csv \
  --provider-exec-temp-dir target/eager-metadata-bench \
  --provider-exec-storage-profile local \
  --provider-exec-workload provider_many_unequal_files \
  --provider-exec-query filter_tail_ids \
  --provider-exec-backend direct-parquet \
  --provider-exec-scan-metadata-mode eager \
  --provider-exec-queries-per-table 3 \
  --provider-exec-scheduling-profile prefetch_2_ap_target_scan_3x \
  --provider-exec-parquet-metadata-size-hint-bytes 65536 \
  --provider-exec-parquet-full-file-read-threshold-bytes disabled \
  --provider-exec-repetitions 5
```

Each repetition loads and registers a new table, then runs the configured query
three times against that same table. Keep `--seed`, the temporary directory,
workload, query, backend, storage, scheduling, Parquet settings, repetition
count, and query count identical between the two commands. Use at least three
queries per table so the comparison includes metadata reuse rather than only
the initialization tradeoff.

The CSV separates the relevant timing boundaries:

- `table_initialization_micros_*` measures table loading. It ends before
  DataFusion table registration.
- `planning_micros_*` measures SQL and physical-plan creation for each query,
  including Delta scan planning. Its percentiles include every query from every
  repetition.
- `total_micros_*` measures planning and execution for each query, but excludes
  table initialization and registration.
- `session_total_micros_*` starts immediately before table loading and ends
  after all benchmark queries, output checks, and metrics collection finish. It
  includes initialization, registration, planning, and execution. Its
  percentiles use one complete session per repetition.

Before comparing timings, confirm that both CSV rows have the same
`fixture_fingerprint`, `seed`, workload, query, storage, backend, scheduling,
repetition count, and query count. `scan_metadata_mode` should be the only
lifecycle setting that differs.

Eager mode moves Delta log/checkpoint replay into table initialization, so first
compare `table_initialization_micros_*` and `planning_micros_*` to confirm that
the work moved between phases. Then compare `session_total_micros_*` to judge
the end-to-end effect for the chosen number of queries. The per-query
`total_micros_*` fields help show whether execution time masks a planning-time
difference.

Do not treat one query count as a universal break-even point. The result depends
on Delta history and checkpoint shape, active-file count, object-store latency,
query selectivity, available statistics, memory pressure, hardware load, and
the number of queries that reuse the loaded table. The local fixture above is a
reproducible comparison of the two lifecycles; measure representative tables on
their real storage before choosing a production default.

## Representative real-S3 result

On August 27, 2026, we also compared the modes against the latest snapshots of
two production-shaped tables in S3. This used the direct Parquet backend through
DataFusion, the unreleased eager-metadata implementation at commit `a6adb83`,
16 target partitions, an 8,192-row batch size, and the same machine as the
[reader comparison](../benchmarks.md#environment). One warmup pair was
discarded. Four measured pairs alternated whether lazy or eager mode ran first;
each isolated process loaded both tables once, then ran the HIP and schedule
queries three times each.
Table initialization and complete-session values below are medians across the
four processes for each mode. Planning values are medians across the 12 query
executions for each table and mode.

| Measurement, median | Lazy | Eager | Difference |
| --- | ---: | ---: | ---: |
| Table initialization | 1.196 s | 4.089 s | +2.894 s |
| Complete six-query session | 50.205 s | 42.451 s | -7.754 s (-15.4%) |
| HIP physical planning | 2.097 s | 5.1 ms | -99.76% |
| Schedule physical planning | 756 ms | 1.1 ms | -99.86% |

Both modes returned the same results and performed the same Parquet I/O. Each
HIP query returned 15,252,906 rows in 2,483 batches and read 505,642,366 bytes
with 1,044 full-file GETs. Each schedule query returned 58,161 rows in 12
batches and read 1,217,686 bytes with four range GETs and four full-file GETs.

We measured memory separately because allocator state from a completed query can
hide the retained-cache cost. Across six counterbalanced initialization-only
pairs, loading both tables increased median resident memory from 38.5 MiB to
68.7 MiB, a 30.2 MiB increase. Across four counterbalanced pairs that each ran
one HIP and one schedule query, median peak resident memory increased from
233.9 MiB to 248.9 MiB, a 15.0 MiB or 6.4% increase. These Linux measurements
used `VmRSS` and `VmHWM` from `/proc/self/status`.

This is a dated case study, not a universal performance promise or a pinned
public fixture. The live table snapshots can advance. Cache memory scales with
active-file metadata, statistics width, partition values, and deletion-vector
metadata rather than table row count or total Parquet data size.
