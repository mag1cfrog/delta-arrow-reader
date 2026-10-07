# Compare lazy and eager metadata loading

Loading scan metadata once can reduce planning time for repeated queries, but
it takes longer to initialize the table and retains metadata in memory. The
useful comparison is the total time for a session, including initialization.

The generated local fixture below compares lazy and eager metadata loading on
the same table and queries. Change only the metadata mode and output path:

## Run the comparison

Run these commands from the repository root.

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

## Read the measurements

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

For the public repeated-query results and the limits of that comparison, see
[repeated queries](../../public/benchmarks/eager-metadata.md).
