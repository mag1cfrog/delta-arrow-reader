# Prepared Parquet metadata

This benchmark compares fetching Parquet metadata during each query with
preparing it once when the table loads. Both modes use eager Delta scan metadata
so the comparison isolates the Parquet metadata lifecycle.

## Run a matched pair

Enable all features and keep every argument except the Parquet metadata mode
and output path identical:

```bash
cargo bench --locked -p delta-arrow-reader --bench reader --all-features -- \
  --mode provider-exec \
  --seed 0 \
  --output target/parquet-metadata-on-demand.csv \
  --provider-exec-temp-dir target/parquet-metadata-bench \
  --provider-exec-storage-profile s3_throttled \
  --provider-exec-workload provider_many_unequal_files \
  --provider-exec-query filter_tail_ids \
  --provider-exec-backend direct-parquet \
  --provider-exec-scan-metadata-mode eager \
  --provider-exec-parquet-metadata-mode on-demand \
  --provider-exec-queries-per-table 10 \
  --provider-exec-scheduling-profile prefetch_2_ap_target_scan_3x \
  --provider-exec-parquet-metadata-size-hint-bytes 65536 \
  --provider-exec-parquet-full-file-read-threshold-bytes disabled \
  --provider-exec-repetitions 5

cargo bench --locked -p delta-arrow-reader --bench reader --all-features -- \
  --mode provider-exec \
  --seed 0 \
  --output target/parquet-metadata-prepared.csv \
  --provider-exec-temp-dir target/parquet-metadata-bench \
  --provider-exec-storage-profile s3_throttled \
  --provider-exec-workload provider_many_unequal_files \
  --provider-exec-query filter_tail_ids \
  --provider-exec-backend direct-parquet \
  --provider-exec-scan-metadata-mode eager \
  --provider-exec-parquet-metadata-mode prepared \
  --provider-exec-queries-per-table 10 \
  --provider-exec-scheduling-profile prefetch_2_ap_target_scan_3x \
  --provider-exec-parquet-metadata-size-hint-bytes 65536 \
  --provider-exec-parquet-full-file-read-threshold-bytes disabled \
  --provider-exec-repetitions 5
```

Each repetition loads and registers a new table, then runs the configured query
against it. The harness checks the result rows, schema, and fixture fingerprint
before writing the CSV.

The `s3_throttled` profile uses 15 ms object-open latency, 8 ms read latency,
and 32 MiB/s bandwidth. It is a deterministic local HTTP model, not a claim
about every S3 deployment.

For prepared runs, the harness sets the file limit to the fixture's exact file
count and leaves the estimated-memory limit unrestricted. This keeps a
benchmark limit from ending a repetition early; it is not a recommended
production configuration.

## Read the measurements

The main fields are:

- `parquet_metadata_preparation_micros_*`: time spent fetching and parsing
  metadata after Delta scan planning.
- `prepared_parquet_metadata_file_count_p50`: unique files prepared.
- `prepared_parquet_metadata_memory_bytes_p50`: estimated retained size of the
  parsed metadata.
- `table_initialization_micros_*`: complete table loading, including
  preparation when selected.
- `total_micros_*`: per-query planning and execution, excluding table loading.
- `session_total_micros_*`: table loading followed by all configured queries.

Preparation request and byte fields report the I/O moved into initialization.
The normal provider read fields report the I/O that remains in each query.

Before comparing timings, confirm that both rows have the same fixture
fingerprint, seed, workload, query, storage profile, backend, scheduling
profile, repetition count, and query count.

## Controlled results

The following results were measured on August 29, 2026. The machine had 16
logical CPUs, and the harness therefore used 16 target partitions. Each value
is the median of five repetitions.

| Workload | Preparation | Query, on demand | Query, prepared | Estimated break-even | Retained metadata |
| --- | ---: | ---: | ---: | ---: | ---: |
| Four files, 32,768 rows, full scan | 31.4 ms | 69.1 ms | 35.7 ms | About 1 query | 9.7 KiB |
| 64 mixed-size files, selective scan | 60.5 ms | 107.6 ms | 77.3 ms | About 2 queries | 154.5 KiB |
| 4,096 one-row files, projected scan | 2.271 s | 6.685 s | 6.565 s | About 19 queries | 9.65 MiB |

The break-even estimate divides preparation time by the median per-query time
saved. It does not include memory cost or assume that later queries select the
same files.

For ten queries against one loaded table:

| Workload | On-demand session | Prepared session | Difference |
| --- | ---: | ---: | ---: |
| Four files, full scan | 766.2 ms | 498.1 ms | -268.1 ms (-35.0%) |
| 64 mixed-size files, selective scan | 1.175 s | 979.8 ms | -195.6 ms (-16.6%) |

All matched modes returned the same rows and fixture fingerprints.

## Interpret the result

The four-file and 64-file workloads recover preparation cost after a small
number of queries under the controlled remote profile. The 4,096-file workload
shows why preparation is opt-in: startup spent about 2.27 seconds preparing
metadata while one query improved by about 120 ms.

The fixtures have two columns and one row group per file. Production metadata
can be larger when schemas are wider or files contain more row groups,
statistics, and page indexes. The 4,096 files are also smaller than the 64 KiB
footer hint, so that case isolates per-file overhead rather than representing a
typical Parquet table.

Choose production limits from measurements on representative tables and their
real object stores. The synthetic break-even counts above are not production
defaults.
