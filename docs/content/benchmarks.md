# Benchmarks

We compared Delta Arrow Reader with delta-rs, DuckDB, Polars, and Daft on four
workloads. Delta Arrow Reader had the lowest median time on every workload an
alternative could run. Polars completed all four workloads. Daft completed the
text projection but rejected the other three because it does not support
deletion vectors.

These numbers describe the workloads and machine shown below. Different tables,
queries, and hardware can produce different results.

## Results

The table below shows how long each reader took to load the Delta snapshot and
stream the full result. Process startup and Python import time are not included.
Each value is the median after one warmup run.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-wall-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-wall-light.svg">
  <img alt="Wall-time comparison across five Delta readers and four workloads" src="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-wall-light.svg">
</picture>

| Workload | Delta Arrow Reader | delta-rs | DuckDB | Polars | Daft |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mixed-column projection | 0.809 s | 1.084 s | 10.580 s | 2.742 s | Unsupported |
| 3 GB text projection | 2.555 s | 2.736 s | 13.003 s | 3.971 s | 4.984 s |
| Read one row from a table with deletion vectors | 0.0220 s | 0.0820 s | 0.2612 s | 0.4991 s | Unsupported |
| Scan a full table with deletion vectors | 0.1161 s | 0.5572 s | 0.8777 s | 0.8578 s | Unsupported |

delta-rs took 1.34 times as long on the mixed-column projection, 3.73 times as
long on the one-row deletion-vector read, and 4.80 times as long on the full
deletion-vector scan.

Compared with Delta Arrow Reader, Polars took 3.39 times as long on the
mixed-column projection, 1.55 times as long on the text projection, 22.7 times
as long on the one-row deletion-vector read, and 7.39 times as long on the full
deletion-vector scan. Daft took 1.95 times as long on the one workload it could
run.

On the text projection, Delta Arrow Reader took 2.252-2.739 seconds and delta-rs
took 2.447-2.969 seconds. Because those ranges overlap, we do not treat the
difference between their medians as conclusive. The Delta Arrow Reader range
did not overlap the Polars or Daft range on that workload.

Speed is only part of the picture. The charts and table below show median CPU
time and median peak memory use:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-cpu-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-cpu-light.svg">
  <img alt="CPU-time comparison across five Delta readers and four workloads" src="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-cpu-light.svg">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-memory-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-memory-light.svg">
  <img alt="Peak-memory comparison across five Delta readers and four workloads" src="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/main/docs/content/assets/reader-benchmark-memory-light.svg">
</picture>

| Workload | Delta Arrow Reader | delta-rs | DuckDB | Polars | Daft |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mixed-column projection | 6.42 s / 325 MiB | 5.95 s / 303 MiB | 8.87 s / 716 MiB | 12.77 s / 1,339 MiB | Unsupported |
| 3 GB text projection | 15.59 s / 1,539 MiB | 13.55 s / 1,362 MiB | 30.92 s / 5,201 MiB | 18.12 s / 3,146 MiB | 44.98 s / 6,186 MiB |
| Read one row with deletion vectors | 0.046 s / 57 MiB | 0.292 s / 270 MiB | 0.730 s / 306 MiB | 0.869 s / 445 MiB | Unsupported |
| Full deletion-vector scan | 1.02 s / 64 MiB | 1.86 s / 269 MiB | 1.24 s / 315 MiB | 1.66 s / 561 MiB | Unsupported |

Each cell shows CPU time followed by peak memory. Delta Arrow Reader used
slightly more CPU and memory than delta-rs on the two projection workloads. It
used less memory than DuckDB, Polars, and Daft on every comparable workload. On
the deletion-vector workloads, it also used less CPU and memory than every
alternative.

## Workloads

The mixed-column workload is based on a real application query. It reads 28
string, numeric, boolean, and time columns from a 14.5-million-row table. It
then reads 14 columns from a small reference table with two deletion vectors.
The larger table contains 1,151 active Parquet files and 448 MB of compressed
data.

The original schema is private, so the published
[query shapes](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/query_shapes.sql)
use neutral table and column names. They preserve the number and types of
projected columns without revealing the source data.

The text workload reads six columns from the public version-4 RedPajama
StackExchange Delta snapshot. It contains 64 Parquet files, 3.03 GB of
compressed data, and 2,697,774 rows.

Deletion vectors mark rows as deleted without rewriting their Parquet files.
To test them, we generated a table with 200 files and 200 million physical
rows. Each file has one deleted row. One query stops after the first available
row. The other reads all 199,999,800 rows that remain. Together, these queries
show both the startup cost of handling deletion vectors and the cost of applying
them across a full scan.

## Method

All readers used the same Delta snapshots stored in local MinIO. Each reader
used the machine's 16 logical CPUs. Results were consumed as Arrow batches
without retaining the full result, and output batches were limited to 8,192
rows.

For each workload, we ran every supported reader once to warm up the system and
discarded those results. We then used balanced, process-isolated run orders:
eight measured rounds for each four-reader workload and ten rounds for the
five-reader text workload. Every reader appeared in every position twice, and
every ordered pair of adjacent readers appeared twice. The exact
[run-order generator](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/run_order.py)
is checked in.

Delta Arrow Reader and delta-rs used two builds of the same Rust harness. Each
reader kept the DataFusion version from its released dependency graph: 54.1.0
for Delta Arrow Reader and 53.1.0 for delta-rs. Both builds used Arrow and
Parquet 58.4.0. DuckDB, Polars, and Daft each ran in a separate Python process
and returned batches through PyArrow 25.0.1.

Wall time covers table opening through consumption of the final Arrow batch.
The timer starts after process startup and, for the Python readers, after
imports. CPU time and peak RSS come from Linux process accounting around the
whole child process, so those figures include startup and imports.

All 146 measured processes completed successfully. Their row counts and logical
schemas matched the frozen workload expectations. Both deletion vectors in the
reference table removed the same 43 rows, and the generated table removed the
expected 200 rows.

The [anonymized per-run results](benchmarks/reader-results.csv) include the
timing, resource use, and parity outcome for all 146 measured processes, along
with the three unsupported Daft probes. The chart generator validates the file
and computes the plotted medians.

The readers did not always represent values in memory in exactly the same way.
We allowed differences in string encoding, timestamp units, dictionary
encoding, and where each reader split its Arrow batches because none of them
changed the logical result.

Daft was not timed on the mixed-column or deletion-vector workloads. A separate
capability probe confirmed that Daft 0.7.24 rejects tables that advertise the
Delta deletion-vector reader feature. We did not bypass that check or ask Daft
to ignore deleted rows.

## Environment

We ran the benchmark on September 1, 2026, using one machine with the following
hardware and software:

- AMD Ryzen 7 8845HS, with 8 cores and 16 threads
- 27.95 GiB of memory
- Crucial P3 Plus NVMe SSD
- Fedora Linux 43 with Linux 6.19.14

| Reader | Version tested |
| --- | --- |
| Delta Arrow Reader | 0.6.0 (`7a1168e`) |
| delta-rs | 0.32.4 |
| DuckDB | 1.5.5, Delta extension `45c4087` |
| Polars | 1.44.1, with deltalake 1.6.3 |
| Daft | 0.7.24, with deltalake 1.6.3 |

We kept all benchmark data in local MinIO so that public-network delays would
not affect the results. Hardware and system load still affect absolute times,
so we will rerun these comparisons when the readers or their execution settings
change.

## Focused benchmarks

- [Selective S3 case study](benchmarks/selective-s3.md) compares four existing
  selective queries across Lakehouse//RT Small (Beta), Serverless SQL Small,
  Delta Arrow Reader, and delta-rs, with every anonymized repetition available.
- [Lazy and eager scan metadata](benchmarks/eager-metadata.md) compares when
  Delta scan metadata is loaded and reused across repeated queries.
- [Parquet row-filter predicate decoding](benchmarks/row-filter.md) compares
  decoding only predicate columns with decoding unrelated columns as well.
- [Parquet page-index range reads](benchmarks/page-index.md) measures page-level
  reads for localized and scattered row-filter matches.
- [Adaptive Parquet range planning](benchmarks/range-planning.md) compares
  exact and merged byte-range reads under controlled transport conditions.
