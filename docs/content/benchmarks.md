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
| Mixed-column projection | 0.873 s | 1.143 s | 12.246 s | 3.152 s | Unsupported |
| 3 GB text projection | 2.254 s | 2.584 s | 13.063 s | 3.866 s | 4.750 s |
| Read one row from a table with deletion vectors | 0.0269 s | 0.0820 s | 0.3920 s | 0.5371 s | Unsupported |
| Scan a full table with deletion vectors | 0.1548 s | 1.0834 s | 1.0818 s | 0.8838 s | Unsupported |

Compared with Delta Arrow Reader, Polars took 3.61 times as long on the
mixed-column projection, 1.71 times as long on the text projection, 19.95 times
as long on the one-row deletion-vector read, and 5.71 times as long on the full
deletion-vector scan. Daft took 2.11 times as long on the one workload it could
run.

On the text projection, Delta Arrow Reader took 2.206-2.791 seconds and delta-rs
took 2.239-4.902 seconds. Because those ranges overlap, we do not treat the
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
| Mixed-column projection | 6.53 s / 334 MiB | 6.09 s / 293 MiB | 10.25 s / 721 MiB | 13.72 s / 1,359 MiB | Unsupported |
| 3 GB text projection | 18.17 s / 1,760 MiB | 16.33 s / 1,677 MiB | 31.07 s / 5,186 MiB | 18.87 s / 3,209 MiB | 43.65 s / 6,263 MiB |
| Read one row with deletion vectors | 0.051 s / 58 MiB | 0.275 s / 266 MiB | 0.816 s / 306 MiB | 0.942 s / 447 MiB | Unsupported |
| Full deletion-vector scan | 1.04 s / 67 MiB | 2.27 s / 266 MiB | 1.39 s / 316 MiB | 1.81 s / 580 MiB | Unsupported |

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

Delta Arrow Reader and delta-rs used the same Rust harness with DataFusion
54.1.0 and Arrow/Parquet 58.4.0. DuckDB, Polars, and Daft each ran in a separate
Python process. Their measured time covers the path from opening the Delta
table to producing Arrow batches, but not interpreter startup or imports. Linux
process accounting recorded wall time, CPU use, memory use, page faults, and
context switches.

All 146 measured processes completed successfully. Their row counts and logical
schemas matched the frozen workload expectations. Both deletion vectors in the
reference table removed the same 43 rows, and the generated table removed the
expected 200 rows.

The readers did not always represent values in memory in exactly the same way.
We allowed differences in string encoding, timestamp units, dictionary
encoding, and where each reader split its Arrow batches because none of them
changed the logical result.

Daft was not timed on the mixed-column or deletion-vector workloads. A separate
capability probe confirmed that Daft 0.7.24 rejects tables that advertise the
Delta deletion-vector reader feature. We did not bypass that check or ask Daft
to ignore deleted rows.

## Environment

We ran the benchmark on August 27, 2026, using one machine with the following
hardware and software:

- AMD Ryzen 7 8845HS, with 8 cores and 16 threads
- 27.95 GiB of memory
- Crucial P3 Plus NVMe SSD
- Fedora Linux 43 with Linux 6.19.14

| Reader | Version tested |
| --- | --- |
| Delta Arrow Reader | 0.3.0 (`aaf6699`) |
| delta-rs | `fd7e96910243f9e67b4eae994d52ef246cfcea38` |
| DuckDB | 1.5.5, Delta extension `45c4087` |
| Polars | 1.44.1, with deltalake 1.6.3 |
| Daft | 0.7.24, with deltalake 1.6.3 |

We kept all benchmark data in local MinIO so that public-network delays would
not affect the results. Hardware and system load still affect absolute times,
so we will rerun these comparisons when the readers or their execution settings
change.

## Focused benchmarks

- [Lazy and eager scan metadata](benchmarks/eager-metadata.md) compares when
  Delta scan metadata is loaded and reused across repeated queries.
- [Parquet row-filter predicate decoding](benchmarks/row-filter.md) compares
  decoding only predicate columns with decoding unrelated columns as well.
- [Parquet page-index range reads](benchmarks/page-index.md) measures page-level
  reads for localized and scattered row-filter matches.
