# Benchmarks

We compared Delta Arrow Reader with delta-rs and DuckDB on four workloads.
Delta Arrow Reader was faster on the mixed-column projection and both
deletion-vector scans. It was effectively tied with delta-rs on the large text
projection, while DuckDB was slower in all four tests.

These numbers describe the workloads and machine shown below. Different tables,
queries, and hardware can produce different results.

## Results

The table below shows how long each reader took to load the Delta snapshot and
stream the full result. Process startup is not included. Each value is the
median of six runs, after one warmup run.

| Workload | Delta Arrow Reader | delta-rs | DuckDB |
| --- | ---: | ---: | ---: |
| Mixed-column projection | 0.801 s | 1.023 s | 10.467 s |
| 3 GB text projection | 2.655 s | 2.620 s | 13.033 s |
| Read one row from a table with deletion vectors | 0.0223 s | 0.0803 s | 0.2616 s |
| Scan a full table with deletion vectors | 0.112 s | 0.811 s | 0.882 s |

On the text projection, Delta Arrow Reader took 2.226-3.244 seconds and delta-rs
took 2.264-2.839 seconds. Because those ranges overlap, we treat the 1.3%
difference between their medians as a tie. On the other three workloads, even
the slowest Delta Arrow Reader run was faster than the fastest run from either
alternative.

Speed is only part of the picture. The next table shows median CPU time and
peak memory use:

| Workload | Delta Arrow Reader | delta-rs | DuckDB |
| --- | ---: | ---: | ---: |
| Mixed-column projection | 6.610 s / 325 MiB | 6.028 s / 297 MiB | 8.763 s / 713 MiB |
| 3 GB text projection | 14.663 s / 1,472 MiB | 11.318 s / 1,392 MiB | 30.357 s / 5,208 MiB |
| Read one row from a table with deletion vectors | 0.0431 s / 66 MiB | 0.2933 s / 278 MiB | 0.7368 s / 309 MiB |
| Scan a full table with deletion vectors | 0.967 s / 74 MiB | 1.867 s / 276 MiB | 1.246 s / 320 MiB |

Each cell shows CPU time followed by peak memory. Delta Arrow Reader used more
CPU and memory than delta-rs on the two projection workloads. On the
deletion-vector workloads, it was faster and used less memory.

## Workloads

The mixed-column workload is based on a real application query. It reads 28
string, numeric, boolean, and time columns from a 14.5-million-row table. It
then reads 14 columns from a small reference table with two deletion vectors.
The larger table contains 1,151 active Parquet files and 448 MB of compressed
data.

The original schema is private, so the published
[query shapes](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benchmarks/query_shapes.sql)
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

All three readers used the same Delta snapshots stored in local MinIO. Each
reader used 16-way parallelism and streamed Arrow batches of 8,192 rows without
keeping the full result in memory.

For each workload, we ran every reader once to warm up the system and discarded
those results. We then ran six measured rounds, changing the order each time so
that every reader appeared first, second, and third twice. The exact
[run order](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benchmarks/run_order.py)
is checked in.

Delta Arrow Reader and delta-rs ran in the same Rust program with DataFusion
54.1.0 and Arrow/Parquet 58.4.0. DuckDB ran in a separate Python process because
it uses its own query engine. Its time covers the full path from opening the
Delta table to producing Arrow batches. Linux process accounting recorded wall
time, CPU use, memory use, page faults, and context switches.

All 72 measured processes completed successfully: four workloads, six rounds,
and three readers. The row counts matched. Both deletion vectors in the
reference table removed the same 43 rows, and the generated table removed the
expected 200 rows.

Each result also had the same logical columns and data types. The readers did
not always represent those values in memory in exactly the same way. We allowed
differences in string encoding, timestamp units, and where each reader split
its Arrow batches because none of them changed the result.

## Environment

We ran the benchmark on August 26, 2026, using one machine with the following
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

We kept all benchmark data in local MinIO so that public-network delays would
not affect the results. Hardware and system load still affect absolute times,
so we will rerun these comparisons when the readers or their execution settings
change.
