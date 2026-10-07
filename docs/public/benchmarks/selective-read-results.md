---
title: Selective reads on wide tables
description: Compare five Delta Lake readers on public tables with about 60 million rows, two table widths, and snapshots with and without deletion vectors.
---

# Selective reads on wide tables

We compared Delta Arrow Reader (DAR), delta-rs, DuckDB, Polars, and
single-machine Spark on filtered queries against Delta tables containing about
60 million rows. Each query returned about 900 rows. The 416-column tables
returned 69 selected columns; the 90-column tables returned 71.

Grouped matches are stored close together; scattered matches are spread out.
We tested both layouts with and without deletion vectors (DVs), which mark
deleted rows without rewriting the data files. These combinations give eight
test cases. A table snapshot is the table as it exists at a particular version.

All five readers completed every case, both when opening a table for one query
and when reusing it for a second query. Each returned exactly the expected
values before timing began. The results contain 400 independent runs, with five
samples for each reader, case, and way of running the query.

The inputs use public TPC-H lineitem data at scale factor 10 (SF10), extended
with synthetic numeric columns. These are custom scan queries, not standard
TPC-H queries or a TPC-H score. See [data and reproduction](#data-and-reproduction)
for results, the evidence archive, and reproduction instructions.

## Open a table and read the result

All readers ran on the same machine, with four physical CPU cores and an
8 GiB memory limit per reader. Storage used an emulated connection with
200 ms request latency (+/-20 ms jitter) and a shared 150 Mbps bandwidth limit.
These conditions affect the balance between request counts and downloaded
bytes; they do not represent a particular cloud service. See
[test conditions](#test-conditions) for the full setup.

Times are medians of five independent runs, in seconds; lower is faster.
Timing includes opening the selected table version and reading the complete
result. Process startup, Python imports, and Spark session/JVM startup are
excluded. Expand the table below the results to see the quartiles.

| Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416, grouped | 5.595 | 11.584 | 12.856 | 11.454 | 16.357 |
| 416, scattered | 11.378 | 11.773 | 13.031 | 11.605 | 19.868 |
| 416, grouped + DV | 6.181 | 71.563 | 14.968 | 66.348 | 19.066 |
| 416, scattered + DV | 11.968 | 72.086 | 14.900 | 66.286 | 21.332 |
| 90, grouped | 6.990 | 22.966 | 23.816 | 22.861 | 16.433 |
| 90, scattered | 22.949 | 23.471 | 24.388 | 23.027 | 31.456 |
| 90, grouped + DV | 7.329 | 178.588 | 25.091 | 166.613 | 19.822 |
| 90, scattered + DV | 23.267 | 179.729 | 25.600 | 167.075 | 32.477 |

??? details "Full timings and quartiles: open and read"

    Values are `median [p25, p75]` in seconds. The quartiles describe
    the middle half of the five independent samples.

    | Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | 416 columns, grouped | 5.595 [5.586, 5.605] | 11.584 [11.577, 11.585] | 12.856 [12.853, 12.857] | 11.454 [11.452, 11.464] | 16.357 [16.342, 16.368] |
    | 416 columns, scattered | 11.378 [11.376, 11.382] | 11.773 [11.772, 11.773] | 13.031 [13.030, 13.038] | 11.605 [11.574, 11.642] | 19.868 [19.772, 19.903] |
    | 416 columns, grouped + DV | 6.181 [6.153, 6.348] | 71.563 [71.559, 71.566] | 14.968 [14.968, 14.971] | 66.348 [66.347, 66.411] | 19.066 [19.041, 19.152] |
    | 416 columns, scattered + DV | 11.968 [11.952, 11.973] | 72.086 [72.086, 72.098] | 14.900 [14.892, 14.903] | 66.286 [66.267, 66.288] | 21.332 [21.234, 21.376] |
    | 90 columns, grouped | 6.990 [6.957, 7.035] | 22.966 [22.963, 22.967] | 23.816 [23.786, 23.851] | 22.861 [22.859, 23.010] | 16.433 [16.402, 16.441] |
    | 90 columns, scattered | 22.949 [22.946, 22.952] | 23.471 [23.466, 23.481] | 24.388 [24.384, 24.398] | 23.027 [23.022, 23.039] | 31.456 [31.398, 31.472] |
    | 90 columns, grouped + DV | 7.329 [7.307, 7.337] | 178.588 [178.579, 178.591] | 25.091 [25.083, 25.098] | 166.613 [166.254, 167.018] | 19.822 [19.795, 19.824] |
    | 90 columns, scattered + DV | 23.267 [23.267, 23.288] | 179.729 [179.718, 179.735] | 25.600 [25.557, 25.607] | 167.075 [166.865, 167.464] | 32.477 [32.413, 32.490] |

Without DVs, delta-rs took 2.07 times as long as DAR on the grouped 416-column
table and 3.29 times as long on the grouped 90-column table. The scattered
cases were close, at 1.03 and 1.02 times as long. Polars was also close to DAR
on both scattered cases without DVs.

With DVs, delta-rs took 6.02-24.37 times as long as DAR. DuckDB and Spark
completed all four DV cases with smaller time differences. Every reader touched
the same Parquet files in each case; the differences concern traffic within
those files and request behavior, rather than how many files were skipped.

## Repeat a query

Each run opens a table once, then plans and runs the same query twice using
that reader's reusable table object. Query results are not cached. Each query
position has five independent samples; the two queries within one process are
not independent samples.

Median time for the second query, in seconds:

| Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416, grouped | 3.768 | 9.896 | 10.825 | 10.224 | 7.501 |
| 416, scattered | 10.125 | 10.149 | 10.923 | 10.297 | 11.176 |
| 416, grouped + DV | 4.067 | 67.778 | 12.110 | 64.460 | 9.345 |
| 416, scattered + DV | 10.350 | 68.345 | 12.075 | 64.966 | 11.795 |
| 90, grouped | 4.300 | 21.670 | 22.460 | 22.140 | 8.407 |
| 90, scattered | 22.063 | 22.122 | 23.010 | 22.109 | 23.020 |
| 90, grouped + DV | 4.508 | 175.209 | 23.684 | 165.890 | 10.852 |
| 90, scattered + DV | 22.349 | 176.236 | 24.198 | 166.514 | 23.670 |

??? details "Full timings and quartiles: second query"

    Values are `median [p25, p75]` in seconds. The quartiles describe
    the middle half of the five independent samples.

    | Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | 416 columns, grouped | 3.768 [3.766, 3.772] | 9.896 [9.895, 9.897] | 10.825 [10.817, 10.826] | 10.224 [10.223, 10.224] | 7.501 [7.496, 7.526] |
    | 416 columns, scattered | 10.125 [10.120, 10.125] | 10.149 [10.143, 10.156] | 10.923 [10.915, 10.925] | 10.297 [10.296, 10.309] | 11.176 [11.164, 11.190] |
    | 416 columns, grouped + DV | 4.067 [4.055, 4.096] | 67.778 [67.767, 67.792] | 12.110 [12.108, 12.111] | 64.460 [64.259, 64.729] | 9.345 [9.327, 9.352] |
    | 416 columns, scattered + DV | 10.350 [10.349, 10.354] | 68.345 [68.335, 68.354] | 12.075 [12.074, 12.077] | 64.966 [64.862, 65.234] | 11.795 [11.777, 11.813] |
    | 90 columns, grouped | 4.300 [4.299, 4.302] | 21.670 [21.669, 21.670] | 22.460 [22.424, 22.477] | 22.140 [22.139, 22.197] | 8.407 [8.356, 8.437] |
    | 90 columns, scattered | 22.063 [22.061, 22.063] | 22.122 [22.116, 22.122] | 23.010 [23.002, 23.027] | 22.109 [22.108, 22.111] | 23.020 [23.005, 23.033] |
    | 90 columns, grouped + DV | 4.508 [4.506, 4.510] | 175.209 [175.197, 175.251] | 23.684 [23.672, 23.684] | 165.890 [165.332, 166.131] | 10.852 [10.840, 10.871] |
    | 90 columns, scattered + DV | 22.349 [22.328, 22.360] | 176.236 [176.236, 176.252] | 24.198 [24.175, 24.200] | 166.514 [166.413, 166.594] | 23.670 [23.614, 23.672] |

??? details "Initialization plus the first query"

    These times include initialization, so they show the cost paid before a
    source can be reused. Values are `median [p25, p75]` in seconds.

    | Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | 416 columns, grouped | 5.595 [5.585, 5.609] | 11.596 [11.595, 11.601] | 12.047 [12.045, 12.049] | 11.454 [11.449, 11.461] | 16.308 [16.299, 16.545] |
    | 416 columns, scattered | 11.388 [11.387, 11.390] | 11.784 [11.783, 11.785] | 12.200 [12.196, 12.201] | 11.571 [11.571, 11.573] | 19.842 [19.815, 19.888] |
    | 416 columns, grouped + DV | 6.326 [6.291, 6.359] | 71.587 [71.587, 71.590] | 13.758 [13.757, 13.762] | 66.244 [66.051, 66.310] | 19.110 [19.090, 19.180] |
    | 416 columns, scattered + DV | 11.977 [11.957, 11.987] | 72.094 [72.086, 72.096] | 13.699 [13.693, 13.701] | 66.606 [66.462, 66.621] | 21.370 [21.364, 21.371] |
    | 90 columns, grouped | 7.036 [7.035, 7.040] | 22.973 [22.968, 22.977] | 23.315 [23.260, 23.329] | 22.861 [22.859, 22.863] | 16.428 [16.381, 16.434] |
    | 90 columns, scattered | 22.954 [22.949, 22.957] | 23.478 [23.470, 23.479] | 23.897 [23.891, 23.897] | 23.060 [23.052, 23.077] | 31.264 [31.249, 31.270] |
    | 90 columns, grouped + DV | 7.307 [7.306, 7.416] | 178.621 [178.613, 178.643] | 24.624 [24.616, 24.630] | 167.006 [166.863, 167.403] | 19.837 [19.704, 19.964] |
    | 90 columns, scattered + DV | 23.280 [23.271, 23.291] | 179.738 [179.709, 179.837] | 25.082 [25.037, 25.115] | 166.921 [166.914, 167.152] | 32.529 [32.356, 32.563] |

See [repeated queries](eager-metadata.md) for DAR's first- and second-query
times and how to interpret reuse savings.

Spark consumes its full result through `DataFrame.toArrow()`, so it has no
comparable streaming first-batch timestamp. The [raw timings](selective-read-timings.csv)
also include startup, initialization, cleanup, and whole-process measurements.
Blank fields mean unavailable, not zero.

## Requests and downloaded data

These measurements come from separate untimed diagnostic runs. Each
cell shows response-body MiB / request count for one open-and-read invocation,
including Delta logs, listings, Parquet, deletion vectors, and other requests.
One MiB is 1,048,576 bytes.

| Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 columns, grouped | 66.92 / 334 | 170.51 / 56 | 179.54 / 42 | 176.34 / 44 | 71.63 / 452 |
| 416 columns, scattered | 171.59 / 44 | 174.31 / 58 | 180.61 / 42 | 177.42 / 44 | 176.23 / 132 |
| 416 columns, grouped + DV | 73.67 / 343 | 1137.81 / 56 | 196.42 / 52 | 1137.93 / 311 | 78.41 / 506 |
| 416 columns, scattered + DV | 178.34 / 53 | 1145.47 / 56 | 197.48 / 52 | 1145.60 / 311 | 183.01 / 186 |
| 90 columns, grouped | 90.40 / 400 | 372.47 / 66 | 379.45 / 43 | 383.57 / 70 | 74.19 / 541 |
| 90 columns, scattered | 380.50 / 58 | 382.00 / 66 | 382.00 / 55 | 386.24 / 70 | 383.08 / 149 |
| 90 columns, grouped + DV | 91.35 / 408 | 2919.27 / 114 | 381.80 / 54 | 2919.57 / 492 | 75.19 / 605 |
| 90 columns, scattered + DV | 381.45 / 67 | 2939.38 / 114 | 384.35 / 66 | 2939.68 / 492 | 384.08 / 213 |

Every reader touched the same five of 130 Parquet files for the 416-column table
and six of 60 for the 90-column table. File statistics identified exactly these
files as possible matches, and all contained matching rows that had not been
deleted. That means 96.2% and 90.0% of files were skipped. DVs did not change
those file sets.

For grouped matches in the 90-column table without DVs, DAR downloaded
90.40 MiB in 400 requests, while delta-rs downloaded 372.47 MiB in 66 requests.
Spark downloaded less than DAR, at 74.19 MiB, but took longer and made
541 requests. With scattered matches, all readers downloaded 380.5-386.2 MiB
and the timing gap narrowed. Bytes alone do not explain latency.

The [I/O data](selective-read-io.csv) separates log, Parquet, listing, and DV
traffic for all 80 reader/profile entries. Reuse measurements cover initialization
and both queries together. File and page geometry describes opportunities to
skip data; it does not measure how many pages an engine decoded.

## Tables and queries

Each base table contains all 59,986,052 SF10 source rows. Parquet divides a
file into row groups, then divides each group's column data into pages. In the
grouped layout, matches sit close together within each row group. The scattered
layout reorders rows within each group, preserving values and file/group
membership. A DV snapshot shares its base table's Parquet bytes and deletes one
of the 895 matching rows.

| Table / matches | Files | Stored columns | Output columns | Parquet GiB | Matching rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 columns, grouped | 130 | 416 | 69 | 65.89 | 895 |
| 416 columns, scattered | 130 | 416 | 69 | 66.09 | 895 |
| 416 columns, grouped + DV | 130 | 416 | 69 | 65.89 | 894 |
| 416 columns, scattered + DV | 130 | 416 | 69 | 66.09 | 894 |
| 90 columns, grouped | 60 | 90 | 71 | 30.04 | 895 |
| 90 columns, scattered | 60 | 90 | 71 | 30.23 | 895 |
| 90 columns, grouped + DV | 60 | 90 | 71 | 30.04 | 894 |
| 90 columns, scattered + DV | 60 | 90 | 71 | 30.23 | 894 |

Both queries apply this `WHERE` filter (the predicate) and read all selected
columns and matching rows, without `COUNT` or `LIMIT`:

```sql
WHERE l_shipdate = DATE '1995-03-15'
  AND l_shipmode = 'AIR'
  AND l_linenumber IN (1)
```

The 416-column query returns five source columns and 64 nullable numeric
payloads. The 90-column query also returns `l_suppkey` and `l_quantity`.
The [provenance data](selective-read-provenance.json) contains the full SQL,
projection lists, fixture identities, file sizes, and writer settings.

The writer targets 512 MiB files with Zstd level 3, plain encoding, and no
dictionaries. Row groups contain at most 131,072 rows: four groups per file
for the 416-column table and eight for the 90-column table. Page settings are
20,000 rows, 1 MiB, and 1,024-row write batches. Checking the row limit at batch
boundaries can produce pages with 20,480 rows.

## Test conditions

| Setting | Recorded value |
| --- | --- |
| Host | AMD Ryzen 7 8845HS, Linux 6.19.14-200.fc43.x86_64 |
| Reader CPUs | Logical CPUs 0-3 and 8-11, the two SMT siblings of four physical cores |
| Reader memory | 8 GiB for the whole process tree, no swap |
| Storage | Dedicated local MinIO behind the same transport proxy for every reader |
| Transport | 200 ms request latency with deterministic +/-20 ms jitter; one shared, progressively paced 150 Mbps response-body budget |
| Caches | Fresh reader process/client per invocation; MinIO and OS caches retained, no flushes |
| Sampling | Five independent timed invocations per reader, case and profile; each reuse invocation contains two queries |
| Reader versions | DAR 0.6.1, delta-rs 1.6.6, DuckDB 1.5.5, Polars 1.44.2, Spark 4.1.1 with Delta Lake 4.3.1 |

The reader allocation uses both SMT siblings of four physical cores. MinIO
has CPUs 4 and 5 and 4 GiB; the observer uses CPU 6 and the proxy CPU 7.
Spark runs with `local[8]` and a 4 GiB JVM heap within the common memory limit.

The scattered 416-column runs use a separate warmup. The other six cases use
their exact validation run as storage warmup. Each case applies its declared
method to all five readers. Compare readers within a case; these preparation
differences limit conclusions drawn by comparing layouts directly.

The transport is emulated, so these timings do not predict a particular S3
service. They include snapshot opening, planning, and request overhead.
Hardware, cache state, query selectivity, and storage conditions affect the
results on other tables.

## Data and reproduction

The downloads preserve every scheduled timed sample, with no outlier removal
or replacements:

- [Summary CSV](selective-read-summary.csv): all 80 entries, including quartiles
  and preparation methods.
- [Timing CSV](selective-read-timings.csv): all 400 independent timed invocations.
- [I/O CSV](selective-read-io.csv): requests and bytes from diagnostic runs.
- [Provenance JSON](selective-read-provenance.json): source report and export
  hashes, build identities, full SQL, and fixture settings.

The [repository benchmark guide](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/README.md)
links the commands for generating inputs, preparing readers, validating results,
and running comparisons. Recorded runs require their pinned harness and input
hashes. A new run with current source has its own identities.

The [complete evidence archive](https://github.com/mag1cfrog/delta-arrow-reader/releases/tag/selective-read-benchmarks-2026-10-07)
includes raw campaigns, exact-value certificates and exports, request captures,
native plans, frozen sources, fixture metadata, and checksums. Its audit regenerates
the published report and all CSVs byte for byte and rechecks 120 retained query
outputs against the references. Bulk tables and installed engines are excluded.

The [reproduction guide](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/EVIDENCE.md)
provides archive verification and pinned commands for new runs. Delivery checks
also exercised fresh small fixtures and all five native readers, reusing the
recorded engine binaries. No new full SF10 timing campaign was run for packaging;
the archived report and provenance keep their original publication-status fields.
