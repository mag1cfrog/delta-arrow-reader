---
title: Selective reads on wide tables
description: Compare five Delta Lake readers on public tables with about 60 million rows, two table widths, and snapshots with and without deletion vectors.
---

# Selective reads on wide tables

We compared Delta Arrow Reader (DAR), delta-rs, DuckDB, Polars, and
single-machine Spark on queries that return about 900 rows from Delta tables
containing about 60 million rows. The 416-column tables return 69 selected
columns; the 90-column tables return 71.

Matching rows are either grouped together or spread out within row groups.
Each layout has a snapshot with and without deletion vectors (DVs), which mark
deleted rows without rewriting data files. These combinations give eight cases.
The inputs use public TPC-H lineitem data at scale factor 10, extended with
synthetic numeric columns. These are custom scans, not standard TPC-H queries
or a TPC-H score.

## Query an initialized table

The README chart shows the **first complete query after table initialization**.
All five readers use this boundary: query planning, reading, and consuming every
result are timed. Process startup, Python imports, Spark session/JVM startup,
and table initialization are excluded. No query result cache is used.

DAR was rerun from merged commit
[`cd3064e`](https://github.com/mag1cfrog/delta-arrow-reader/commit/cd3064e7551bf04b4c763b540d855b285d65a738)
with its default automatic warmup and partial-page policy. The other four
readers retain their original five samples per case. The same input hashes,
queries, hardware, CPU and memory limits, MinIO binary, and network proxy
settings were verified before the update. This comparison combines the new
DAR measurements with the earlier competitor measurements.

Every value is the median of five independent processes, in seconds; lower is
faster. Each process initializes its table once and runs two queries. Only its
first query contributes to this table. Every case passed exact value checks
before timing began.

All readers had four physical CPU cores and an 8 GiB memory limit. Storage used
200 ms request latency (+/-20 ms jitter) and a shared 150 Mbps bandwidth limit.
These conditions emulate a network connection, not a particular cloud service.

| Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416, grouped | 1.730 | 10.385 | 10.847 | 10.241 | 10.302 |
| 416, scattered | 6.254 | 10.526 | 10.953 | 10.311 | 13.963 |
| 416, grouped + DV | 2.001 | 69.937 | 12.130 | 64.594 | 12.900 |
| 416, scattered + DV | 6.512 | 70.488 | 12.113 | 65.004 | 15.288 |
| 90, grouped | 1.689 | 22.101 | 22.449 | 21.988 | 10.931 |
| 90, scattered | 9.975 | 22.594 | 23.014 | 22.175 | 25.534 |
| 90, grouped + DV | 1.979 | 177.713 | 23.719 | 166.096 | 14.071 |
| 90, scattered + DV | 10.274 | 178.820 | 24.169 | 166.000 | 26.840 |

??? details "First-query quartiles"

    Values are `median [p25, p75]` in seconds. The bracketed interval covers
    the middle half of the five independent samples.

    | Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | 416, grouped | 1.730 [1.728, 1.737] | 10.385 [10.379, 10.388] | 10.847 [10.845, 10.847] | 10.241 [10.238, 10.244] | 10.302 [10.286, 10.424] |
    | 416, scattered | 6.254 [6.254, 6.261] | 10.526 [10.523, 10.526] | 10.953 [10.949, 10.953] | 10.311 [10.310, 10.312] | 13.963 [13.962, 13.981] |
    | 416, grouped + DV | 2.001 [1.995, 2.044] | 69.937 [69.937, 69.939] | 12.130 [12.126, 12.133] | 64.594 [64.403, 64.658] | 12.900 [12.839, 12.933] |
    | 416, scattered + DV | 6.512 [6.487, 6.513] | 70.488 [70.483, 70.491] | 12.113 [12.103, 12.121] | 65.004 [64.860, 65.014] | 15.288 [15.272, 15.329] |
    | 90, grouped | 1.689 [1.682, 1.694] | 22.101 [22.101, 22.107] | 22.449 [22.392, 22.462] | 21.988 [21.987, 21.989] | 10.931 [10.868, 10.960] |
    | 90, scattered | 9.975 [9.931, 9.995] | 22.594 [22.585, 22.596] | 23.014 [23.013, 23.019] | 22.175 [22.163, 22.195] | 25.534 [25.531, 25.571] |
    | 90, grouped + DV | 1.979 [1.944, 2.019] | 177.713 [177.700, 177.732] | 23.719 [23.708, 23.726] | 166.096 [165.950, 166.493] | 14.071 [14.055, 14.121] |
    | 90, scattered + DV | 10.274 [10.267, 10.364] | 178.820 [178.792, 178.921] | 24.169 [24.122, 24.199] | 166.000 [165.995, 166.237] | 26.840 [26.749, 26.948] |

## Initialization and repeated queries

Initialization is paid once per loaded table. DAR's default initialization
prepares metadata and samples the connection before deciding whether smaller
reads should help. That work is included in the initialization figures below.
The query chart describes applications that keep a table object and query it.
Use the combined figures below to include its first-use cost.

??? details "Table initialization"

    Values are `median [p25, p75]` in seconds. Engine startup is excluded.
    Each reader uses its native table initialization API.

    | Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | 416, grouped | 4.024 [4.021, 4.026] | 1.213 [1.213, 1.215] | 1.199 [1.199, 1.201] | 1.214 [1.212, 1.214] | 6.012 [6.006, 6.053] |
    | 416, scattered | 4.094 [4.093, 4.097] | 1.258 [1.257, 1.258] | 1.247 [1.247, 1.249] | 1.261 [1.260, 1.263] | 5.868 [5.861, 5.926] |
    | 416, grouped + DV | 4.429 [4.426, 4.440] | 1.651 [1.650, 1.651] | 1.629 [1.629, 1.629] | 1.650 [1.649, 1.652] | 6.243 [6.224, 6.273] |
    | 416, scattered + DV | 4.480 [4.478, 4.485] | 1.603 [1.603, 1.603] | 1.586 [1.582, 1.590] | 1.604 [1.603, 1.605] | 6.042 [6.039, 6.077] |
    | 90, grouped | 3.711 [3.706, 3.716] | 0.870 [0.868, 0.870] | 0.867 [0.866, 0.868] | 0.872 [0.871, 0.874] | 5.497 [5.473, 5.513] |
    | 90, scattered | 4.101 [3.977, 4.112] | 0.884 [0.884, 0.884] | 0.882 [0.878, 0.883] | 0.886 [0.886, 0.889] | 5.708 [5.693, 5.736] |
    | 90, grouped + DV | 3.804 [3.775, 3.816] | 0.908 [0.908, 0.911] | 0.908 [0.905, 0.909] | 0.913 [0.910, 0.914] | 5.766 [5.734, 5.767] |
    | 90, scattered + DV | 3.705 [3.695, 3.989] | 0.917 [0.917, 0.920] | 0.915 [0.913, 0.916] | 0.919 [0.916, 0.920] | 5.615 [5.591, 5.780] |

??? details "Initialization plus the first query"

    Values are `median [p25, p75]` in seconds. These are measured from the
    same sessions as the chart, with table initialization included.

    | Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | 416, grouped | 5.754 [5.753, 5.758] | 11.596 [11.595, 11.601] | 12.047 [12.045, 12.049] | 11.454 [11.449, 11.461] | 16.308 [16.299, 16.545] |
    | 416, scattered | 10.348 [10.318, 10.378] | 11.784 [11.783, 11.785] | 12.200 [12.196, 12.201] | 11.571 [11.571, 11.573] | 19.842 [19.815, 19.888] |
    | 416, grouped + DV | 6.436 [6.431, 6.470] | 71.587 [71.587, 71.590] | 13.758 [13.757, 13.762] | 66.244 [66.051, 66.310] | 19.110 [19.090, 19.180] |
    | 416, scattered + DV | 10.991 [10.977, 10.992] | 72.094 [72.086, 72.096] | 13.699 [13.693, 13.701] | 66.606 [66.462, 66.621] | 21.370 [21.364, 21.371] |
    | 90, grouped | 5.393 [5.376, 5.410] | 22.973 [22.968, 22.977] | 23.315 [23.260, 23.329] | 22.861 [22.859, 22.863] | 16.428 [16.381, 16.434] |
    | 90, scattered | 14.032 [13.971, 14.087] | 23.478 [23.470, 23.479] | 23.897 [23.891, 23.897] | 23.060 [23.052, 23.077] | 31.264 [31.249, 31.270] |
    | 90, grouped + DV | 5.782 [5.741, 5.854] | 178.621 [178.613, 178.643] | 24.624 [24.616, 24.630] | 167.006 [166.863, 167.403] | 19.837 [19.704, 19.964] |
    | 90, scattered + DV | 14.059 [13.966, 14.256] | 179.738 [179.709, 179.837] | 25.082 [25.037, 25.115] | 166.921 [166.914, 167.152] | 32.529 [32.356, 32.563] |

??? details "Second query on the same table object"

    Values are `median [p25, p75]` in seconds. Query results are not cached.
    The two queries within a process are not independent samples.

    | Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | 416, grouped | 1.739 [1.729, 1.746] | 9.896 [9.895, 9.897] | 10.825 [10.817, 10.826] | 10.224 [10.223, 10.224] | 7.501 [7.496, 7.526] |
    | 416, scattered | 6.247 [6.245, 6.273] | 10.149 [10.143, 10.156] | 10.923 [10.915, 10.925] | 10.297 [10.296, 10.309] | 11.176 [11.164, 11.190] |
    | 416, grouped + DV | 1.997 [1.991, 1.998] | 67.778 [67.767, 67.792] | 12.110 [12.108, 12.111] | 64.460 [64.259, 64.729] | 9.345 [9.327, 9.352] |
    | 416, scattered + DV | 6.455 [6.442, 6.505] | 68.345 [68.335, 68.354] | 12.075 [12.074, 12.077] | 64.966 [64.862, 65.234] | 11.795 [11.777, 11.813] |
    | 90, grouped | 1.704 [1.684, 1.705] | 21.670 [21.669, 21.670] | 22.460 [22.424, 22.477] | 22.140 [22.139, 22.197] | 8.407 [8.356, 8.437] |
    | 90, scattered | 9.956 [9.950, 9.977] | 22.122 [22.116, 22.122] | 23.010 [23.002, 23.027] | 22.109 [22.108, 22.111] | 23.020 [23.005, 23.033] |
    | 90, grouped + DV | 2.060 [1.994, 2.071] | 175.209 [175.197, 175.251] | 23.684 [23.672, 23.684] | 165.890 [165.332, 166.131] | 10.852 [10.840, 10.871] |
    | 90, scattered + DV | 10.243 [10.239, 10.260] | 176.236 [176.236, 176.252] | 24.198 [24.175, 24.200] | 166.514 [166.413, 166.594] | 23.670 [23.614, 23.672] |

Spark consumes the full result through `DataFrame.toArrow()`, so it has no
comparable streaming first-batch timestamp. The [raw timings](selective-read-current-timings.csv)
retain initialization, both query clocks, cleanup, and process measurements.
Blank fields mean unavailable, not zero.

## Requests and downloaded data

The following values come from separate diagnostic runs with request tracing
enabled. Tracing is disabled during timing. Each cell shows response-body MiB /
request count for **initialization and both queries together**, including Delta
logs, listings, Parquet, deletion vectors, and DAR's connection sampling.
One MiB is 1,048,576 bytes. The byte count is what the proxy accepted for
delivery to the reader, including bytes from interrupted responses.

| Columns / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416, grouped | 41.37 / 5077 | 331.73 / 86 | 339.06 / 76 | 346.00 / 84 | 136.59 / 887 |
| 416, scattered | 212.76 / 28191 | 339.10 / 88 | 341.20 / 76 | 348.17 / 84 | 345.79 / 247 |
| 416, grouped + DV | 47.90 / 5092 | 2261.01 / 96 | 352.57 / 90 | 2262.44 / 616 | 143.38 / 991 |
| 416, scattered + DV | 220.82 / 27782 | 2276.34 / 96 | 354.70 / 90 | 2277.77 / 616 | 352.58 / 351 |
| 90, grouped | 35.14 / 5505 | 742.82 / 102 | 756.19 / 78 | 766.23 / 136 | 147.48 / 1065 |
| 90, scattered | 435.38 / 34717 | 761.89 / 102 | 761.29 / 102 | 771.57 / 136 | 765.26 / 281 |
| 90, grouped + DV | 37.38 / 5517 | 5836.14 / 210 | 758.09 / 94 | 5837.30 / 978 | 148.54 / 1189 |
| 90, scattered + DV | 340.85 / 44492 | 5876.37 / 210 | 763.18 / 118 | 5877.53 / 978 | 766.32 / 405 |

File statistics identify five of 130 Parquet files for the 416-column query
and six of 60 for the 90-column query. The predicates and matching file sets
are unchanged by DVs. Whole-session diagnostics can touch additional files
during initialization, so their file counts also include preparation work.

DAR can request parts of eligible pages when the page layout and measured
connection cost make that useful. Fewer downloaded bytes can come with more
requests; latency, bandwidth, and concurrency determine whether that trade
helps. The [I/O CSV](selective-read-current-io.csv) separates traffic by object
type. DAR chooses ranges using its connection measurements, so its request
pattern can vary between invocations. These records do not provide comparable
decoded-page counters for all five readers.

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
| Transport | 200 ms request latency with deterministic +/-20 ms jitter; one shared, progressively paced 150 Mbps response-body budget; seed `q2-full-network-v1` |
| Caches | Fresh reader process/client per invocation; MinIO and OS caches retained, no flushes |
| Sampling | Five independent processes per reader and case; two sequential queries per process |
| DAR source | Merged main at `cd3064e`; automatic warmup and partial-page reads use public defaults |
| Other readers | delta-rs 1.6.6, DuckDB 1.5.5, Polars 1.44.2, Spark 4.1.1 with Delta Lake 4.3.1 |

The reader allocation uses both SMT siblings of four physical cores. MinIO
has CPUs 4 and 5 and 4 GiB; the observer uses CPU 6 and the proxy CPU 7.
Spark runs with `local[8]` and a 4 GiB JVM heap within the common memory limit.
Other runner settings retain the original benchmark's fixed resource budget;
using DAR's default read policy does not mean every runner setting is a library
default. The measured DAR build includes changes after the 0.6.2 release.

The scattered 416-column cases use a separate storage warmup. The other six
use their exact validation run as storage warmup. The update preserves this
per-case choice. Storage warmup happens in a separate process and is distinct
from the table initialization measured in every invocation.

The transport is emulated. Hardware, cache state, query selectivity, file
encoding, and storage conditions can change the results. These timings do not
predict performance on a particular S3 service or every Delta table.

## Data and reproduction

The current comparison contains 200 timed invocations: 40 new DAR sessions
and 160 retained competitor sessions. No samples were removed or replaced
within a case's five-run set.

- [Current timings](selective-read-current-timings.csv): every displayed sample,
  including separate initialization and first- and second-query clocks.
- [Current I/O](selective-read-current-io.csv): complete-session diagnostic traffic.
- [Current provenance](selective-read-current-provenance.json): build and input
  identities, exact-value certificates, source-record hashes, and export checksums.

The [original report](https://github.com/mag1cfrog/delta-arrow-reader/blob/cd3064e7551bf04b4c763b540d855b285d65a738/docs/public/benchmarks/selective-read-results.md)
and its [400 timing samples](selective-read-timings.csv),
[summary](selective-read-summary.csv), [I/O](selective-read-io.csv), and
[provenance](selective-read-provenance.json) remain unchanged. They include the
older DAR build and measurements that open a fresh table for each query.
Those opening times are not used in the current chart.

The [evidence release](https://github.com/mag1cfrog/delta-arrow-reader/releases/tag/selective-read-benchmarks-2026-10-07)
contains the original campaign archive. The default-policy update is a separate
archive, `selective-read-defaults-cd3064e.tar.gz`, with its raw runs, reference
results, exact exports, diagnostics, frozen sources, and replay script.
Bulk tables and installed engines are excluded.

The [reproduction guide](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/EVIDENCE.md)
explains archive verification and the pinned fixture and reader setup.
