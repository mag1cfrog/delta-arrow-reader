---
title: TPC-H-derived selective-read results
description: Eight Delta Lake read cases derived from TPC-H lineitem at SF10, comparing DAR, delta-rs, DuckDB, Polars and Spark.
---

# TPC-H-derived selective-read results

Review draft. All eight formal cases are complete. The complete reproduction
package and owner review remain open under
[#323](https://github.com/mag1cfrog/delta-arrow-reader/issues/323) and
[#324](https://github.com/mag1cfrog/delta-arrow-reader/issues/324).
The source report retains `publication_ready: false`.

This comparison reads small, complete results from wide Delta Lake tables.
It covers 416-column and 90-column tables, localized and scattered matches,
and snapshots with and without deletion vectors (DVs). Every reader completed
every case and both execution profiles: 80 reader/profile entries and 400 independent timed
invocations. Each entry passed exact output validation before timing.

The inputs derive from TPC-H lineitem at SF10, extended with synthetic numeric
columns. The queries are custom selective scans, not the standard TPC-H queries,
and the results are not a TPC-H score.

The case names describe stored table width. Raw artifact IDs retain `q2` for
the 416-column table and `q4` for the 90-column table. These are query-shape IDs
from the [private S3 case study](selective-s3.md), not TPC-H query numbers.
That earlier study used different data, hardware and transport.

## What the measurements show

Without DVs, delta-rs open-query medians were 2.07 times DAR's for the localized
416-column table and 3.29 times DAR's for the localized 90-column table.
The scattered variants were close:
delta-rs took 1.03 and 1.02 times as long. Polars was also close to DAR on the
two scattered no-DV cases. Complexity alone does not predict a large gap.

With DVs, delta-rs open-query medians were 6.02-24.37 times DAR's. DuckDB and
Spark also completed all four DV cases, with smaller time differences from DAR.
The full tables retain the near ties as well as the larger differences.

Every reader touched the same five Parquet files from the 416-column table or
six from the 90-column table, including on DV snapshots. The observed differences
concern traffic within those retained files and request behavior. These results do not establish that a comparator
stopped skipping files when DVs were present.

## Inputs and conditions

Each base table contains all 59,986,052 SF10 source rows. A DV snapshot shares
its base snapshot's Parquet bytes and removes one of the 895 qualifying rows.
The two layouts preserve logical values and file/group membership; scattering
changes row order within groups. The
[workload definition](selective-read-production-shapes.md) describes the mapping.

| Table / matches | Files | Stored columns | Output columns | Parquet GiB | Matching rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 columns, localized | 130 | 416 | 69 | 65.89 | 895 |
| 416 columns, scattered | 130 | 416 | 69 | 66.09 | 895 |
| 416 columns, localized + DV | 130 | 416 | 69 | 65.89 | 894 |
| 416 columns, scattered + DV | 130 | 416 | 69 | 66.09 | 894 |
| 90 columns, localized | 60 | 90 | 71 | 30.04 | 895 |
| 90 columns, scattered | 60 | 90 | 71 | 30.23 | 895 |
| 90 columns, localized + DV | 60 | 90 | 71 | 30.04 | 894 |
| 90 columns, scattered + DV | 60 | 90 | 71 | 30.23 | 894 |

Both queries apply this predicate and consume their full projection without
COUNT or LIMIT:

```sql
WHERE l_shipdate = DATE '1995-03-15'
  AND l_shipmode = 'AIR'
  AND l_linenumber IN (1)
```

The query on the 416-column table returns five source columns and 64 nullable
numeric payloads. The query on the 90-column table also returns `l_suppkey` and
`l_quantity`. Complete SQL, projection lists, fixture identities,
file sizes and writer settings are in the
[provenance extract](selective-read-provenance.json).

The Parquet writer targets 512 MiB files, uses Zstd level 3 and plain encoding
without dictionaries, and limits groups to 131,072 rows. The page settings are
20,000 rows, 1 MiB and 1,024-row write batches; the row limit can produce
20,480-row pages because the writer checks it at batch boundaries. The actual
file counts and sizes are shown above. The 416-column table has four groups per
file and the 90-column table eight.
Geometry describes opportunities to skip data, not measured decode counts.

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

The eight reader CPUs are logical CPUs, not eight dedicated physical cores.
MinIO uses CPUs 4 and 5 with 4 GiB; the observer uses CPU 6 and the proxy CPU 7.
Spark uses `local[8]` and a 4 GiB JVM heap within the common process-tree limit.
The [revision 6 contract](selective-read-spark-matrix.md) fixes the reader roster
and execution boundaries.

The scattered 416-column campaigns use a separate warmup. The other six use
their exact validation invocation as storage warmup. Each campaign applies its declared
method to all five readers. Compare readers within the same case; these
preparation differences limit conclusions drawn by comparing layouts directly.
No campaign used the later combined validation/diagnostic optimization.

This is controlled transport emulation. It does not measure a production S3
service or reproduce the private case study's network conditions. Query times
of several seconds still include opening, planning and request overhead.

## Open a snapshot and read the result

Times are `median [p25, p75]` in seconds. p25 and p75 are the 25th and 75th
percentiles of the five independent samples. The interval includes opening the
selected Delta snapshot and consuming the full result. Process startup and
Python imports are outside this clock. Spark session/JVM startup is recorded
separately and is also excluded here.

| Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 columns, localized | 5.595 [5.586, 5.605] | 11.584 [11.577, 11.585] | 12.856 [12.853, 12.857] | 11.454 [11.452, 11.464] | 16.357 [16.342, 16.368] |
| 416 columns, scattered | 11.378 [11.376, 11.382] | 11.773 [11.772, 11.773] | 13.031 [13.030, 13.038] | 11.605 [11.574, 11.642] | 19.868 [19.772, 19.903] |
| 416 columns, localized + DV | 6.181 [6.153, 6.348] | 71.563 [71.559, 71.566] | 14.968 [14.968, 14.971] | 66.348 [66.347, 66.411] | 19.066 [19.041, 19.152] |
| 416 columns, scattered + DV | 11.968 [11.952, 11.973] | 72.086 [72.086, 72.098] | 14.900 [14.892, 14.903] | 66.286 [66.267, 66.288] | 21.332 [21.234, 21.376] |
| 90 columns, localized | 6.990 [6.957, 7.035] | 22.966 [22.963, 22.967] | 23.816 [23.786, 23.851] | 22.861 [22.859, 23.010] | 16.433 [16.402, 16.441] |
| 90 columns, scattered | 22.949 [22.946, 22.952] | 23.471 [23.466, 23.481] | 24.388 [24.384, 24.398] | 23.027 [23.022, 23.039] | 31.456 [31.398, 31.472] |
| 90 columns, localized + DV | 7.329 [7.307, 7.337] | 178.588 [178.579, 178.591] | 25.091 [25.083, 25.098] | 166.613 [166.254, 167.018] | 19.822 [19.795, 19.824] |
| 90 columns, scattered + DV | 23.267 [23.267, 23.288] | 179.729 [179.718, 179.735] | 25.600 [25.557, 25.607] | 167.075 [166.865, 167.464] | 32.477 [32.413, 32.490] |

## Reuse the same native source

Each independent reuse invocation initializes one source, then plans and runs
the same query twice. It does not cache or persist query results. Query positions
remain separate distributions across five invocations; the two queries are not
ten independent samples.

Initialization plus query 1, in seconds:

| Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 columns, localized | 5.595 [5.585, 5.609] | 11.596 [11.595, 11.601] | 12.047 [12.045, 12.049] | 11.454 [11.449, 11.461] | 16.308 [16.299, 16.545] |
| 416 columns, scattered | 11.388 [11.387, 11.390] | 11.784 [11.783, 11.785] | 12.200 [12.196, 12.201] | 11.571 [11.571, 11.573] | 19.842 [19.815, 19.888] |
| 416 columns, localized + DV | 6.326 [6.291, 6.359] | 71.587 [71.587, 71.590] | 13.758 [13.757, 13.762] | 66.244 [66.051, 66.310] | 19.110 [19.090, 19.180] |
| 416 columns, scattered + DV | 11.977 [11.957, 11.987] | 72.094 [72.086, 72.096] | 13.699 [13.693, 13.701] | 66.606 [66.462, 66.621] | 21.370 [21.364, 21.371] |
| 90 columns, localized | 7.036 [7.035, 7.040] | 22.973 [22.968, 22.977] | 23.315 [23.260, 23.329] | 22.861 [22.859, 22.863] | 16.428 [16.381, 16.434] |
| 90 columns, scattered | 22.954 [22.949, 22.957] | 23.478 [23.470, 23.479] | 23.897 [23.891, 23.897] | 23.060 [23.052, 23.077] | 31.264 [31.249, 31.270] |
| 90 columns, localized + DV | 7.307 [7.306, 7.416] | 178.621 [178.613, 178.643] | 24.624 [24.616, 24.630] | 167.006 [166.863, 167.403] | 19.837 [19.704, 19.964] |
| 90 columns, scattered + DV | 23.280 [23.271, 23.291] | 179.738 [179.709, 179.837] | 25.082 [25.037, 25.115] | 166.921 [166.914, 167.152] | 32.529 [32.356, 32.563] |

Query 2 on the initialized source, in seconds:

| Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 columns, localized | 3.768 [3.766, 3.772] | 9.896 [9.895, 9.897] | 10.825 [10.817, 10.826] | 10.224 [10.223, 10.224] | 7.501 [7.496, 7.526] |
| 416 columns, scattered | 10.125 [10.120, 10.125] | 10.149 [10.143, 10.156] | 10.923 [10.915, 10.925] | 10.297 [10.296, 10.309] | 11.176 [11.164, 11.190] |
| 416 columns, localized + DV | 4.067 [4.055, 4.096] | 67.778 [67.767, 67.792] | 12.110 [12.108, 12.111] | 64.460 [64.259, 64.729] | 9.345 [9.327, 9.352] |
| 416 columns, scattered + DV | 10.350 [10.349, 10.354] | 68.345 [68.335, 68.354] | 12.075 [12.074, 12.077] | 64.966 [64.862, 65.234] | 11.795 [11.777, 11.813] |
| 90 columns, localized | 4.300 [4.299, 4.302] | 21.670 [21.669, 21.670] | 22.460 [22.424, 22.477] | 22.140 [22.139, 22.197] | 8.407 [8.356, 8.437] |
| 90 columns, scattered | 22.063 [22.061, 22.063] | 22.122 [22.116, 22.122] | 23.010 [23.002, 23.027] | 22.109 [22.108, 22.111] | 23.020 [23.005, 23.033] |
| 90 columns, localized + DV | 4.508 [4.506, 4.510] | 175.209 [175.197, 175.251] | 23.684 [23.672, 23.684] | 165.890 [165.332, 166.131] | 10.852 [10.840, 10.871] |
| 90 columns, scattered + DV | 22.349 [22.328, 22.360] | 176.236 [176.236, 176.252] | 24.198 [24.175, 24.200] | 166.514 [166.413, 166.594] | 23.670 [23.614, 23.672] |

Spark consumes the complete selective result through `DataFrame.toArrow()`.
It has no comparable streaming first-batch timestamp. The
[raw timing extract](selective-read-timings.csv) retains startup, initialization,
query, cleanup and whole-process measurements. Blank values remain unavailable;
they do not mean zero. The [summary extract](selective-read-summary.csv) retains
the quartiles, statuses and preparation method for all 80 entries.

## Requests and downloaded data

These are separate untimed diagnostic invocations. Each cell shows total
response-body MiB / request count for one open/query invocation, including
Delta logs, listings, Parquet, DVs and other requests. One MiB is 1,048,576 bytes.

| Table / matches | DAR | delta-rs | DuckDB | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 columns, localized | 66.92 / 334 | 170.51 / 56 | 179.54 / 42 | 176.34 / 44 | 71.63 / 452 |
| 416 columns, scattered | 171.59 / 44 | 174.31 / 58 | 180.61 / 42 | 177.42 / 44 | 176.23 / 132 |
| 416 columns, localized + DV | 73.67 / 343 | 1137.81 / 56 | 196.42 / 52 | 1137.93 / 311 | 78.41 / 506 |
| 416 columns, scattered + DV | 178.34 / 53 | 1145.47 / 56 | 197.48 / 52 | 1145.60 / 311 | 183.01 / 186 |
| 90 columns, localized | 90.40 / 400 | 372.47 / 66 | 379.45 / 43 | 383.57 / 70 | 74.19 / 541 |
| 90 columns, scattered | 380.50 / 58 | 382.00 / 66 | 382.00 / 55 | 386.24 / 70 | 383.08 / 149 |
| 90 columns, localized + DV | 91.35 / 408 | 2919.27 / 114 | 381.80 / 54 | 2919.57 / 492 | 75.19 / 605 |
| 90 columns, scattered + DV | 381.45 / 67 | 2939.38 / 114 | 384.35 / 66 | 2939.68 / 492 | 384.08 / 213 |

The file-stat candidates and files with live matches are five of 130 for the
416-column table and six of 60 for the 90-column table. Every reader touched
those same Parquet file sets. This corresponds to skipping 96.2% and 90.0% of
the files, respectively.

On the localized 90-column table without DVs, DAR downloaded 90.40 MiB in
400 requests, while delta-rs downloaded 372.47 MiB in 66 requests. Spark downloaded less than DAR,
74.19 MiB, but took longer and made 541 requests. Byte counts alone do not
explain latency. On the scattered 90-column table without DVs, all five readers
downloaded 380.5-386.2 MiB, and the time gap narrowed.

The [I/O extract](selective-read-io.csv) includes all 80 reader/profile entries,
with log, Parquet, listing and DV counts separated. Reuse rows cover initialization
and both queries together. Candidate files, files containing matches and touched
objects describe different things; page/group geometry does not establish how
many pages or groups an engine decoded.

## Evidence and reproduction status

The extracts come from one audited eight-case report. Its SHA-256 is
`6b62fd2265f425044b09b621394c54873c488ac20c17b31e3b4e4d059b645119`.
The [provenance extract](selective-read-provenance.json) records the source report,
per-campaign observation/summary hashes, build identities, exact SQL, writer
settings and hashes of these CSV exports. The raw timing extract contains all
400 scheduled timing records from the completed campaigns, without outlier
removal or replacements. Interrupted earlier studies contribute no samples.

The full retained campaign directories also contain exact-value certificates,
requests, native plans and storage-capture evidence. Those directories and the
clean-checkout reproduction package have not yet been published with this draft.
The CSV extracts alone are not the complete audit package.

The existing commands cover each stage:

1. [Generate the pinned public source](selective-read-fixtures.md) and
   [write the table layouts and DV pairs](selective-read-production-fixtures.md).
2. [Prepare the pinned readers](selective-read-runners.md), including
   [Spark and Delta Lake](selective-read-spark.md), and use the
   [revision 6 roster](selective-read-spark-matrix.md).
3. [Prepare independent exact references](selective-read-oracle.md), configure
   [storage and transport](selective-read-storage.md), and
   [run the predeclared campaign](selective-read-campaign.md).
4. [Generate and audit the staged overview](selective-read-production-report.md).

Historical artifacts require their recorded harness and source hashes. Current
source can generate a new campaign with new identities; editing old manifests
to make them pass current-source checks would invalidate their provenance.
Keep bulk tables outside Git and stage snapshots within the documented 192 GiB
data allowance. New timing runs remain manual.

## Regenerate the README chart

The light and dark SVGs use the open-query medians in the
[summary extract](selective-read-summary.csv):

```console
python -B benches/render_selective_read_chart.py
```

Add `--check` to verify the checked-in charts without rewriting them. The
README pins its image URLs to the commit containing the SVGs so they also work
on docs.rs. After changing the charts, update those URLs to the new asset commit.
