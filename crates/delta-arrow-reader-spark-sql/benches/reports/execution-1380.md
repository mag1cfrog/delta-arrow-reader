# Spark Delta execution measurements (#1380)

Three of the 36 Spark query/configuration comparisons crossed the frozen
completion threshold on this SF1 snapshot: STRING-to-DATE/DECIMAL casts in both
ANSI modes and string functions with ANSI on. None crossed the peak pool
reservation threshold. All exact preflight comparisons passed, and the timed
native/Spark comparisons found no extra source files or bytes. These results
apply to the recorded local workload and bounds, not every Spark query.

Product source is pinned to staging merge
`8954dcec63f934359c7bcdb74b5220c842d898e4`; the protocol/harness commit is
`6376636dccfc10458f6b939a585d9173b469daf6`. Only bench and report files changed.
The [protocol](execution-1380-protocol.md) fixes the queries, resource bounds,
process order, timing boundaries, thresholds and profile rule before timing.

## Method and correctness

The existing native selective-read writer generated SF1 `wide.clustered`:
6,001,215 TPC-H-derived lineitems, 16 original fields and 64 deterministic
nullable INT64 payloads, six unchanged Parquet files totaling 3,808,281,862 bytes.
A benchmark-only version-0 Delta log adds the genuine virtual INT partition
column `file_bucket`, giving 81 snapshot columns with 80 stored columns. Each
bucket is one path-sorted source file. No data rows or Parquet bytes were
rewritten. [Preparation commands](execution-1380-fixture-commands.json) and the
[fixture hashes](execution-1380-fixture.json) preserve provenance and exact
layout. The optional separate 90-column producer was not used.

All three configurations use the same `configure_spark_sql` session setup,
snapshot, common typed native view and 10,000-row dimension. Native uses
`SessionContext::sql`; Spark uses its planner with UTC and ANSI off/on. The view
produces actual nullable DOUBLE operands and Boolean predicates, valid DATE and
DECIMAL strings, and the IN key. Its work remains visible and included in both
execution paths. Delta statistics record 353,763 and 353,138 NULLs in the two
source DOUBLE operands' payload columns (5.895% and 5.884% of input rows).
The three Boolean source columns have 352,793, 352,247 and 353,269 NULLs.

One separate untimed preflight exported all 54 query/configuration results and
passed all 36 native-versus-Spark full value/type comparisons. Unordered results
were globally sorted only for preflight. The comparator preserves multiplicity,
NULLs and SQL types across unequal batches; it normalizes Arrow string storage
and schema metadata/nullability. Negative self-checks reject changed cells,
types, NULLs, missing rows and duplicate rows. The timed plans have no export or
comparison sort. [Full preflight evidence](execution-1380-preflight.json.gz)
retains schemas, source reads, plans and IPC export hashes; the exact IPC files
remain in the external evidence directory. The [compact receipt](../execution/preflight-receipt.json)
binds that evidence to source, binary, queries, schedule and snapshot.

The physical partition filter read one of six real files and returned 1,048,576
rows. The statistics filter read one of six and returned 2,528 rows. Both were
nonempty proper subsets in all three configurations before freeze. Empty-filter
results and all other rows/types also matched. Each retained timed/concurrent
row count was checked against this exact preflight. All 810 retained single-query
source counter sets were final after drain, with recorded range bytes equal to
concrete received-byte counters. There was no semantic or source-I/O finding.

## Build and process schedule

This Linux worker has a Ryzen 7 8845HS, eight cores/16 threads, and approximately
23 GiB available RAM at capacity selection. Rust 1.96.0 built one release binary
with two Cargo jobs and release debug information. A rebuild of the exact clean
protocol commit produced the same SHA-256 as preflight:
`afabd4101f885adc9fbe4ee07950733c15d5638d9039d8881834b19c054389c6`.
The [metadata](execution-1380-metadata.json) includes the committed-source build
receipt, compiler, kernel, governor, CPU details, input hashes and full schedule.
Fixture generation used its existing separate Rust 1.98.1 package.

The once-only schedule completed 270 fresh query processes: 108 native A/B noise
processes, followed by 162 main processes in rotated configuration order. Each
process performed one warmup and three retained executions of its one query.
Each query/configuration therefore has three process medians. The six additional
concurrency processes each ran four independent executions. CPU affinity was
2,3,4,5 throughout; single-query runtime workers/target partitions were two,
with batch size 8,192. No competing Cargo work ran during timing. No unchanged
timing schedule or diagnostic profile was repeated.

Session creation, snapshot loading/registration and common-view/dimension setup
are excluded. Query entry precedes logical planning; planning includes physical
planning. Execution-first-batch starts before stream opening; query-first-batch
also includes planning. Execution completion drains to stream end. Required query
completion uses query entry through that same stream end. Result batches are
counted and dropped. Plan rendering, metrics reads and JSON output occur outside
these intervals. The cache was not flushed; these are warm-filesystem samples.

All reported medians first take each process's median of its three iterations,
then the median of its three process medians. CSVs retain minimum, maximum and
IQR of those process medians for every metric. Query-total samples are measured
at the stream-end boundary; they are not sums of independently aggregated phase
medians. Values below are rounded; [results CSV](execution-1380-results.csv),
[noise CSV](execution-1380-noise.csv), [summary](execution-1380-summary.json) and
[compressed raw samples](execution-1380-samples.jsonl.gz) retain the evidence.

## Threshold comparisons

A completion flag requires both >1.25x native and >50 ms additional query-total
time. A memory flag requires both >1.25x native and >16 MiB additional actual peak
pool reservation. Comparisons use unrounded medians and strict inequalities.
No division is needed for a zero native reservation baseline.


| Query | Configuration | Native total ms | Spark total ms | Increase ms | Ratio | Completion flag | Pool flag |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cast_strings | spark-off | 688.885 | 1129.266 | 440.381 | 1.639 | yes | no |
| cast_strings | spark-on | 688.885 | 1131.417 | 442.532 | 1.642 | yes | no |
| string_functions | spark-on | 341.726 | 431.068 | 89.342 | 1.261 | yes | no |


The string-functions result is close to the ratio boundary: the off/on
classification is the result of the frozen rule on these samples, not evidence
that ANSI itself caused the difference. Native A/B noise for this shape was
6.752 ms; the on-mode difference was 89.342 ms. Cast-shape noise was 0.578 ms,
well below its 440.381/442.532 ms differences. Planning is a small part of these
three totals; the detailed phase table keeps that visible.

## Query timing

All units are milliseconds. Total ranges are the minimum/maximum of three
process medians. `n/a` means no result batch was yielded, rather than zero time.


| Query | Configuration | Planning | Execution first batch | Query first batch | Execution completion | Query total | Total range |
| --- | --- | --- | --- | --- | --- | --- | --- |
| projection_five | native | 1.242 | 3.631 | 4.836 | 96.837 | 98.050 | 93.731..99.524 |
| projection_five | spark-off | 2.337 | 3.804 | 6.169 | 93.433 | 95.798 | 90.412..98.277 |
| projection_five | spark-on | 2.624 | 3.128 | 5.905 | 82.292 | 85.129 | 78.753..98.497 |
| partition_filter | native | 1.506 | 3.703 | 5.217 | 20.181 | 21.816 | 21.644..21.883 |
| partition_filter | spark-off | 2.876 | 3.677 | 6.485 | 20.624 | 23.500 | 21.992..24.791 |
| partition_filter | spark-on | 3.200 | 3.598 | 6.661 | 20.297 | 23.506 | 18.946..23.624 |
| statistics_filter | native | 2.098 | 4.290 | 6.227 | 4.299 | 6.234 | 5.989..6.433 |
| statistics_filter | spark-off | 3.289 | 3.593 | 6.752 | 3.602 | 6.761 | 6.589..7.285 |
| statistics_filter | spark-on | 3.481 | 4.095 | 7.775 | 4.104 | 7.785 | 7.660..8.011 |
| empty_filter | native | 0.917 | n/a | n/a | 0.015 | 0.932 | 0.918..1.344 |
| empty_filter | spark-off | 1.802 | n/a | n/a | 0.021 | 1.824 | 1.762..1.897 |
| empty_filter | spark-on | 1.760 | n/a | n/a | 0.022 | 1.782 | 1.765..1.942 |
| ordered_limit | native | 2.397 | 67.351 | 69.369 | 67.353 | 69.370 | 64.108..77.447 |
| ordered_limit | spark-off | 3.563 | 63.146 | 66.856 | 63.148 | 66.858 | 63.214..77.763 |
| ordered_limit | spark-on | 3.693 | 64.818 | 68.185 | 64.820 | 68.187 | 65.933..79.723 |
| group_low | native | 1.743 | 91.762 | 93.534 | 91.768 | 93.540 | 93.204..93.676 |
| group_low | spark-off | 2.994 | 96.599 | 99.578 | 96.605 | 99.585 | 98.971..99.715 |
| group_low | spark-on | 2.861 | 90.891 | 94.237 | 90.897 | 94.243 | 93.937..94.905 |
| group_high | native | 1.620 | 312.403 | 314.053 | 325.109 | 326.745 | 325.626..338.528 |
| group_high | spark-off | 2.619 | 322.176 | 325.287 | 338.353 | 341.092 | 332.002..342.129 |
| group_high | spark-on | 2.614 | 317.253 | 319.974 | 332.625 | 335.345 | 334.059..337.781 |
| count_distinct | native | 1.833 | 87.741 | 89.548 | 87.743 | 89.549 | 88.811..90.630 |
| count_distinct | spark-off | 2.999 | 87.948 | 91.291 | 87.951 | 91.293 | 90.264..91.652 |
| count_distinct | spark-on | 2.794 | 89.580 | 92.437 | 89.582 | 92.440 | 89.666..92.652 |
| row_number | native | 1.580 | 233.956 | 236.538 | 792.710 | 795.293 | 789.394..797.074 |
| row_number | spark-off | 2.585 | 233.918 | 236.503 | 797.497 | 799.966 | 790.339..808.599 |
| row_number | spark-on | 2.545 | 230.873 | 233.290 | 795.616 | 798.033 | 788.128..799.614 |
| dimension_join | native | 1.906 | 2.302 | 4.220 | 63.261 | 65.289 | 64.829..66.013 |
| dimension_join | spark-off | 2.897 | 2.102 | 5.093 | 65.064 | 68.203 | 66.741..68.936 |
| dimension_join | spark-on | 2.937 | 2.115 | 5.061 | 63.708 | 66.538 | 66.402..68.992 |
| self_join | native | 2.062 | 240.865 | 243.036 | 453.440 | 455.640 | 452.417..455.717 |
| self_join | spark-off | 4.016 | 244.561 | 248.844 | 454.686 | 458.602 | 457.225..470.373 |
| self_join | spark-on | 4.159 | 242.396 | 246.556 | 455.729 | 462.550 | 459.683..463.160 |
| decimal_division_round | native | 1.618 | 3.571 | 5.407 | 147.844 | 149.424 | 140.997..163.858 |
| decimal_division_round | spark-off | 2.861 | 3.899 | 7.250 | 156.785 | 160.250 | 157.892..164.129 |
| decimal_division_round | spark-on | 2.679 | 3.776 | 6.672 | 159.731 | 163.051 | 148.554..169.231 |
| cast_strings | native | 1.542 | 5.256 | 6.730 | 687.053 | 688.885 | 686.936..690.937 |
| cast_strings | spark-off | 2.574 | 6.338 | 9.337 | 1126.987 | 1129.266 | 1117.659..1138.407 |
| cast_strings | spark-on | 2.359 | 6.515 | 9.187 | 1128.232 | 1131.417 | 1128.498..1139.503 |
| string_functions | native | 1.984 | 3.633 | 5.682 | 339.717 | 341.726 | 333.653..354.528 |
| string_functions | spark-off | 2.934 | 3.778 | 6.959 | 416.939 | 419.957 | 417.384..420.296 |
| string_functions | spark-on | 2.668 | 3.942 | 6.625 | 428.226 | 431.068 | 415.929..432.273 |
| nullif_nullable | native | 1.840 | 4.871 | 6.643 | 169.684 | 172.135 | 171.559..172.312 |
| nullif_nullable | spark-off | 3.462 | 4.955 | 8.274 | 209.200 | 213.060 | 209.911..214.810 |
| nullif_nullable | spark-on | 3.333 | 4.690 | 8.061 | 209.896 | 213.430 | 212.740..215.671 |
| in_10 | native | 1.781 | 131.573 | 133.234 | 144.638 | 146.300 | 143.296..156.991 |
| in_10 | spark-off | 2.813 | 132.862 | 135.675 | 143.455 | 146.293 | 142.458..157.969 |
| in_10 | spark-on | 2.824 | 131.231 | 134.579 | 143.120 | 146.305 | 141.626..160.482 |
| in_1000 | native | 6.947 | 5.132 | 12.078 | 166.094 | 172.998 | 163.506..174.530 |
| in_1000 | spark-off | 25.022 | 4.883 | 29.850 | 171.921 | 196.942 | 179.156..199.334 |
| in_1000 | spark-on | 25.212 | 4.441 | 29.586 | 155.913 | 180.521 | 171.791..185.012 |
| mixed_boolean | native | 1.672 | 8.064 | 9.711 | 288.985 | 290.780 | 276.177..292.929 |
| mixed_boolean | spark-off | 2.989 | 9.070 | 12.005 | 276.398 | 279.333 | 270.174..282.099 |
| mixed_boolean | spark-on | 2.951 | 8.732 | 11.684 | 273.305 | 276.641 | 276.236..298.503 |


## Reservations, resident memory and source I/O

Pool values are actual tracked reservation maxima, including brief grow/shrink
peaks, not periodic observations or configured limits. Tracking serializes pool
mutations and peak updates, including concurrent reservations. Pool peaks are
reset between iterations only after reservations return to zero. Zero means no
reservation in this pool; reader buffers, scalar allocations and other memory
can still contribute to RSS. Process peak RSS includes startup, registration,
warmup and all three samples of this one query/configuration. Each query gets a
fresh process, so RSS cannot inherit another query's high-water mark. It is not
presented as a resettable per-iteration counter. Setup RSS is available in CSV.
In this table each triple is native / Spark ANSI off / Spark ANSI on, in MiB.


| Query | Peak pool MiB (N / off / on) | Process peak RSS MiB (N / off / on) |
| --- | --- | --- |
| projection_five | 0.000 / 0.000 / 0.000 | 100.410 / 107.648 / 107.332 |
| partition_filter | 0.000 / 0.000 / 0.000 | 90.395 / 99.348 / 99.383 |
| statistics_filter | 0.000 / 0.000 / 0.000 | 84.922 / 93.000 / 92.992 |
| empty_filter | 0.000 / 0.000 / 0.000 | 74.004 / 83.793 / 82.387 |
| ordered_limit | 0.827 / 0.829 / 0.827 | 105.707 / 115.992 / 114.586 |
| group_low | 1.001 / 1.001 / 1.001 | 106.156 / 113.520 / 111.844 |
| group_high | 237.384 / 245.815 / 245.189 | 360.004 / 367.664 / 367.074 |
| count_distinct | 20.501 / 20.501 / 20.501 | 117.008 / 124.277 / 125.395 |
| row_number | 291.959 / 291.959 / 291.959 | 396.488 / 404.750 / 402.938 |
| dimension_join | 0.632 / 0.632 / 0.632 | 94.348 / 103.180 / 102.668 |
| self_join | 353.658 / 352.783 / 352.346 | 639.531 / 658.566 / 654.324 |
| decimal_division_round | 0.000 / 0.000 / 0.000 | 101.441 / 111.258 / 111.297 |
| cast_strings | 0.000 / 0.000 / 0.000 | 103.023 / 113.359 / 113.215 |
| string_functions | 0.000 / 0.000 / 0.000 | 112.867 / 123.297 / 122.047 |
| nullif_nullable | 0.000 / 0.000 / 0.000 | 102.801 / 112.242 / 114.395 |
| in_10 | 0.000 / 0.000 / 0.000 | 120.871 / 127.980 / 126.453 |
| in_1000 | 0.000 / 0.000 / 0.000 | 120.223 / 128.633 / 129.566 |
| mixed_boolean | 0.000 / 0.000 / 0.000 | 121.633 / 125.496 / 129.535 |


The following median counts are identical in all three configurations. Distinct
files are unique Parquet paths with data reads. File tasks are scan tasks, so a
self join reads six distinct files through 12 tasks; a partitioned file may also
feed more than one task. Received bytes include every delivered range, including
repeated reads, metadata/footer ranges and self-join reads. They describe source
object-store I/O, not disk-controller bytes or OS cache misses. All full-GET
counts were zero. The raw counters preserve GETs, exact ranges and scan metrics.


| Query | Rows | Distinct files | File tasks started/completed | Received bytes | Range GETs | Output partitions |
| --- | --- | --- | --- | --- | --- | --- |
| projection_five | 6,001,215 | 6 | 6/6 | 57,208,208 | 58 | 2 |
| partition_filter | 1,048,576 | 1 | 2/2 | 10,016,945 | 10 | 2 |
| statistics_filter | 2,528 | 1 | 2/2 | 973,157 | 5 | 2 |
| empty_filter | 0 | 0 | 0/0 | 0 | 0 | 1 |
| ordered_limit | 100 | 4 | 4/4 | 33,466,874 | 66 | 1 |
| group_low | 4 | 6 | 6/6 | 37,462,756 | 58 | 2 |
| group_high | 1,500,000 | 6 | 6/6 | 59,504,591 | 58 | 2 |
| count_distinct | 1 | 6 | 6/6 | 23,630,457 | 58 | 1 |
| row_number | 6,001,215 | 6 | 6/6 | 37,176,189 | 58 | 2 |
| dimension_join | 6,001,215 | 6 | 6/6 | 37,176,189 | 58 | 2 |
| self_join | 6,001,215 | 6 | 12/12 | 87,221,751 | 116 | 2 |
| decimal_division_round | 6,001,215 | 6 | 6/6 | 61,660,413 | 58 | 2 |
| cast_strings | 6,001,215 | 6 | 6/6 | 57,151,761 | 58 | 2 |
| string_functions | 6,001,215 | 6 | 6/6 | 81,807,546 | 58 | 2 |
| nullif_nullable | 6,001,215 | 6 | 6/6 | 139,291,557 | 58 | 2 |
| in_10 | 13,774 | 6 | 6/6 | 114,080,509 | 58 | 2 |
| in_1000 | 1,379,288 | 6 | 6/6 | 114,080,509 | 58 | 2 |
| mixed_boolean | 1,112,548 | 6 | 6/6 | 227,822,142 | 58 | 2 |


Source counters exclude session/snapshot setup and local spill traffic. Operator
metrics are retained in raw samples, separately from source I/O. No main paired
sample read more distinct source files or received more source bytes on Spark
than its matching native iteration; successful drain counters prevent a prior
iteration's reads from silently moving into the next reset.

## Same-against-same noise

Each arm is the same native binary/query/configuration in three fresh processes.
Absolute differences below compare the two arms' medians. Relative differences
are relative to arm A. The largest percentage belongs to the short statistics
filter, illustrating why the rule also requires an absolute difference. This
small bounded noise run is not a confidence interval or a corrected estimate.


| Query | Native A total ms | Native B total ms | Absolute difference ms | Absolute relative difference |
| --- | --- | --- | --- | --- |
| projection_five | 95.800 | 89.489 | 6.311 | 6.588% |
| partition_filter | 21.979 | 21.302 | 0.677 | 3.081% |
| statistics_filter | 5.397 | 6.158 | 0.761 | 14.101% |
| empty_filter | 0.984 | 0.954 | 0.030 | 3.096% |
| ordered_limit | 78.226 | 70.828 | 7.398 | 9.458% |
| group_low | 91.634 | 91.832 | 0.198 | 0.217% |
| group_high | 328.593 | 330.810 | 2.217 | 0.675% |
| count_distinct | 90.051 | 90.227 | 0.175 | 0.195% |
| row_number | 789.794 | 790.204 | 0.411 | 0.052% |
| dimension_join | 67.566 | 64.460 | 3.106 | 4.596% |
| self_join | 454.166 | 456.957 | 2.792 | 0.615% |
| decimal_division_round | 151.331 | 161.083 | 9.752 | 6.444% |
| cast_strings | 680.827 | 681.404 | 0.578 | 0.085% |
| string_functions | 339.214 | 332.462 | 6.752 | 1.991% |
| nullif_nullable | 162.876 | 169.038 | 6.162 | 3.783% |
| in_10 | 142.544 | 141.317 | 1.227 | 0.861% |
| in_1000 | 164.577 | 171.544 | 6.967 | 4.233% |
| mixed_boolean | 281.056 | 277.843 | 3.213 | 1.143% |


## Physical plans and diagnostic profiles

[All 54 plans and 36 native-to-Spark diffs](execution-1380-plans.tar.gz) retain
the raw query plans before preflight sorting. Spark generally adds an output
renaming projection and internal `#` expression aliases; that alone does not
prove an expensive operator. The common native view's casts/predicates remain
visible on both paths. Shape-specific differences include the following.


| Shapes | Plan differences and retained work |
| --- | --- |
| projection_five, partition_filter, statistics_filter, empty_filter | Output alias/projection differences; same Delta snapshot and pruned scan behavior. The empty filter opens no source files. |
| ordered_limit | Internal aliases around the filtered sort/limit; one output partition in both paths. |
| group_low, group_high | Internal aggregate/projection aliases; same required grouping and three aggregate outputs. |
| count_distinct | Internal aggregate/output aliases; same executed BIGINT result. |
| row_number | Spark output aliases with the explicit common BIGINT cast; both retain partitioned sort/window work. |
| dimension_join, self_join | Internal join/output aliases; same join contracts and source-read counts. The self join uses unique composite keys. |
| decimal_division_round | Spark numeric/decimal casts and metadata wrappers with fused_decimal_divide; native decimal casts/division. Final DECIMAL(18,2) is explicit on both. |
| cast_strings | spark_date, spark_cast_string_to_decimal and metadata wrappers replace native DATE/DECIMAL casts. |
| string_functions | substring and spark_concat replace native substr and concat; upper is common. |
| nullif_nullable | Output aliases; both keep nullif(double_a, double_b + 1), column/column, direct-zero and scalar-left branches. |
| in_10, in_1000 | Spark output projections/aliases; both retain native IN(SET) filtering over the common nullable key. |
| mixed_boolean | Output aliases around the common nullable three-valued predicate/filter. |


Exactly one fresh profile was taken for each of the three flagged cases, with
three executions, CPU affinity 2,3,4,5, user-only `cpu-clock:u` sampling at 199 Hz,
and DWARF stack dumps bounded to 8,192 bytes. [Profile artifacts](execution-1380-profiles.tar.gz)
include the exact commands, perf data, decoded scripts, leaf/call-chain reports,
exit receipts and stack summaries. No samples were reported lost. These profiles
cover process startup, registration, planning and execution, not isolated timed
phases. Sample proportions cannot be interpreted as the added wall time.


| Case | Samples | Selected resolved leaf observations | Stack/attribution limit |
| --- | --- | --- | --- |
| cast_strings / off | 1,270 | chumsky::Parser::parse 14.57%; parse_ascii_decimal 5.67% | 987 decoded samples have no chain frames; only 76 have multiple frames. |
| cast_strings / on | 1,269 | chumsky::Parser::parse 13.40%; parse_ascii_decimal 5.91% | 998 decoded samples have no chain frames; only 72 have multiple frames. |
| string_functions / on | 480 | ZSTD 13.75%; concat builder 9.79%; substring bounds 8.12%; case conversion 7.29%; view-to-byte cast 6.04% | 445 samples have multiple decoded frames; 142 include an unknown frame. |


For casts, the physical plan directly identifies Spark's date and decimal scalar
functions. The existing Spark date implementation calls Sail's `parse_date`,
which uses Chumsky, and the decimal parser symbol is explicit in the leaf report.
Together these are consistent with work in the Spark cast path. Sparse unwinds
prevent a reliable caller-level split or a claim that all Chumsky samples belong
to one execution rule. Both casts and shared STRING creation remain in the query;
this run does not separately measure their individual net costs.

For string functions, decoded stacks include substring, STRING-view conversion
and concatenation work. Those correspond to plan differences, while ZSTD and
uppercase work are shared with native and cannot be blamed on Spark from this
profile. The surviving Spark substring wrapper and conversion samples are
candidates for maintainer inspection, not a demonstrated cause of the complete
89.342 ms difference. There is no native diagnostic profile or counterfactual
product change here. The observed near-threshold off/on split supports no ANSI
causality claim. No optimization or performance issue was created.

## Related issue shapes and bounded concurrency

All #601 NULLIF, #671 IN-list and #669 Boolean cases are below the combined
query-total and pool thresholds on this workload. Their actual query-total
medians follow; zero tracked reservation does not mean zero allocation.


| Shape / existing issue | Native total ms | Spark off total ms | Spark on total ms | Peak pool MiB (all paths) | Above combined thresholds |
| --- | --- | --- | --- | --- | --- |
| nullif_nullable / #601, #620 | 172.135 | 213.060 | 213.430 | 0 | no |
| in_10 / #671 | 146.300 | 146.293 | 146.305 | 0 | no |
| in_1000 / #671 | 172.998 | 196.942 | 180.521 | 0 | no |
| mixed_boolean / #669 | 290.780 | 279.333 | 276.641 | 0 | no |


The #620 release point is the NULLIF row above at batch size 8,192 with actual
nullable DOUBLE operands, including computed right operand `double_b + 1`.
It includes scan, common-view derivation, all four NULLIF outputs and frontend
planning. It is not a kernel-only before/after comparison, and does not complete
#620's separate multi-fraction, multi-batch or checked-caller acceptance.

The one concurrency check ran four independently planned executions through a
shared start barrier for each shape/configuration, on the same four-core mask
with four runtime workers and an 8 GiB shared tracked pool. Group time starts
immediately before the coordinator waits on the start barrier, after planning;
it includes the wait for tasks to arrive, and excludes session setup and planning.
These six single observations are a bounded concurrency check, not a throughput
curve or independently repeated concurrency medians. Each execution returned
its exact preflight row count. RSS includes process setup; pool is the aggregate
tracked execution peak, rather than a sum of individual peaks.


| Shape | Configuration | Four-execution group ms | Aggregate pool MiB | Process peak RSS MiB | Rows per execution |
| --- | --- | --- | --- | --- | --- |
| group_high | native | 979.087 | 855.793 | 923.051 | 1,500,000 |
| group_high | spark-off | 973.610 | 718.270 | 901.363 | 1,500,000 |
| group_high | spark-on | 977.080 | 950.169 | 994.535 | 1,500,000 |
| nullif_nullable | spark-off | 432.933 | 0.000 | 176.090 | 6,001,215 |
| nullif_nullable | spark-on | 446.551 | 0.000 | 183.410 | 6,001,215 |
| nullif_nullable | native | 318.743 | 0.000 | 178.426 | 6,001,215 |


## Validation and limits

The release harness, reservation/comparator self-checks, inventory/aggregation
self-checks, strict root Spark package all-target Clippy and Spark workspace
formatting passed before freeze. The aggregation self-check covers missing and
duplicate process records, unavailable first-batch values and zero native pool
baselines. One postmeasurement CSV unit-label correction marks
`source_bytes_received` as bytes; no raw sample, number, query or schedule changed.
The full local Spark workspace passed: 3,162 tests passed, none failed, and three
existing tests remained ignored across 229 result groups. All 3,165 named
outcomes and group summaries exactly match the accepted #1379 checkpoint,
using a two-pass inventory comparison that handles interleaved Cargo headers.
Final workspace formatting also passed. The strict all-target Clippy gate's
Rust/Cargo inputs are unchanged from its passing pre-freeze run. The
[validation receipt](execution-1380-validation.json) records commands, input
hashes, log hashes and the inventory result. The coordinator arranges independent
clean-context review before PR/merge.

The snapshot is local SF1, has six files and one derived physical partition per
file, and contains deterministic valid cast inputs. It does not cover remote
storage, malformed casts, cold caches, larger scales, different partition layouts
or all ANSI-sensitive data. Fresh processes isolate query RSS but include their
startup/setup high-water marks. Pool accounting covers actual registered
reservations and excludes allocations outside the pool. Three process medians
and the same-config noise bound describe this run; they do not establish a
portable confidence interval. The four-execution checks and each outlier profile
have one observation per configuration. Product source and existing test outcomes
were not changed. #1381 remains outside this report.

External generated tables, full IPC exports and original process logs are retained
under `/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380`.
Committed compressed samples, metadata, plans and profiles permit rechecking
aggregates and attribution without running another timing schedule.
