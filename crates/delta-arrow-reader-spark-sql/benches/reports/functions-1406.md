# Classification of the 42 function cases excluded by #1391

On the reduced inputs, 26 functions matched Spark 4.2.0 in both ANSI modes.
Three bitmap windows also matched in values and types, with nullable-field
flags differing. Seven regression-statistic windows returned different values.
The same `map_entries` panic occurred in stock DataFusion 54.1.0 with Arrow
58.4.0 and in the earlier #1391 build with the dependency forks.

## Outcomes

The Spark comparisons use 96 rows from the original table, including six rows in
each group. The timing columns are gross execution nanoseconds per input row.
They use the original one-million-row table for matching cases and 2,048 input
rows for bitmap windows. Median timings use three release processes per mode
and three retained executions per process. The data includes plain projection
times and the projection-adjusted values. [Protocol](functions-1406-protocol.md),
[executed types and rows in both engines](functions-1406-comparisons.jsonl.gz),
[retained measurements](functions-1406-timings.jsonl.gz).

| Function | Form | Outcome | ANSI off, ns/row | ANSI on, ns/row | Evidence |
| --- | --- | --- | ---: | ---: | --- |
| `approx_percentile` | aggregate | Matches Spark | 10.84 | 10.87 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `percentile_approx` | aggregate | Matches Spark | 10.84 | 10.86 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_avgx` | aggregate | Matches Spark | 4.75 | 4.73 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_avgy` | aggregate | Matches Spark | 4.70 | 4.71 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_r2` | aggregate | Matches Spark | 4.72 | 4.73 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_sxy` | aggregate | Matches Spark | 4.72 | 4.76 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `try_avg` | aggregate | Matches Spark | 0.82 | 0.82 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `array_append` | scalar | Matches Spark | 59.03 | 37.56 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `array_compact` | scalar | Matches Spark | 12.59 | 10.78 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `array_contains_all` | scalar | Spark rejects |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `array_insert` | scalar | Matches Spark | 199.88 | 199.96 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `array_prepend` | scalar | Matches Spark | 45.66 | 49.54 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `arrays_zip` | scalar | Matches Spark | 59.01 | 53.18 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `cardinality` | scalar | Matches Spark | 52.23 | 48.69 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `concat` | scalar | Matches Spark | 14.52 | 14.46 | [String fixture](../../tests/fixtures/binary-concat-spark.jsonl), [benchmark capture](functions-1406-comparisons.jsonl.gz) |
| `size` | scalar | Matches Spark | 53.86 | 50.38 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `date_format` | scalar | Matches Spark | 197.24 | 197.84 | [Temporal fixture](../../tests/fixtures/temporal-contracts/temporal-contracts-spark.jsonl), [benchmark capture](functions-1406-comparisons.jsonl.gz) |
| `map_entries` | scalar | Same panic in the stock build |  |  | [Stock probe](../functions_1406/stock_map_entries.rs) |
| `expm1` | scalar | Matches Spark | 9.04 | 9.02 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `aes_decrypt` | scalar | Column-input failure |  |  | [#1405](https://github.com/mag1cfrog/delta-arrow-reader/issues/1405) |
| `aes_encrypt` | scalar | Column-input failure |  |  | [#1405](https://github.com/mag1cfrog/delta-arrow-reader/issues/1405) |
| `base64` | scalar | Matches Spark | 25.95 | 25.77 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regexp_replace` | scalar | Matches Spark | 230.59 | 228.52 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `approx_percentile` | window | Matches Spark | 835.91 | 839.57 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `array_agg` | window | Matches Spark | 861.65 | 860.37 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `bitmap_and_agg` | window | Nullable flags differ | 2628.18 | 3540.25 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `bitmap_construct_agg` | window | Nullable flags differ | 3644.07 | 1234.10 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `bitmap_or_agg` | window | Nullable flags differ | 2279.41 | 2259.54 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `collect_list` | window | Matches Spark | 856.15 | 858.44 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `collect_set` | window | Matches Spark | 1135.31 | 1138.79 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `grouping` | window | Spark rejects |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `percentile_approx` | window | Matches Spark | 840.90 | 827.18 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_avgx` | window | Matches Spark | 413.21 | 412.88 | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_avgy` | window | Values differ |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_intercept` | window | Values differ |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_r2` | window | Values differ |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_slope` | window | Values differ |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_sxx` | window | Values differ |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_sxy` | window | Values differ |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `regr_syy` | window | Values differ |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `theta_intersection_agg` | window | Spark rejects |  |  | [Spark capture](functions-1406-comparisons.jsonl.gz) |
| `try_avg` | window | Matches Spark | 248.49 | 246.85 | [Spark capture](functions-1406-comparisons.jsonl.gz) |

## Value differences

For the seven differing cases, the capture archive contains the full SQL,
executed fields and every output row from both engines in both ANSI modes.
Each row below gives one example and the count per mode. `regr_r2` returned
NULL at IDs 4099 and 4106, where Spark returned `1.0`. In each of those
two-row frames, the dependent value is `0.7` at both rows and the independent
values differ. The other 11 `regr_r2` differences and every difference in
the other six cases are in the last digits of floating-point values.

| Window | Example ID | DAR | Spark | Differing rows per mode | Largest finite absolute difference |
| --- | ---: | ---: | ---: | ---: | ---: |
| `regr_avgy` | 20482 | 0.4999999999999999 | 0.5 | 8 | 2.22e-16 |
| `regr_intercept` | 20482 | 0.5807692307692306 | 0.5807692307692307 | 9 | 4.44e-16 |
| `regr_r2` | 4099 | NULL | 1.0 | 13 | 2.22e-16 |
| `regr_slope` | 20481 | -0.027777777777777773 | -0.027777777777777776 | 14 | 4.16e-17 |
| `regr_sxx` | 20481 | 61.2 | 61.199999999999996 | 7 | 1.42e-14 |
| `regr_sxy` | 20482 | -0.8999999999999994 | -0.8999999999999995 | 10 | 9.99e-16 |
| `regr_syy` | 20481 | 0.20000000000000007 | 0.2 | 10 | 1.11e-16 |

Spark does not resolve `array_contains_all`, rejects `grouping` in the
window, and fails during collection for an all-NULL `theta_intersection_agg`
frame with `SketchesStateException`. Those inputs provide no timed Spark reference.

## Bitmap output and memory

| Function | Input rows | ANSI off, ns/row | ANSI on, ns/row | Off peak RSS, MiB | On peak RSS, MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `bitmap_and_agg` | 512 | 926.31 | 925.62 | 76.46 | 75.37 |
| `bitmap_and_agg` | 2,048 | 2628.18 | 3540.25 | 90.28 | 90.41 |
| `bitmap_construct_agg` | 512 | 860.80 | 906.84 | 76.57 | 76.70 |
| `bitmap_construct_agg` | 2,048 | 3644.07 | 1234.10 | 90.18 | 90.96 |
| `bitmap_or_agg` | 512 | 856.90 | 1880.88 | 75.95 | 75.47 |
| `bitmap_or_agg` | 2,048 | 2279.41 | 2259.54 | 87.46 | 90.30 |

Each emitted frame held 4,096 binary bytes. With 512 input rows, the window
produced 2 MiB of payload. With 2,048 input rows, it produced 8 MiB. Running
the same bitmap aggregate over either input produced one 4 KiB result.
Binary payload therefore grows with the number of emitted frames. The window
emits a frame per input row in this workload. At one million rows, the payload
alone would total 3.81 GiB, and a hex copy would take 7.63 GiB before row
strings and allocation overhead. This explains the pressure from retaining
and encoding the original results under the 12 GiB address-space cap. The
RSS columns are medians of the peak RSS reported by each process. The peaks
include process setup and execution buffers.

## Stock map_entries

The root workspace probe used locked registry dependencies and enabled
DataFusion's `nested_expressions` feature to register the function. The
stock statement panicked while building a ListArray: it declared entry
children named `key` and `value` but supplied children named `keys` and
`values`. The first two original rows triggered the same panic seen
in #1391 with the forked build.

The local Linux timing session ran from 2026-11-20T17:40:02Z to
2026-11-20T17:43:17Z, using the committed protocol at `ae29c9f4`.
All 64 timing cells completed, with 576 retained samples. Staging product
code was pinned at `6a520463`.

## Existing reference checks

The committed temporal fixture tests `DATE_FORMAT(ts, 'yyyy-MM-dd')` on a
nullable timestamp column in UTC in both ANSI modes. Its existing
`temporal_contracts_match_spark` test checks the DAR rows and executed types
against that capture. The string fixture checks `CONCAT` on two nullable
string columns in both modes. The reduced benchmark capture covers the exact
#1391 statements on this report's input.
[Temporal fixture](../../tests/fixtures/temporal-contracts/temporal-contracts-spark.jsonl),
[string fixture](../../tests/fixtures/binary-concat-spark.jsonl).

Additional committed captures exercise literal pattern and replacement
arguments for `regexp_replace`, the 0.5 argument for DOUBLE approximate
percentiles, and `array_compact`, `size` and `cardinality` on nullable integer
arrays. The benchmark captures cover its own regex, the five-row percentile
frames and BIGINT elements. The six retained fixture suites passed all 233
tests on Linux.
[Regex fixture](../../tests/fixtures/regex-word-categories-spark.jsonl),
[approximate percentiles](../../tests/fixtures/approx-percentile-ranks/spark-results.jsonl),
[array compaction](../../tests/fixtures/array-compact-fields-spark.jsonl),
[collection length](../../tests/fixtures/collection-length-fields/spark-reference.jsonl).
