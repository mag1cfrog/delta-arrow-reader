# Restricted native comparisons for the #1406 functions (#1516)

The restricted cases give 23 exact Spark/native comparisons. The
three largest distinct gaps are the approximate-percentile windows,
ARRAY_INSERT and REGEXP_REPLACE. The six other cases have a separate
source review below. Each timed pair kept the #1391 query and native
statement. The input restrictions apply before either execution clock.

## Paired cases

Values are median [min, max] ns per input row above the matching
projection, from nine retained samples per mode. The native repeat
floor uses the three process medians from each native arm. Inputs
contain 937,500 rows when filtered and one million when projected.
The preflight compares all result rows and the executed Arrow types
for native and both ANSI modes.

| Case | Input restriction | ANSI off | ANSI on | Native | A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| APPROX_PERCENTILE (aggregate) | Non-NULL values are 0.5 | 9.860 [9.688, 11.077] | 9.825 [9.748, 9.935] | 4.494 [4.450, 4.765] | 0.124 |
| PERCENTILE_APPROX (aggregate) | Non-NULL values are 0.5 | 9.792 [9.754, 9.823] | 9.812 [9.691, 10.878] | 4.500 [4.449, 4.755] | 0.030 |
| REGR_AVGX (aggregate) | Non-NULL x values are 8 | 4.544 [4.534, 4.591] | 4.595 [4.561, 4.633] | 4.577 [4.520, 4.597] | 0.032 |
| REGR_AVGY (aggregate) | Non-NULL y values are 0.5 | 4.489 [4.454, 8.490] | 4.510 [4.472, 4.746] | 4.451 [4.435, 4.473] | 0.014 |
| REGR_R2 (aggregate) | Non-NULL y equals x | 4.485 [4.441, 4.511] | 4.491 [4.457, 4.562] | 4.453 [4.431, 4.580] | 0.060 |
| REGR_SXY (aggregate) | Non-NULL y values are 0.5 | 4.513 [4.489, 8.388] | 4.492 [4.441, 4.516] | 4.458 [4.440, 4.548] | 0.040 |
| TRY_AVG (aggregate) | Non-NULL values are 0.5 | 0.635 [0.624, 0.686] | 0.635 [0.620, 0.648] | 0.571 [0.565, 0.577] | 0.011 |
| ARRAY_APPEND (scalar) | Non-NULL arrays and elements | 20.961 [20.766, 21.134] | 20.920 [20.765, 21.001] | 21.129 [21.040, 21.238] | 0.047 |
| ARRAY_INSERT (scalar) | Non-NULL arrays and elements | 208.244 [206.780, 210.657] | 207.625 [206.965, 211.510] | 80.576 [80.359, 80.935] | 0.128 |
| ARRAY_PREPEND (scalar) | Non-NULL arrays and elements | 21.138 [21.023, 21.350] | 21.114 [21.012, 21.295] | 21.278 [21.227, 21.381] | 0.028 |
| CARDINALITY (scalar) | Non-NULL arrays | 68.366 [67.994, 68.577] | 67.611 [66.826, 68.734] | 68.601 [67.622, 70.862] | 0.585 |
| CONCAT (scalar) | Both strings present | 10.526 [10.150, 12.771] | 10.581 [10.184, 10.654] | 10.554 [10.131, 10.714] | 0.318 |
| SIZE (scalar) | Non-NULL arrays | 68.821 [67.425, 79.585] | 70.337 [67.928, 70.767] | 69.391 [68.131, 69.720] | 0.954 |
| EXPM1 (scalar) | Non-NULL values are 0.5 | 3.198 [3.182, 3.222] | 3.185 [3.172, 3.195] | 3.829 [3.802, 3.890] | 0.018 |
| BASE64 (scalar) | Six-byte strings as binary | 18.845 [18.755, 19.545] | 18.974 [18.802, 19.075] | 21.808 [21.672, 22.391] | 0.240 |
| REGEXP_REPLACE (scalar) | One ASCII digit per non-NULL string | 110.534 [109.312, 114.321] | 110.327 [109.294, 111.331] | 68.259 [67.851, 70.378] | 1.908 |
| APPROX_PERCENTILE (window) | Non-NULL values are 0.5 | 809.706 [801.517, 813.489] | 807.944 [801.161, 817.889] | 487.828 [484.050, 489.934] | 2.260 |
| ARRAY_AGG (window) | Non-NULL values | 711.664 [704.353, 744.586] | 708.221 [704.610, 715.605] | 878.619 [863.455, 1034.906] | 32.254 |
| COLLECT_LIST (window) | Non-NULL values | 710.384 [706.000, 730.680] | 709.230 [704.216, 715.598] | 859.856 [853.179, 875.851] | 2.602 |
| COLLECT_SET (window) | Non-NULL values | 1069.555 [1067.378, 1079.763] | 1075.748 [1068.521, 1078.681] | 1066.476 [1061.093, 1082.753] | 5.771 |
| PERCENTILE_APPROX (window) | Non-NULL values are 0.5 | 811.480 [801.544, 830.299] | 805.133 [799.201, 813.364] | 485.105 [481.335, 494.855] | 4.693 |
| REGR_AVGX (window) | Non-NULL x values are 8 | 405.861 [401.453, 411.199] | 404.816 [402.268, 407.449] | 400.162 [396.816, 404.856] | 4.170 |
| TRY_AVG (window) | Non-NULL values are 0.5 | 243.229 [240.021, 247.032] | 240.990 [238.998, 242.169] | 288.460 [285.768, 290.678] | 1.267 |

## Source review where the original native statement differs

| Case | Native comparison | Spark work |
| --- | --- | --- |
| ARRAY_COMPACT | The native NULL removal operand returns NULL on the restricted non-NULL arrays. Spark retains the non-NULL elements. | Checks visible child nulls and compacts the child buffers only as needed. No constant argument. [Spark array compaction](../../vendor/sail/crates/sail-function/src/scalar/array/spark_array_compact.rs) |
| ARRAYS_ZIP | Native's nested fields are named `1` and `2`. Spark uses the argument names `arr` and `arr`, with a different list element declaration. Filtering the rows cannot align those types. | Resolves field names while planning, then reads each array's offsets, lengths and values at runtime. [Spark zip kernel](../../vendor/sail/crates/sail-function/src/scalar/array/arrays_zip.rs) |
| DATE_FORMAT | Native interprets the fixed `yyyy-MM-dd` as Chrono format text. Spark renders the year, month and day. | The Spark literal formatter is prepared once per batch. Each valid timestamp still contributes its own calendar fields. [Spark formatter](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_date_format.rs), [native format alias](../../vendor/dependencies/datafusion-functions/src/datetime/to_char.rs) |
| BITMAP_AND_AGG (window) | #1391 has no native reference. | Initializes a 4 KiB identity bitmap, ANDs each non-NULL input and clears the suffix beyond a short input. A later input can clear an already-zero suffix. Evaluating the frame copies its 4 KiB result. There is no constant SQL argument. [AND accumulator](../../vendor/sail/crates/sail-function/src/aggregate/bitmap_and_agg.rs) |
| BITMAP_CONSTRUCT_AGG (window) | #1391 has no native reference. | The input position selects a bit in a 4 KiB state. Every output materializes a 4 KiB bitmap. Position math and state updates depend on the actual rows. [Construct accumulator](../../vendor/sail/crates/sail-function/src/aggregate/bitmap_construct_agg.rs) |
| BITMAP_OR_AGG (window) | #1391 has no native reference. | ORs the actual input bytes and emits one 4 KiB result per frame. The fixed zero initialization belongs to the frame state, and there is no constant SQL argument. [OR accumulator](../../vendor/sail/crates/sail-function/src/aggregate/bitmap_or_agg.rs) |

The bitmap windows have no retractable accumulator here. Moving a
frame can recreate its state and replay the rows. The earlier #1406
experiment logged output volume, memory and the spread of the
2,048-row runs. This comparison does not assign that spread to a
constant argument or compare it with native. [Bitmap timing evidence](functions-1406.md#bitmap-output-and-memory)

## Where the work occurs

1. The two approximate-percentile names share the same accumulator.
   The window comparisons are 320-326 ns per row above native.
   Recreating the accumulator reads the fixed percentile and accuracy
   and allocates the one-element percentile vector again. Evaluation
   allocates another vector for the scalar result. Frame replay,
   quantile compression and the approximate result algorithm still
   depend on the selected input rows. Try avoiding the repeated
   scalar configuration and result packaging before revisiting the
   quantile work. [Accumulator creation and evaluation](../../vendor/sail/crates/sail-function/src/aggregate/approx_percentile.rs)
2. ARRAY_INSERT is 127-128 ns per row above native. The literal
   second-position plan still computes array length for CASE
   branches, constructs left and right slices and concatenates
   them with a one-element array. Try one offset-based insertion
   at that position while preserving the original fallback.
   [Spark INSERT planning](../../vendor/sail/crates/sail-plan/src/function/scalar/array.rs)
3. REGEXP_REPLACE is 42 ns per row above native. The Spark loop
   reads and validates the same literal pattern, replacement and
   position on each live row. It uses a regex cache, then executes
   the capture-based replacement loop, including parsing the
   unchanged replacement for every match. Try one prepared path
   for a literal replacement with no group references or escapes.
   Keep the original invalid-argument and match behavior.
   [Spark replacement loop](../../vendor/sail/crates/sail-function/src/scalar/string/spark_regexp_replace.rs)

SIZE and CARDINALITY both use the generic array-length kernel and
then cast the length. That kernel accesses a child array slice per
row for the default dimension. ANSI off adds the legacy NULL
sentinel through COALESCE. The no-NULL measurements are within
native repeat noise or faster. ARRAY_APPEND uses the same Spark
plan in both modes on the restricted input. It differs by 0.041
ns per row in this comparison. Its #1406 gross sample ranges were
37.640-59.531 off and 32.339-58.097 on. The ranges overlap and
the input restriction changed, so the old spread does not identify
an ANSI-only cost. [Collection lowering](../../vendor/sail/crates/sail-plan/src/function/scalar/collection.rs)
[Original ARRAY_APPEND samples](functions-1406-timings.jsonl.gz)

COLLECT_SET uses a native distinct array aggregator with an explicit
NULL filter and Spark's empty-array and element declaration. Only
ANSI on exceeds the native repeat floor. The extra projection work
and aggregate work are both present in the timings. This experiment
does not isolate a removable cost from the residual. REGR_AVGX
has a native moving accumulator on both paths and Spark's input
metadata expression. The window residual is 5.699 / 4.654 ns per
row, and the source and whole-expression timing do not separate
those operations. REGR_AVGY and REGR_SXY also retain measured
excess in at least one mode. TRY_AVG has a Spark-specific ordered
sum and its native reference uses native AVG. The difference in
floating-point results on the full input makes changing the sum
order unsuitable. These gaps remain for the issue to track.
[Window collection lowering](../../vendor/sail/crates/sail-plan/src/function/window.rs)
[Regression accumulator](../../vendor/dependencies/datafusion-functions-aggregate/src/regr.rs)
[TRY_AVG accumulator](../../vendor/sail/crates/sail-function/src/aggregate/try_avg.rs)

## Evidence

The [protocol](../functions_1516/protocol.md) and the final binary
input type in the [manifest](../functions_1516/manifest.json) were
committed at `607cf7f1` before the timing run. The initial BASE64
fixture failed during preparation because the native SQL parser
does not recognize the Spark BINARY type spelling. The timed
protocol uses the native BYTEA spelling for the fixture only.
The #1391 statements are unchanged.

The Linux release timings ran from 2026-11-23 05:30:23 UTC to
05:38:45 UTC. All 276 processes completed and retained
[828 raw samples](../functions_1516/baseline-samples.jsonl.gz).
The release source and binary digest are in the protocol.
