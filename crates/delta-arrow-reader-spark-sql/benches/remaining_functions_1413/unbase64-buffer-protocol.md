# UNBASE64 array decoding buffer (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
Spark's UNBASE64 decoder in the profile. Its array loop allocates
the decoded bytes for each nonnull row. The candidate reuses one
array-batch buffer with the same Base64 engine. Input filtering
after a failed decode and the scalar decoder keep the original rules.

Require the direct array-versus-scalar differential test in
[`spark_base64.rs`](../../vendor/sail/crates/sail-function/src/scalar/string/spark_base64.rs).
Both ANSI settings cover Utf8, Utf8View and LargeUtf8, NULLs,
empty inputs, slices, padding, trailing bits, filtered characters,
sequential long and short inputs and exact decoding errors.

Use the [unchanged UNBASE64 comparison](unbase64-buffer-comparison.json).
Require SparkUnbase64 in both physical plans and identical rows and
executed types for both builds, native and both ANSI modes after
1,000,000 inputs. Use the
[matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second bound. Retain the change if each ANSI mode
improves in at least two of three process pairs and its paired
median decrease exceeds the native A/A floor. The target is
candidate native plus the floor in both ANSI modes.
