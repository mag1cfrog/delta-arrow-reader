# LIKE with a literal ASCII prefix (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
the Spark pattern adapter in the timed plan. A Utf8 scalar pattern
with a short ASCII alphanumeric prefix and one trailing `%` already
has Arrow's default escaping. For a Utf8 source, the adapter can pass
this scalar through without rewriting it or converting its storage.
This retains Spark's local expression and error checks.

The [differential test](../../tests/like_escapes/literal_prefix.rs)
compares plain prefixes with a safe explicit escape that uses the
original preparation path. It covers NULLs, empty inputs, a nonempty
slice, case, Unicode, length bounds, metacharacters, escapes and
another string representation. Both modes retain a local assertion
failure inside LIMIT 0. A direct adapter test checks that the eligible
pattern keeps its original string allocation.

Use the [unchanged LIKE comparison](like-prefix-comparison.json).
Require the original SparkLike pattern in both physical plans and
exact million-row results and executed types across both builds,
native and both ANSI modes. Use the
[matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second session bound. Retain the change if each ANSI mode
improves in at least two of three process pairs and both median paired
reductions exceed the native A/A floor. The target is candidate native
plus the floor. Report both native builds and any remaining difference.
