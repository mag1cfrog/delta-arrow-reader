# LIKE with a literal ASCII prefix (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
the Spark pattern adapter in the timed plan. A literal with a short
ASCII alphanumeric prefix and one trailing `%` has the same
case-sensitive meaning in the native predicate under the default escape.
The candidate uses that predicate for an exact Utf8 column.

The [differential test](../../tests/like_escapes/literal_prefix.rs)
checks the candidate against the adapter with the default escape supplied
explicitly. Both ANSI modes cover NULLs, empty inputs, a nonempty slice,
case, Unicode, length bounds, metacharacters, escapes and another string
representation.

Use the [unchanged LIKE comparison](like-prefix-comparison.json).
Check that the old physical plans have SparkLike and that the new
plans use native LIKE. Require exact million-row results and executed
types across both builds, native and both ANSI modes. Use the
[matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second session bound. Retain the change if each ANSI mode
improves in at least two of three process pairs and both median paired
reductions exceed the native A/A floor. The target is candidate native
plus the floor. Report both native builds and any remaining difference.
