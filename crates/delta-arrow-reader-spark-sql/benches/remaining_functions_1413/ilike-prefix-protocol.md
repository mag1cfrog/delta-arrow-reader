# ILIKE with a literal ASCII prefix (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
LOWER, SparkLike and a Boolean field adapter in the Spark plan.
The candidate uses the existing native ILIKE predicate when a
Utf8 column batch is ASCII and the literal is a short ASCII prefix
ending in `%`. Unicode batches still use LOWER and case-sensitive LIKE.
All other literal forms, expression inputs and explicit escapes keep
the general planner path.

Require [differential assertions](../../tests/ilike/literal_prefix.rs)
against the previous route in both ANSI modes. Include nulls, empty
batches and strings, case changes, Unicode with different lowercase
and case-fold behavior, a different string representation, the prefix
length bounds, metacharacters and valid and invalid escapes.

Use the [unchanged ILIKE comparison](ilike-prefix-comparison.json).
The original Spark plans must contain LOWER and SparkLike, and the
candidate must use the prefix function. Require exact million-row
results and executed types across both builds, native and both ANSI
modes. Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second session bound. Retain the change if each ANSI mode
improves in at least two of three paired process medians and each
median paired decrease exceeds the native A/A floor. The target is
candidate native plus the floor in both ANSI modes.
