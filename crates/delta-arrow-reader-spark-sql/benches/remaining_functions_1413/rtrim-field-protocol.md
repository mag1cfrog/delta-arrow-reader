# RTRIM result field adapter (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
the same RTRIM kernel behind a Spark result-field adapter.
For one nullable Utf8 column, the native function's field declaration
already matches the Spark declaration. The candidate uses the native
function for that shape during expression simplification.

Require exact values and fields from
[`trim_case_fields.rs`](../../tests/trim_case_fields.rs) in both ANSI
modes. Compare default RTRIM with an explicit space via the retained
adapter. Cover null and empty values, whitespace and Unicode,
nullable and required fields, views, filtering and a subquery.

Use the [unchanged RTRIM statements](rtrim-field-comparison.json).
The displayed physical expressions have the same name. Check the
actual function records for the Spark adapter in staging and the
native RTRIM function without the adapter in the candidate. Require
exact million-row output and executed types for both builds, native
and both ANSI modes.

Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second bound. Retain the change if each ANSI mode improves
in at least two of three paired process medians and each median paired
decrease exceeds the native A/A floor. The target is candidate native
plus that floor. Record every remaining difference.
