# Plain Boolean COUNT_IF comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
identify the nullable CASE argument built for COUNT_IF. The candidate
uses the native COUNT filter for a plain Boolean column with no
aggregate modifiers. Admission and other forms retain their existing
paths.

Use [count-if.json](count-if.json) to pin the executables and select the
frozen aggregate case. Retain one million source and projection rows.
Check the single exact INT64 result in both builds, both ANSI modes and
native before clocks. Check that the physical Spark plans switch from
the Boolean CASE count to the native filter.

Before clocks, compare the [boundary queries](count-if-validation.json)
on both builds and ANSI modes. Check each group with true, false and
NULL predicates alongside DISTINCT and explicit FILTER, the zero
count for empty input, and the exact result or error after a derived
Boolean cast.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below the candidate native median plus the native
repeat floor in both ANSI modes. Record any remaining cost.
