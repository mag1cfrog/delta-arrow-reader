# INLINE_OUTER comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
identify list slicing in the emptiness check. The candidate uses offsets
for List and LargeList columns and keeps the same CASE and expansion.

Use [inline-outer.json](inline-outer.json) to pin the executables and
select the frozen INLINE_OUTER statement. Check all one million result
rows and both executed types in both ANSI modes and the native control
before clocks. Confirm the old and new emptiness expressions in the plans.
Require the differential test to agree on NULL and empty arrays, NULL
STRUCTs and children, sliced offsets and empty batches.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Report the Spark medians and ranges before and after in both ANSI
modes. Keep the candidate if both medians decrease and all correctness
checks pass. The native projection is a cost reference for this fixture.
Report its repeat floor alongside the Spark measurements and describe the
cost of the remaining row selection, field projection and expansion.
