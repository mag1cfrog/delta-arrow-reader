# Insert a BIGINT at the second array position (#1516)

The [attribution](../reports/classified-functions-1516-attribution.md)
measured a 127-128 ns per row gap for ARRAY_INSERT. The literal
second-position plan still uses repeated array lengths, several
CASE conditions, slices and concatenation. Try one change to assemble
the result directly from offsets for BIGINT List columns at that
position. Keep NULL parent short-circuiting and leave other positions,
types and argument shapes on the original path.

Check empty arrays, the first and second positions, NULL parents,
NULL elements, sliced storage, and executed result fields. Compare
both implementations over the full #1391 fixture in both ANSI
modes. Then require that both builds and the native reference agree
on every row and executed type for the original restricted case.
Require formatting, root Clippy, full workspace and independent
review. Keep all existing fixtures and outcomes.

Use staging `b72a9c9b`, whose runtime inputs match the sealed
approximate-percentile candidate. Use the
[paired driver](compare_candidate.py) and fixed restricted manifest.
Run three alternating blocks with the same Linux release settings,
six native A/A processes, a 3,600-second overall schedule and
130 seconds per process. A complete run is 24 processes and
72 retained samples. Exclude a row or type mismatch.

Keep the trial only when both Spark modes improve in all three
pairs and their median paired reductions exceed the native
A/A floor. The floor is the largest of the absolute A/A median
difference and both half-ranges of process medians. Native fails
if all three increases are positive, their median exceeds the
floor, and all candidate medians exceed the staging medians.
Apply the layout exception with matching executed instructions
and state layouts. A missing comparison or unresolved native
gate holds the trial. Commit all raw samples and report the
remaining difference in either outcome.
