# Final Spark-only comparison (#1414)

The comparison configuration pins the product at the start of #1414,
the last retained staged product, and the release harness used by both
binaries. The manifest selects the [frozen #1391 SQL](../functions/manifest.json)
for each case without an individual trial under #1414. The output row
counts for generators and filtered window cases describe the original
fixture cardinality. Every case still consumes one million input rows.

Run one bounded session with the [paired driver](../window_aggregates_1408/compare.py)
and the existing release, CPU, batch and memory settings. The configured
limit is 3,600 seconds for preflight and clocks combined. Verify complete
rows, executed types and both row guards in both ANSI modes before timing.
Record each preflight mismatch with its reason and omit its timings.

Use the [shared protocol](protocol.md) for the three process pairs,
ordering, warmups, observations, process medians and comparison floors.
A timed mode contributes a result only when all three pairs complete.
Preserve the partial observations and report any incomplete case.
Keep the measurements of individual trials attached to their original
comparison sessions. Each final-table row takes both numbers from one
session or names the reason the comparison is unavailable.
