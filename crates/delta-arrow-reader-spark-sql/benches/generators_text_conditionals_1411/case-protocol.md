# Searched CASE with a widening ELSE comparison (#1411)

The [attribution](../reports/generators-text-conditionals-1411-attribution.md#when-and-case)
shows a true-base Spark CASE taking the general branch evaluation
path. A single Boolean column chooses between a BIGINT column and
an INT column widened to BIGINT. Both arms already have the
declared BIGINT type, and the widening cast is infallible. Use
searched CASE for this shape. Keep the cast and its existing
result annotation. Other explicit ELSE forms keep the Spark path.

Run the [pinned comparison](case.json) on the unchanged million-row
CASE fixture. Require exact rows and executed Arrow types for both
builds, both ANSI modes and the native statement before clocks.
Verify the physical plan with a searched CASE in the candidate and
the true-base form in staging.

Compare the [untimed controls](case-validation.json) in both builds
and ANSI modes before clocks. Cover NULL conditions and results,
both INT and BIGINT endpoints, empty input, an invalid string cast
with its bad rows unselected, and the same cast when selected. Also
compare a widened INT column derived from a potentially failing
upstream expression and a guard derived from the BIGINT arm's NULL
status. Require exact rows and types on success and the original
failure when present.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus the measured A/A
floor in both ANSI modes. Record any residual gap.
