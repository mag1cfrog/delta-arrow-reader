# Result projection aliases (#1413)

In the [attribution](../reports/functions-1413-attribution.md), the
four scalar kernels and the two window functions have a Spark result
projection that reads every output of a preceding projection and
renames it. The candidate gives that projection the requested names
directly. It requires the same executed fields and schema metadata and
keeps the two layers for a row-error prefix producer. Window input
projections, ordering and the function kernels retain their original plans.

The [differential test](../../src/result_projection.rs) compares the
two paths from the same physical input. It checks both ANSI modes,
nulls, empty batches, repeated names, metadata and a fallible cast.
Check that the [six fixed statements](result-alias-comparison.json)
have one result projection in the candidate and retain their computation
and output fields. Require exact complete rows and executed types in both
builds, native and both ANSI modes, before timing.

Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with three replica pairs and a 1,800-second session bound. Retain the
change if at least one function improves in at least two of three paired
process medians in both ANSI modes, with a median paired decrease above
its native A/A floor. Reject it if any Spark mode has all three pairs
slower, a median increase above its floor and disjoint process median
ranges. Report native timing on both builds and each remaining difference
from candidate native plus the floor.
