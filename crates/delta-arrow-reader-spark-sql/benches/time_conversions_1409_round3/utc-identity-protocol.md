# Planning literal UTC identities (#1409)

The [previous comparison](../reports/fixed-offsets-1409.md) found
39-41 ns per row in UTC conversions after calendar work was removed.
It attributed that cost to the remaining UDF evaluation, including
lazy argument collection and result scattering in CONVERT_TIMEZONE.
Mark zero-copy conversions when building each expression and
return the coerced microsecond input at execution. Keep the
logical calls for grouping and lazy evaluation. Preserve the
closing Arrow timestamp casts on the UTC functions.

Use the [paired driver](run_pair.py) with the [committed build pins](utc-identity.json)
and the unchanged #1409 manifest with its three UTC cases. Before timing, compare all
one million result rows and their executed types in both ANSI
modes and native. Exclude a difference from timing. Differential
tests compare literal and dynamic zone expressions in UTC and
Los Angeles sessions. The earlier boundary captures still apply,
including NULLs, invalid zone names and extreme timestamps.
Require formatting, root Clippy, the full Spark workspace and
an independent review.

Run three blocks with staging and candidate in turn, alternating
their order. Each block measures all three cases in both ANSI
modes and native, plus candidate native A/A. The schedule has
72 processes and 216 raw samples, bounded by 3,600 seconds overall
and 130 seconds per timing process. Report ns per input row above
projection and the range of nine retained samples per arm.

All six Spark comparisons must improve in all three blocks and
the median paired drop must exceed that case's native A/A floor.
The floor is the largest of the absolute native A/A median
difference and either repeat arm's half-range of process medians.
Reject a native control if all three candidate-minus-staging
paired deltas are positive, their median exceeds the floor, and
all three candidate process medians lie above all three staging
process medians. Allow the layout exception only with proven
equality of executed normalized instructions and state layouts.
Any excluded case, missing sample or unverified native change
holds the candidate. Commit the raw samples in either outcome.

Compare the candidate Spark medians with paired native and the
floor to report any positive residual. Report which equal zones
keep the converter because the original conversion can change
the value or fail.
