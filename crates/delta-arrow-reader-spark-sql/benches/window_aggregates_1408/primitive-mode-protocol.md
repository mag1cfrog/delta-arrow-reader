# Sliding primitive MODE comparison (#1408)

Run the MODE statement in [primitive_mode.json](primitive_mode.json) over
the unchanged [million-row fixture](manifest.json). Use the
[matched schedule](string-aggregation-protocol.md#schedule-and-pass-rule)
with the same release flags, CPU affinity and projection for both builds.
There is no native reference or native A/A control for this case.

Before clocks, require exact rows by ID and executed types for both builds
and ANSI modes. Exclude the function if the results differ. The committed
staging comparisons include numeric extremes, ties, explicit true or false,
NULL values and invalid options, filtered and partitioned frames, ROWS and
RANGE, growing, whole-partition and empty frames, ordinary aggregation and
FLOAT64 and string fallbacks. A separate test compares the counts and mode
with fresh frame replays.

Report the median and range of the nine retained samples per arm in ns per
input row above projection. Claim a measured reduction if the candidate
range lies below the staging range in both ANSI modes. Report overlap as
inconclusive and stop after the one comparison.
