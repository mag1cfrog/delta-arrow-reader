# Constant-zone timestamp comparison (#1409)

Run all nine zone statements in [fixed_offsets.json](fixed_offsets.json)
using the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Reuse the attribution table, UTC session, release flags and CPU affinity.
Each before-and-after pair runs in one session.

Require exact rows by ID and executed types across builds and both ANSI
modes before clocks. The three UTC statements must also agree with their
native projections. Exclude any mismatch. The tests cover Chrono's limits,
the interior boundary of the arithmetic path, both ends of the Int64
timestamp range, named and fixed zones, equal and distinct zero offsets,
NULLs, labels and existing Arrow errors. Check original Chrono boundary
panics one input at a time so they cannot mask other assertions.

Report median and range of nine retained samples in ns per input row above
projection. Use native process medians for the UTC A/A floors. Treat UTC
costs as absolute and use the Los Angeles statements as named-zone controls.
For the six UTC and fixed-offset cases, claim a reduction if the candidate
ranges lie below staging in both ANSI modes. Record any overlap and the
cost of the preserved calendar and output paths.
