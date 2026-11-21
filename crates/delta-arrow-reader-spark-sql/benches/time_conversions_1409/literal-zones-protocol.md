# Literal CONVERT_TIMEZONE comparison (#1409)

Run the three CONVERT_TIMEZONE statements in [literal_zones.json](literal_zones.json)
on both builds with the pinned table, UTC session, CPU affinity and Linux
release flags from the [attribution protocol](protocol.md).

Before clocks, require exact million-row values and executed types by
ID. The UTC case also must agree with its native projection. Check the
physical plans for the original lazy invocation and the candidate scalar
invocation. Exclude a case if any of these conditions fail. The committed
boundary comparisons cover both ANSI modes, UTC and Los Angeles sessions,
fixed offsets, DST gaps and overlaps, microsecond extremes, dynamic and
invalid zones, NULL in each argument, a bad timestamp and empty input.

Use the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Both builds run in turn in one session. Report median and range of nine
samples per arm in ns per row above projection. Report the native UTC
floor from candidate control process medians and compare absolute cost.
The literal-zone removal demonstrates a reduction when the candidate
ranges lie below staging for all three cases in both ANSI modes. Report
any overlap. This slice retains the calendar conversion for each instant.
