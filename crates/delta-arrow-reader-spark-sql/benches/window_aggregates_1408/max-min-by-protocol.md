# Sliding BIGINT MAX_BY and MIN_BY comparison (#1408)

Run the two cases in [max_min_by.json](max_min_by.json) using the
[matched comparison](string-aggregation-protocol.md#schedule-and-pass-rule)
and the unchanged million-row fixture in [manifest.json](manifest.json).
Staging's executable was built before the benchmark-only commits listed in
the manifest. Both builds use the same projection, affinity and release flags.

Before the clocks, require exact rows and executed types by ID in both ANSI
modes and against the native first/last-value references on these inputs.
Those references agree here because the ordering key is unique and matches
the sort key. Exclude either function if the full result or type differs.
The boundary tests compare the candidate against staging with different
ordering and sort keys, ties, nullable keys and values, filtered rows, BIGINT
extremes, empty input and frames, ROWS and RANGE frames, growing and whole
partitions, the plain aggregate and both non-BIGINT fallbacks.

The target is met when the candidate Spark median for each function and ANSI
mode is at most its candidate native median plus the same-session native A/A
floor. Use native process medians to calculate that floor and report the
median and range across nine timing samples per arm in ns per input row above
projection. If a gap remains after this change, report its size and its cause.
