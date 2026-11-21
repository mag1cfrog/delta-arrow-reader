# Default UTC unix timestamp comparison (#1409)

Run the frozen UNIX_TIMESTAMP and TO_UNIX_TIMESTAMP statements in
[unix_timestamp.json](unix_timestamp.json) with the
[matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Both builds use the attribution fixture, projection, affinity and release
flags. Their outputs must match native and each other exactly by ID,
including executed BIGINT types and NULLs, before clocks.

Compare the omitted pattern with the original explicit default pattern
on the committed inputs. Cover string widths, year boundaries, leap
dates and seconds, negative epochs, extra whitespace or offset text,
DST gaps and overlaps, malformed values, NULLs and empty input. Exercise
UTC and Los Angeles sessions in both ANSI modes.

Report median and range across nine retained samples in ns per input row
above projection. For each alias and ANSI mode the target is candidate
Spark at or below the candidate native median plus the same-session
native A/A floor from process medians. Exclude mismatched results, record
any remaining gap and stop after this comparison.
