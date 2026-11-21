# Positive literal array lookup comparison (#1410)

Run both frozen statements in [array_element.json](array_element.json)
with the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Keep the #1391 fixture, projection, affinity and Linux release flags.
Verify all million values by ID, executed types and input guards
across both builds, ANSI modes and the native references before clocks.

Compare positive INT-representable literal indexes with the prior CASE
path in both ANSI modes. Cover List and LargeList BIGINT columns, NULL
parents and elements, empty lists and input, integer value limits, and
indexes within and past the list, including the largest INT. Run the
existing Spark-reference array access suite for the original zero,
negative, dynamic, map and other admitted cases.

Report median and range of nine retained pairs in ns per input row above
projection. Each Spark median must be at or below the same-session
candidate native median plus its native process-median A/A floor.
Exclude mismatched cases. Record remaining gaps and causes.
