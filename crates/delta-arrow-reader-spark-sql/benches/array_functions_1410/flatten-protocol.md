# FLATTEN inner null bitmap comparison (#1410)

Use the frozen statement in [flatten.json](flatten.json) and the
[matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Keep the #1391 table, projection, affinity and Linux release flags.
Compare all million rows by ID, executed nested types and input guards
across both builds, both ANSI modes and native before clocks.

Compare the existing membership probe with the inner validity bitmap
using both input offset widths and both child offset widths. Include
NULL parents, NULL inner arrays, NULL scalar elements, empty arrays,
empty batches and slices. Run the existing Spark-reference array tests.

Report median and range of nine retained samples in ns per input row
above projection. Each ANSI median must be at or below the same-session
candidate native median plus its native process-median A/A floor.
Exclude mismatched cases and record remaining gaps with their causes.
