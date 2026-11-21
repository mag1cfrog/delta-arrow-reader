# Direct BIGINT ARRAYS_OVERLAP comparison (#1410)

Run the frozen statement in [array_overlap.json](array_overlap.json)
and the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Use the #1391 fixture, projection, affinity and Linux release flags.
Check all one million rows by ID, both executed Arrow types and input
guards across the two builds, native and both ANSI modes before clocks.

Compare the typed physical expression with the original CASE path in
both ANSI modes on committed List and LargeList inputs. Include NULL
arrays, empty lists and batches, sliced arrays, NULL children, BIGINT
limits, and larger rows that use either side as the lookup table.
Run the existing Spark-reference array tests for other storage and types.
The SQL regression also exercises derived operands, both directions and
ANSI cast failures with NULL and empty arrays. Compare the base and
candidate plans and results on the two derived-operand fixture probes
before clocks.

Report median and range of nine retained samples in ns per input row
above projection. For each ANSI mode the target is candidate Spark
at or below candidate native plus the same-session A/A floor from
native process medians. Exclude a mismatch. If a gap remains, record
its size and cause after this comparison.
