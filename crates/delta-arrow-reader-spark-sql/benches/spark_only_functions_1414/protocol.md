# Constant argument work in Spark-only functions (#1414)

Each trial selects SQL and projection controls from the frozen #1391 manifest.
Both builds use the same million-row fixture, release flags, CPU affinity,
batch size and memory bounds. The linked [source inventory](../reports/functions-1414-inventory.md)
names the repeated operation for each candidate. The per-trial configuration
pins the staging product, candidate source and exact case IDs.

Run the existing [comparison driver](../window_aggregates_1408/compare.py)
once with each configuration. Preflight compares the complete output rows and
executed Arrow types of both builds in both ANSI modes. Each execution must
consume all one million input rows. Exclude a case before clocks if its
preflight differs from the reference. Record the difference.

The driver uses three process pairs in each ANSI mode with the Spark
portion of the [matched process schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The second replica reverses the engine and ANSI order. Each process has one
warmup and three retained observations. Report each arm's median and range
of process medians in ns per input row, above the plain projection. Compare
each pair directly. Use the larger half-range of the two process-median
sets as the comparison floor in that mode.

Retain a trial when the source and differential tests establish removal of
the named repeated work with exact results and when every timed case and
mode satisfies the no-regression check. A regression means two or more
slower candidate process medians and a median paired increase greater than
the floor. Apply the same rule in reverse to claim a measured improvement.
If timings overlap, report that outcome and the source work removed. If the
trial loses correctness or regresses, retain staging and report the reason.

Commit the configuration and any case-specific rules before clocks. The
configured wall-time limit ends the session. Use only the retained
observations, including slow samples, with one bounded comparison per case.
