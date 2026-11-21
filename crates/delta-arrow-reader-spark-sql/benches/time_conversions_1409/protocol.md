# Time conversion attribution (#1409)

Pin the current staging source, `ff4ed624`, and run the 16 cases in
[manifest.json](manifest.json). Ten statements and projections come
unchanged from #1391. Add six Spark-only statements for CONVERT_TIMEZONE,
TO_UTC_TIMESTAMP and FROM_UTC_TIMESTAMP using America/Los_Angeles and
`+05:45`. The session time zone stays UTC. The executable from #1408
has the same product source as this staging tip, with only benchmark and
report commits between them.

## Exact results and schedule

Use the #1391 Linux release settings, CPU affinity, million-row table,
memory budgets, one warmup and three retained query/projection pairs per
process. The observer keeps the full results and exact executed Arrow
types. Require both ANSI modes and any supplied native statement to agree
on all rows by ID, including NULLs. Exclude any mismatch from timing and
record its cause. Real zone conversions have Spark-only controls.

Run three replicas, each with one fresh process per configuration for each
eligible case. Rotate cases by one third between replicas and reverse the
configuration order on replica two. For paired cases add two fresh native
A/A runs per replica in #1391 label order. Report ordinary medians and
ranges in ns per row above projection, with native noise calculated from
the median of each native process. Report UTC identity costs as absolute
numbers because their native reference is a projection. The process caps
are 120 seconds per case, 130 seconds per timed process and 300 seconds
per preflight. The entire session is bounded to four hours.

An attribution is complete when the report traces every frozen statement
to its physical implementation, identifies the repeating work and records
all eligible named-zone and fixed-offset cases. Take no product changes
during this measurement. Compare future candidates and staging in the same
new session after committing that slice's pass rule.
