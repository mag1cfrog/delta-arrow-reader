# Rebuilding metadata for ordered wide scan renames (#1390)

Compare against staging `40a967a3`. The [default-planner profile](planning-1390-round3-planner-split.md)
measured about 0.90 ms rebuilding the wide result schema during
metadata alignment. The candidate uses the scan's fields by ordinal
when the root is a wide ordered rename of unique qualified columns
without alias metadata, functional dependencies or scan filters.
The existing alignment handles all other shapes.

Use the [paired controller](../planning_round3_1390/compare.py), the
[committed build settings](../planning_round3_1390/rename-metadata.json),
the frozen #1379 fixture and CPU 2. Before
timing, require formatting, Spark-crate Clippy, the full local Linux
workspace and independent review. Require exact Arrow schemas, logical
and physical plans, properties and zero Delta data reads for all 678
canonical traces per arm and the eight wide observer traces. The
existing #1379 executed references provide result and type coverage.
The benchmark only plans queries.

Run the existing alternating 50-process canonical schedule over 25
paired and five stress statements in all four configurations. Retain
all 13,500 paired observations. Use the same bounded 40-process wide
observer comparison with 360 retained plans, requiring exact physical
rule and schema-check pairs. Observe `physical.planner.align_result_metadata`,
which is present once per wide plan in Spark and native-configured and
absent in native-plain. Each ordinary alias-walk and schema-recompute
event appears twice per staging Spark sample and is absent from both
native modes and the candidate's Spark samples. Capture the overhead of the new branch in
the entire alignment span and in canonical physical time.

Retain the change if both Spark modes improve canonical wide physical time
and the observed alignment span in at least four of five pairs. The
canonical median paired drop must exceed that query's native A/B
floor. Any canonical regression blocks acceptance when all five paired
deltas are positive, the median exceeds the floor and the ranges are
disjoint with the candidate slower. Apply the maintainer's exception
only to unchanged controls whose actual normalized instructions and
state layouts match. Missing proof or a failed target holds the product
change. Report remaining costs and publish all raw samples either way.
