# Splitting the default physical planner span (#1390)

Use staging `b0de2e2b`. Before choosing a product change, split the
default planner span from the [wide attribution](planning-1390-round3-wide-attribution.md).
Distinguish initial lowering, projection expression translation,
projection construction, physical rule bodies and invariant checks.
Within metadata alignment, measure expression traversal and schema
recomputation separately. Report nested spans under their parent.

Build `planner-stages` with the [sealed builder](../planning_round3_1390/build.py).
The builder checks the DataFusion archive against the locked checksum
and applies the extra spans in a disposable Cargo path override. Keep
the lockfile and product source unchanged. Match the observer's wide
plans against the canonical release in all four configurations and
verify the same rule callbacks with the inventory release. Verify
the exact Arrow schemas, planning properties and zero Delta data I/O.

Run [measure_default_planner.py](../planning_round3_1390/measure_default_planner.py)
with the existing canonical and inventory builds, the new observer,
the frozen #1379 Delta fixture and a fresh evidence directory.
Use the existing Linux release flags and CPU 2. Run five blocks
of four configurations, rotating order. Each process has two warmups
and nine retained plans. The 20 processes have 300 seconds each, and
the schedule checks its 600-second budget at process boundaries.
No query is executed. Commit the 180 samples compressed.

Compare median process medians with native-configured. Use disjoint
child spans to attribute the default planner excess and keep each
span that contains sampling overhead identified as observed. If a
cost cannot be localized or cannot be addressed with one change that
preserves exact plans, report why and continue to the next component.
A product candidate gets a separate committed paired protocol and
the canonical 30-statement comparison.
