# Lowering ordered wide scan renames (#1390)

Compare a narrow Spark lowering path against staging `b0de2e2b`.
The [default-planner profile](planning-1390-round3-planner-split.md)
measured about 0.44 ms translating full-width ordered aliases.
The candidate binds those aliases by scan ordinal after logical
optimization, creates the same `ProjectionExec`, and leaves the
physical optimizer schedule in place.

Use the [paired controller](../planning_round3_1390/compare.py), fixed
release artifacts in the [committed settings](../planning_round3_1390/physical-renames.json), the existing #1379
fixture and CPU 2. Before clocks, require the full Linux Spark
workspace, formatting, root-crate Clippy and independent review.
Require exact logical and physical plans, schemas, properties and
zero Delta data reads for all 678 canonical traces per arm and the
eight additional wide traces. Validation uses the frozen #1379
executed reference captures. The benchmark only plans queries.

Run the existing 50-process alternating canonical schedule over
the 25 paired and five stress statements in all four configurations.
Retain all 13,500 wall timing pairs. Observe initial lowering plus
physical optimization in the same session with the pinned
`planner-stages` observer recipe. Use 40 processes with alternating
build order and 360 retained wide plans. Verify the exact ordered
physical rule body and schema-check spans in each plan, with each
shared parent span captured once. Each process has 300 seconds.
Each schedule checks its 600-second budget at process boundaries.

Check that each canonical Spark wide shape takes the intended path
by inspecting its matching observer run. The standard expression and
projection-build spans each appear once in every staging Spark sample
and are absent in candidates and both native modes. The new extension
still constructs `ProjectionExec`, so that construction belongs to the
candidate's lowering remainder. The canonical wall also includes the
eligibility check before the observed default planner.

Retain the product change only if both Spark modes improve the
canonical wide physical phase and the observed combined default
planner phase in at least four of five pairs. The canonical median
drop must exceed that phase's native A/B floor. Report each
remaining observed component and the canonical gap to native-configured.

A canonical regression blocks acceptance when all five paired deltas
are positive, the median delta exceeds the native A/B floor, and the
candidate minimum exceeds the staging maximum. An unchanged control
passes under the maintainer's rule only when both the executed
normalized instructions and corresponding layouts match. Any failed
gate or missing proof holds the product change. Commit the report
and raw samples in either case.
