# Physical lowering of ordered scan renames (#1390)

The candidate reached the wide-query improvement target in both
ANSI modes. The native-configured struct access control failed the
regression gate, so this PR carries the trial and samples only.

## Wide query

The candidate binds a guarded ordered root projection by scan ordinal.
Both paths construct a regular `ProjectionExec` and run the full
physical optimizer. Times are medians of five process medians in ms.

| Mode | Default planner before | Default planner trial | Physical before | Physical trial |
| --- | ---: | ---: | ---: | ---: |
| Native plain | 0.071 | 0.083 | 2.181 | 2.143 |
| Native configured | 0.086 | 0.088 | 2.329 | 2.388 |
| Spark ANSI off | 1.680 | 1.307 | 10.495 | 10.244 |
| Spark ANSI on | 1.698 | 1.314 | 10.593 | 10.223 |

Default planner times sum lowering and physical optimization within
each plan. Both modes improved in all five observed pairs. Canonical
wide physical planning improved in five pairs with ANSI off and four
with ANSI on. The canonical native A/B floor was 0.001593 ms.

The standard projection name-resolution and constructor spans appeared
once in each of the 90 staging Spark observations and were absent in
the candidate and native observations. The candidate's projection
construction moves into its lowering remainder, which was 0.382 / 0.385 ms.
The eligibility proof runs outside the default planner and is included
in the canonical physical phase. Physical optimizer times are nearly
unchanged. Staging retains 1.593 / 1.611 ms of extra default-planner work
and 8.166 / 8.264 ms of extra physical planning, ANSI off / on.

## Native controls

Both flagged rows were native-configured. Values are in microseconds.
Both had five positive paired deltas and disjoint observed ranges.

| Query and phase | Staging median | Trial median | Median paired increase | Native A/B floor | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Map access, logical | 68.287 | 73.386 | 5.099 | 0.542 | Layout shift |
| Struct access, total | 288.205 | 293.826 | 5.019 | 0.662 | Gate stands |

The untimed traces select the same normalized SQL-to-statement routine
and the same 1,199-byte schema lookup body in both builds. The other
selected logical parsing, planning and driver routines match. The
matching bodies use the same stack and state offsets, and the locked
DataFusion and schema definitions are unchanged. A different compiled
schema-lookup copy was unused by either query. Map's logical timer ends
before calling the physical planner, so its change is a layout shift.

Struct's total includes physical planning. Its untimed trace enters
the changed `SparkQueryPlanner` future in both builds. Its normalized
instructions diverge at entry and stack allocation grows from 2,056 to
2,104 bytes. The executed path differs, so the rule for unchanged
controls leaves the gate in force. The comparison does not isolate
the contribution of an individual instruction.

## Validation and replay

All 678 canonical plan traces per build and the eight wide observer
traces matched before timing. Their snapshots include exact Arrow
schemas, the plan tree, ordering, partitioning, input requirements and
scan counters. No Delta data bytes were read and no query was executed
during measurement. Formatting, Spark-crate Clippy and 3,298 workspace
tests passed, with three ignored. The differential test compares plan
display, schemas and results using a reversed scan projection. The
[protocol](planning-1390-round3-physical-renames-protocol.md) was committed
at `0bb5844da3a9b3dbf2523322d3f1ef8088e57298` before timing.

- [All 360 canonical comparisons](../planning_round3_1390/physical-rename-summary.csv) and [observed components](../planning_round3_1390/physical-rename-planner-components.csv).
- [50 canonical processes and 13,500 retained pairs](../planning_round3_1390/physical-rename-paired-samples.jsonl.gz).
- [40 wide observer processes and 360 retained plans](../planning_round3_1390/physical-rename-stage-samples.jsonl.gz).
- [Build and control evidence](../planning_round3_1390/physical-rename-proof.json.gz).

The [trial bundle](../planning_round3_1390/physical-rename-candidate.bundle)
contains the exact candidate and protocol commits based on `2d33b13a`.
Fetch its `refs/heads/bench/1390-default-planner-attribution` into a
disposable checkout and check out `0bb5844d` to replay the trial.
