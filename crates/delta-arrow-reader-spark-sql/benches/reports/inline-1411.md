# BIGINT INLINE child projection results (#1411)

INLINE reuses a BIGINT child when its validity already covers
its parent STRUCT's NULLs. The existing union handles other inputs.
UNNEST retains the same list expansion.

## Remaining gaps

- INLINE with ANSI off remains 8.218 ns per row above native. Its floor is 0.067 ns.
- INLINE with ANSI on remains 8.377 ns per row above native. Its floor is 0.067 ns.

The Spark plan projects the STRUCT child and expands the list into
individual rows. On this fixture the native reference filters the
parent validity and projects the known one-item field directly.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| INLINE | Native | 7.059 [6.968, 7.144] | 7.050 [6.947, 7.192] | 0.067 |
| INLINE | ANSI off | 15.539 [14.093, 16.910] | 15.268 [14.676, 15.746] | 0.067 |
| INLINE | ANSI on | 15.552 [14.794, 15.745] | 15.427 [14.443, 15.821] | 0.067 |

Each timed statement matched all 937,500 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential compares field types and metadata, shifted STRUCT
and child validity, empty and sliced lists, and NULL parent and child
values. The boundary SQL compares all 24 mixed-field result rows, names
and types in both modes, including NULL, empty and multi-element lists.
All 649 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../generators_text_conditionals_1411/inline-protocol.md) was
committed at `6f4e22a41e71e03872aed7457fbc3321d143e84c` before clocks.
The Linux release session ran 2026-11-21T08:33:17Z to 2026-11-21T08:33:49Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1411/inline`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `7e84d3dfaa866627985fa5b2f6eef73483b02e65` | `495dfc2a6c976a34cdd0f97392691c51729b21c19765e32886b1d45b684d6247` |
| candidate | `503ff44aa11e13ffff079e8aeff88c999b141ffa` | `9b97764a332c2110d618d2c57b1e41345f1650a982e46801f970272eb617bdba` |

## Candidate decision

The retained process medians vary in both directions. The numbers are
candidate minus staging in ns per input row.

| ANSI mode | Replica 1 | Replica 2 | Replica 3 |
| --- | ---: | ---: | ---: |
| Off | -0.648 | -0.648 | +0.050 |
| On | -0.071 | +0.544 | -0.137 |

The timings establish parity and an unmet target. The small aggregate
median change lacks consistent paired support. Staging keeps the
original projection. Its two ANSI medians are 8.481 and 8.494 ns per
row above the staging native control in this session.

The [saved candidate branch](https://github.com/mag1cfrog/delta-arrow-reader/tree/perf/1411-reuse-inline-int64-child-buffer)
retains the implementation and the timed protocol commit above.
