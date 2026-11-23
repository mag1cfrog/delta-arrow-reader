# Where the default planner spends time on 400 columns (#1390)

The default planner adds about 1.60 ms to Spark's wide physical
planning over native-configured. Projection name and expression
resolution is the largest measured child span. Its 0.44 ms is enough
to test a narrower lowering path for ordered column aliases.

| Observed span | Native configured, us | Spark ANSI off, us | Spark ANSI on, us |
| --- | ---: | ---: | ---: |
| Initial physical lowering | 9.899 | 764.630 | 765.872 |
| Projection name and expression resolution | n/a | 440.087 | 441.179 |
| Initial `ProjectionExec` construction | n/a | 259.672 | 263.439 |
| Lowering outside those projection spans | 9.899 | 64.960 | 64.811 |
| Physical optimizer, including checks | 74.608 | 922.123 | 925.820 |
| `EnforceDistribution` rule body | 21.911 | 445.848 | 446.098 |
| `FilterPushdown` rule body | 2.154 | 204.610 | 205.652 |
| Post-optimization `FilterPushdown` rule body | 0.321 | 195.594 | 196.174 |
| Runtime result-metadata expression walk | n/a | 34.093 | 34.083 |
| Runtime result-metadata schema recomputation | n/a | 897.137 | 896.946 |

Each entry is a median of five process medians. The two initial
projection spans are inside lowering. The remainder is computed
per sample and includes the asynchronous-expression check, scan
setup and observation overhead. The three rule bodies are inside
physical optimization. The final two rows are outside the default
planner. Timed child spans exclude their own allocation and storage,
while parent spans include that overhead.

Native planning eliminates the `SELECT *` projection. Spark keeps
qualified column aliases to preserve the bindings of its resolved
plan. The default planner walks those aliases to calculate physical
names and resolves their underlying columns again by name. Ordered
scan renames allow those columns to be bound by their proven ordinals.
The paired trial will keep the same `ProjectionExec` output and full
physical optimizer schedule. The remaining child costs stay measured.

## Validation and samples

The exact four wide plans matched between the canonical, observer and
inventory builds. Canonical planning checked 678 traces. Eight wide
overlays preserved Arrow schemas, plan properties and scan counters.
The wide fixture is in-memory. The canonical corpus also includes local
Delta scans with no data reads during planning. All 180 retained wide
plans recorded the exact 22 physical rule bodies and release schema
checks in order, alongside the logical-rule observations. No query ran.

- [Pre-clock protocol](planning-1390-round3-planner-split-protocol.md) at `5d60ad7c3541c037e3cdef47c3949d56a5b04ab0`.
- [Full span table](../planning_round3_1390/default-planner-split.csv) and [raw samples](../planning_round3_1390/default-planner-split-samples.jsonl.gz).
- [Build receipts, trace digests and process medians](../planning_round3_1390/default-planner-split-proof.json.gz).
