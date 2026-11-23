# Deferred set-session snapshot retest (#1390)

Keep the rebased `cbe4704f` trial. Both previously flagged native
controls passed in the new paired session. The only timing flag was
an unchanged Spark physical path, which passed the layout check.

All values below are in microseconds. Rows for the 25 paired queries
use the median across their individual logical planning medians.

| Path and phase | Staging | Candidate | Median paired delta | Native A/B floor |
| --- | ---: | ---: | ---: | ---: |
| Native-configured EXCEPT physical | 295.008 | 294.296 | -0.712 | 0.050 |
| Native-configured INTERSECT total | 385.355 | 384.875 | -1.823 | 1.242 |
| 25 paired statements, Spark ANSI off logical | 146.552 | 133.087 | -12.363 | n/a |
| 25 paired statements, Spark ANSI on logical | 146.652 | 131.995 | -12.644 | n/a |

The remaining paired logical excess over native-configured is
52.709 / 51.335 us. The candidate skips this snapshot when set lowering
finds no pending set. It takes the same snapshot and clears the query
start time on that copy when a set is present.

## Spark filter control

ANSI-off `paired/filter` crossed the physical gate with a positive
paired median of 2.425 us and a 0.861 us floor. The two process-median
ranges are disjoint. The observed medians were 265.262 and 267.687 us.

The untimed exact releases followed the same six physical planning
routines for that filter. All six selected compiled bodies match after
normalizing addresses, including their stack allocations and state
offsets. The 19 compared physical and driver bodies match, and the
relevant state definitions and dependency lockfile are byte-identical.
The product change reaches only logical set lowering. This flag is a
layout shift under the maintainer's rule. All other rows passed the
timing gate directly.

## Remaining planning gaps

These are differences from native-configured in the same retest.

| Statement and phase | Spark ANSI off, ms | Spark ANSI on, ms |
| --- | ---: | ---: |
| 400-column projection, physical | 7.270 | 7.287 |
| 1,000-literal IN list, logical | 1.015 | 1.031 |
| 200-branch CASE, logical | 1.408 | 1.438 |

The [wide rule trial](planning-1390-round3-scan-ordinals.md) and
[default planner trial](planning-1390-round3-physical-renames.md) record
why those candidates stayed out. The
[metadata change](planning-1390-round3-rename-metadata.md) removed most
of the excess in its part of the wide query. Its own residual is
reported with the measured spans. #1390 remains open for the remaining
planning costs.

## Validation

Both builds matched all 678 canonical traces, including exact Arrow
schemas, the logical and physical trees, ordering, partitioning,
input requirements and zero Delta data bytes read. Measurement ran no
queries. Formatting, Spark-crate Clippy and the full Linux workspace
passed 3,299 tests with three ignored. The runtime edits match the
archived source exactly. The [protocol](planning-1390-round3-session-snapshot-protocol.md)
was committed at `e0409c825f4f8c3c0e778aae3b7e66ba67908671` before timing.

- [All 360 comparisons](../planning_round3_1390/session-snapshot-summary.csv).
- [50 canonical processes and 13,500 retained pairs](../planning_round3_1390/session-snapshot-paired-samples.jsonl.gz).
- [Build receipts, trace digests and normalized control evidence](../planning_round3_1390/session-snapshot-proof.json.gz).
