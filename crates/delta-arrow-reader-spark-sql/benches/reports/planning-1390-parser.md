# Statement parsing after the error-tracking change (#1390)

The logical phase fell from about 10.65 ms to 1.79 ms for the 1,000-literal
IN list, and from about 9.2 ms to 2.65 ms for the 200-branch CASE. The
statement parser now collects Rich errors when its first attempt fails.
The first attempt uses the same grammar with a parser graph that can be freed.

| Shape and mode | Before logical (ms) | After logical (ms) | After native-configured (ms) | Gap (ms) | After native A/B difference (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000-literal IN, ANSI off | 10.649191 | 1.789575 | 0.754341 | 1.035234 | 0.003036 |
| 1,000-literal IN, ANSI on | 10.645915 | 1.795004 | 0.754341 | 1.040663 | 0.003036 |
| 200-branch CASE, ANSI off | 9.197194 | 2.654451 | 1.267264 | 1.387187 | 0.005170 |
| 200-branch CASE, ANSI on | 9.216149 | 2.633812 | 1.267264 | 1.366548 | 0.005170 |

The middle paired statement improved by 0.0818 ms (ANSI off) and 0.0820 ms
(ANSI on), taking the median change across the 25 paired shapes. Their
median positive logical gaps to native-configured are now 0.0656 and
0.0655 ms. All 25 in each mode still cost more than their native A/B
difference. The remaining IN and CASE gaps are also above noise.

The cost of collecting full errors on successful statements is gone from
the source path. Parser construction, lexing, grammar traversal and
resolution still run. The follow-up measures the whole logical phase, so
it cannot apportion the remaining gap among them. The original per-stage
values and baseline process spreads are in the [attribution report](planning-1390.md)
and [stage table](planning-1390-stages.csv). The candidate [phase table](planning-1390-parser-walls.csv)
includes each of 30 shapes in four configurations, all three phase medians,
ranges, interquartile ranges and both rounds' native A/B differences.

Native-configured control median changes across shapes were +0.000165 ms
logical, +0.000066 ms physical and -0.000009 ms total. Individual process
ranges shifted, including a +0.01465 ms median change in six-table join
total planning. Neither native configuration uses the changed parser.

## Validation

The release parser returned byte-identical AST snapshots for the 309
frozen statements. All 678 public plan and exact Arrow schema snapshots
match the retained reference, including both ANSI modes and both native
configurations. Every trace recorded zero Delta data reads. One new test
checks Rich and lightweight paths and exact errors on 100 controls.

In the 512-call parser memory probe, the valid case's VmSize growth fell
from 25,228 KiB to zero. Invalid input grew by 25,264 KiB versus 25,188 KiB
before, consistent with the original Rich fallback allocation curve.
The new temporary parse does extra work when it fails. This measurement
does not estimate malformed-statement latency.

The initial workspace run and a focused rerun found incorrect route
labels in the new fixtures. The original release parser was used to check
all 100 routes, and the fixture categories were corrected. The focused
test, formatting, root-crate Clippy and the complete Linux workspace then
passed, with 3,183 passed and three ignored. All 3,185 prior named outcomes
match. The timed release predates the fixture-only correction. Its product
source and benchmark driver are the same as the tested version.

The [committed protocol](planning-1390-parser-protocol.md) preceded the
30-process, 8,100-sample schedule. The [raw samples](planning-1390-parser-samples.jsonl.gz)
and [validation receipt](planning-1390-parser-proof.json.gz) retain the
exact binary, source, plan comparison and named-test evidence. This
schedule measured planning only. #1390's next slice addresses the
400-column projection cost.
