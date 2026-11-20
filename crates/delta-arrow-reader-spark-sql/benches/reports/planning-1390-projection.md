# Wide projection planning after indexing column names (#1390)

The 400-column Spark query spends about 0.33 ms less in the projection
rule. Its physical planning still takes about 15.57 ms, compared with
2.50 ms in native-configured. The single projection change is complete.

## Projection rule and physical planning

| Configuration | Before rule (ms) | After rule (ms) | After rule A/B (ms) | Before physical (ms) | After physical (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| native-plain | 0.514917 | 0.512391 | 0.000530 | 2.335198 | 2.351237 |
| native-configured | 0.512571 | 0.512692 | 0.000139 | 2.504222 | 2.501957 |
| spark-off | 4.848326 | 4.522741 | 0.029113 | 15.841086 | 15.568680 |
| spark-on | 4.794165 | 4.463801 | 0.011663 | 15.776265 | 15.571364 |

The rule times sum its two body invocations per sample. The original
observed attribution supplies the rule's before times. The parser
slice supplies the canonical before times. Each mode has its own
fresh-process rule A/B comparison. [Rule table](planning-1390-projection-rule.csv)

The lookup index removes repeated scans for unique column names in wide
stacked projections. Other work in `OptimizeProjections::rewrite`,
including required-index discovery, expression rewrites and projection
construction, still runs. About 4.01 ms (ANSI off) and 3.95 ms (ANSI on)
of rule time remain above the native-configured rule. These measurements
do not subdivide that remaining rule time.

On the 400-column ANSI-off query the observed analyzer costs 4.063 ms
versus 1.000 ms native-configured. `TypeCoercion::analyze` accounts for
2.295 versus 0.753 ms within it. The Spark planner costs 3.074 ms versus
0.071 ms, including 1.168 ms to align Spark result metadata. Final name
projection costs 0.309 ms on the Spark path. These intervals overlap
their parents. The canonical physical phase gap remains 13.067 ms with
ANSI off and 13.069 ms with ANSI on.

## Native controls

The same canonical release binary runs both native configurations and
both Spark ANSI modes. Across all native phases and shapes, no slower
candidate median also had a candidate minimum above the prior maximum
and exceeded the larger same-shape native A/B difference from the two
rounds. Each native 400-column rule median stayed within the larger of its
two observed A/B differences. The native-plain physical median for that query
rose 0.0160 ms, with overlapping before/after process ranges.

A separate focused test binary built from the same source executes the
400-column stacked projection in plain and configured native sessions.
Both match the expected ordered Arrow schema and record batch, including
values, NULLs and executed types. The rule's additional differential
test compares 882 cases to the original ordered field lookup. The
original repeated-computation protection has its own test. All three pass.

## Planning costs still open

| Shape and phase | Original ANSI off (ms) | Final ANSI off (ms) | Final ANSI on (ms) | Final native-configured (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000-literal IN, logical | 10.649191 | 1.787991 | 1.774637 | 0.763768 |
| 200-branch CASE, logical | 9.197194 | 2.615068 | 2.653889 | 1.261804 |
| 400 columns, physical | 15.989831 | 15.568680 | 15.571364 | 2.501957 |

All 25 paired queries remain above their native logical A/B difference in
each ANSI mode. The median logical gap is 0.0656 ms with ANSI off and
0.0650 ms with ANSI on. IN and CASE still pay Spark lexing, parser
construction, grammar traversal and resolution after the Rich
error-collection change. The remaining logical gap has no new breakdown
among these steps. The [phase table](planning-1390-projection-walls.csv)
covers all 30 inputs, both native configurations, both ANSI modes and
all phases, with before/after medians, ranges and A/B differences.

#1390 stays open for the remaining costs. The two bounded changes are
complete.

## Validation and evidence

The initial differential test accidentally triggered the original
repeated-computation guard. The fixture was corrected to keep that guard
out of the lookup test. Production source stayed the same during this
correction. The focused rerun, formatting, strict root-crate Clippy and
complete Linux workspace passed. All 3,186 prior named outcomes match.
The final suite has 3,186 passed, zero failed and three ignored.

The two release builds produced 678 exact public plan/schema snapshots
each, byte-identical to each other and the original canonical reference.
All traces recorded zero Delta data reads. Each of the 78 process stderr
files is empty. The [committed protocol](planning-1390-projection-protocol.md)
set the two schedules before timing. The [canonical samples](planning-1390-projection-samples.jsonl.gz)
and [rule samples](planning-1390-projection-rule-samples.jsonl.gz) preserve
8,100 canonical samples and 10,800 observed rule-body samples. The
[receipt](planning-1390-projection-proof.json.gz) binds the test inventory,
builds, trace and measurement hashes.
