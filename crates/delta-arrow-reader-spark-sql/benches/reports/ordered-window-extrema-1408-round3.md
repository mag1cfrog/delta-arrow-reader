# MAX_BY and MIN_BY over their window order key (#1408)

Ordering the window by the key is insufficient for a one-position
read. The Spark captures show that NULL keys can occupy an endpoint
even when the window contains a usable key. They also show different
tie winners at equal keys. The general moving-frame accumulator
stays in place. The measurements use one staging release binary.

## When the frame endpoint differs

The independent Spark 4.2.0 session ran 20 cases in each ANSI mode.
They cover duplicate keys, NULL keys and values, ascending and
descending order, both NULL placements, and bounded ROWS, bounded
RANGE, growing, whole-partition and potentially empty frames.
Both modes produced the same captured values and executed types.
Across the 180 rows in each mode, the order-selected endpoint
differs from MAX_BY on 112 rows and from MIN_BY on 112.

| Window and row | Spark by-key result | Value at the endpoint |
| --- | ---: | ---: |
| Whole partition, ASC NULLS LAST, row 1, MAX_BY | 35 | 60 at row 6, whose key is NULL |
| Whole partition, ASC NULLS FIRST, row 1, MIN_BY | 12 | 40 at row 0, whose key is NULL |
| Bounded RANGE, ASC NULLS FIRST, row 1, MIN_BY | 12 | 11 at row 1, tied with row 2 |
| Whole partition, ASC NULLS LAST, row 8, MAX_BY and MIN_BY | NULL | 80 or 70 in the all-NULL-key partition |

Checking the endpoint key and returning NULL still gives the wrong
answer in the first two cases. Ignoring NULL values is also wrong.
In bounded ROWS with ASC NULLS FIRST, row 4 has the maximum key and
a NULL value. Spark returns NULL for MAX_BY at that row.

Spark skips NULL keys and can update the value when a tied key is
encountered. This capture records the tie result for this execution.
DAR's existing moving accumulator chooses the earliest candidate on
equal keys and retains that value even when it is NULL. Substituting
an endpoint from key order alone cannot preserve those rules.
[Spark aggregate source](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/MaxByAndMinBy.scala#L57-L128)
[Current DAR accumulator](../../vendor/sail/crates/sail-function/src/aggregate/max_min_by.rs)

## Three references on the same frame

The frozen million-row cases order by the unique `id` and use
`ROWS BETWEEN 4 PRECEDING AND CURRENT ROW`. The by-key statements
match their native endpoint results and executed Int64 types.
The MAX and MIN scale queries separately match Spark MAX and MIN.
All four preflights compare every result row.

| Path | Median [range], ns per input row | Native A/A floor |
| --- | ---: | ---: |
| Spark MAX_BY, ANSI off | 375.580 [370.511, 379.260] | |
| Spark MAX_BY, ANSI on | 376.609 [371.894, 382.271] | |
| Native LAST_VALUE | 147.757 [146.244, 153.212] | 2.885 |
| Native MAX | 360.769 [359.019, 369.195] | 1.976 |
| Spark MIN_BY, ANSI off | 373.356 [370.006, 374.683] | |
| Spark MIN_BY, ANSI on | 368.089 [366.091, 375.162] | |
| Native FIRST_VALUE | 147.075 [146.156, 148.728] | 1.255 |
| Native MIN | 402.681 [397.800, 406.012] | 1.979 |

Times are above the same input projection. Each range spans nine
samples. MAX_BY is 227.823 / 228.852 ns per row above LAST_VALUE
and 14.812 / 15.840 above MAX in ANSI off / on. MIN_BY is
226.280 / 221.014 above FIRST_VALUE and 29.325 / 34.591 below
MIN. The endpoint references read a position. The Spark operators
still update and retract value/key candidates as the frame moves.
The MAX and MIN figures give the scale of window extrema work.
#1408 remains open for the endpoint-reference differences.

## Evidence

The [protocol](../window_extrema_1408_round3/protocol.md) and cases
were committed at `cf2c7970`. The driver and 40
[Spark observations](../window_extrema_1408_round3/spark-capture.jsonl.gz)
were committed at `028ca474` before the release run. All 36
processes completed and retained
[108 samples](../window_extrema_1408_round3/reference-samples.jsonl.gz)
from 2026-11-23 05:17:54 to 05:19:23 UTC. The release source was
`f5ea78cb8485523738775056d28a2232e9c32762`, with identical
runtime source to staging `8e790f97f4730ad4c46c5c11a7ae320cb8c54cf6`.
Executable SHA256
`95af9da86355f99940a1c3834422a3f878b2e203410caabc55f124d8a666f444`.
