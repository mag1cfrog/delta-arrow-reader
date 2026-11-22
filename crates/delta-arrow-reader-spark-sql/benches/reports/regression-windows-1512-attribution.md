# Regression window attribution (#1512)

The independent reference is Spark 4.2.0, revision `32f72996`. The DAR baseline
is staging commit `b78d82bf`. Both read the same 96 synthetic rows from #1406's
seeded fixture and run with ANSI off and on.

## Why the five-row window differs

DataFusion's sliding window adds rows entering the upper bound and calls
`retract_batch` to remove rows leaving the lower bound. Sail's retraction
updates the means, sums, second moments and cross moments in place. Plain
aggregation uses `update_batch` and can merge partial states, but does not
subtract outgoing rows. Spark's bounded sliding frame resets its aggregation
buffer and adds the remaining rows in order when the frame changes.
[DataFusion sliding window](../../vendor/dependencies/datafusion-physical-expr/src/window/sliding_aggregate.rs),
[Sail regression state](../../vendor/sail/crates/sail-function/src/aggregate/regr.rs),
[Spark sliding frame](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowFunctionFrame.scala#L435-L466).

The [arithmetic replay](../../tests/fixtures/regression-windows-1512/replay_original_frames.py)
compares the current recurrence with rebuilding each frame from its member
rows. The current recurrence reproduces every captured #1406 DAR value bit for
bit. The columns below count differences against Spark out of 96 rows in each
ANSI mode.

| Window function | State used for the result | Current | Rebuild only | Correct R2 only | Both |
| --- | --- | ---: | ---: | ---: | ---: |
| `regr_avgy` | Dependent sum divided by pair count | 8 | 0 | 8 | 0 |
| `regr_intercept` | Both means and the fitted slope | 9 | 0 | 9 | 0 |
| `regr_r2` | Correlation moments and the zero-variance branches | 13 | 2 | 11 | 0 |
| `regr_slope` | Cross moment divided by independent second moment | 14 | 0 | 14 | 0 |
| `regr_sxx` | Independent second moment | 7 | 0 | 7 | 0 |
| `regr_sxy` | Cross moment in correlation order | 10 | 0 | 10 | 0 |
| `regr_syy` | Dependent second moment | 10 | 0 | 10 | 0 |
| `regr_avgx` | Independent sum divided by pair count | 0 | 0 | 0 | 0 |
| `regr_count` | Pair count | 0 | 0 | 0 | 0 |

### The two NULL results

At IDs 4099 and 4106, the dependent value is `0.7` in each of two pairs.
The independent values are `7, 9` and `5, 10`, respectively. Both the running
and rebuilt states have independent second moments of `2.0` and `12.5`, and
dependent second moments of zero. Sail checks dependent variance first and
returns NULL. Spark checks independent variance first. It returns NULL if
that variance is zero and returns `1.0` when only dependent variance is zero.
The replay changes the branch order separately from the frame update and
recovers both `1.0` values.
[Spark R2 evaluation](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/linearRegression.scala#L140-L154).

## Current staging results

The staging release executed all 42 statements and read all 96 fixture rows
per statement. Every original window recorded by #1406 reproduces its DAR values and types.
Across the wider capture there are 1,391 differing window values per ANSI
mode. A one-row frame can retain arithmetic from the previous row under
retraction. The growing frame never retracts and still has two R2 differences,
which isolate the zero-variance branch.

The plain-aggregate controls add three pre-existing value differences per
mode: one last-digit intercept difference and both constant-argument R2
cases. They preserve the baseline DAR values in the fix. `regr_count` also
retains its existing unsigned 64-bit executed type in DAR. Spark reports a
signed long. The correction covers the seven DOUBLE windows only.
[Staging observations](../../tests/fixtures/regression-windows-1512/dar-baseline.jsonl).

## References

The [Spark capture](../../tests/fixtures/regression-windows-1512/spark-results.jsonl)
includes every original regression window plus two other ROWS frames, a RANGE
frame, a growing frame, a whole partition, NULL in either argument, a constant
on each side and single-row frames. The three plain aggregate controls
include both constant-argument cases. Values store the exact DOUBLE bits and
the types come from Spark's executed plan. All original rows also present in
the #1406 comparison agree with that capture.

Run `capture_spark.py OUTPUT.jsonl.gz` from the fixture directory with Python
3.12, Java 21 and PySpark 4.2.0. Decompress the output before passing it to
`python3 replay_original_frames.py spark-results.jsonl`. The capture and replay
use the committed fixture and do not contact a remote table.
