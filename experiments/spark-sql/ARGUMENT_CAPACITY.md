# UDF argument capacity experiment

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
follow-up to cold overflow formatting at `c4bef63`. The candidate reduces
requested allocation bytes by 144 per ordinary binary-expression batch and 352
per nested NULL-expression batch. It is not selected: one calibrated native
query control slows 5.1%, and the cause remains unresolved. The selected runtime
stays at `c4bef63` while the next candidate is narrowed to NULL-path vector growth.

## Runtime change

The archived `candidate.patch` changes three vector constructions in DataFusion
54.1.0 `ScalarFunctionExpr`. Both
fallible argument/field collections reserve `self.args.len()` before pushing
their results. The partial-NULL branch reserves the same capacity before pushing
its selected left operand and evaluating the remaining arguments.

The interface still receives owned `Vec<ColumnarValue>` and `Vec<FieldRef>`.
Evaluation order, early error returns, column metadata, result validation and
NULL short circuiting are unchanged. The shared function also serves other UDFs,
which are included in the regression and query controls. No dependency, public
API, allocator policy or new expression type is introduced.

Only `scalar_function.rs` changes from the preceding optional runtime. All 85
recorded source identities are checked; other sources and Cargo.lock are
unchanged. The build restores those paths and three executable slots. Normal
vendored-build integration remains separate under issue 165.

## Allocation results

The unchanged physical-expression diagnostic checks every value and type over
1,048,576 rows and asserts the NULL short-circuit boundary. One before and one
after process run at each batch size. All native modes retain their exact counts;
the two repeated Spark modes agree. Incidental counting-pass timings are retained
but excluded from latency claims.

| INT case | Batch | Allocation calls, before -> after | Requested bytes, before -> after |
| --- | ---: | ---: | ---: |
| Array + array | 8,192 | 640 -> 640 | 4,252,672 -> 4,234,240 |
| Array + array | 256 | 20,480 -> 20,480 | 6,062,080 -> 5,472,256 |
| Array + scalar | 256 | 32,768 -> 32,768 | 6,766,592 -> 6,176,768 |
| Nested NULL addition | 8,192 | 4,864 -> 4,736 | 27,323,608 -> 27,278,552 |
| Nested NULL addition | 256 | 135,168 -> 131,072 | 35,157,720 -> 33,715,928 |

BIGINT has the same absolute reductions in calls and requested bytes. Counts
include expression output and temporary allocations, exclude prebuilt inputs
and plans, and return to zero measured live bytes after execution. Peak live
bytes for ordinary binary expressions fall by 144 per batch. Large-batch nested
peaks remain unchanged because the peak occurs elsewhere in evaluation.

The ordinary array/array path still performs five allocations per batch, compared
with three in the native checked diagnostic. The two owned argument vectors
remain part of the current UDF interface. This change removes excess capacity
without claiming that wrapper overhead has disappeared.

## Rejected filter replacement

Before this change, a separate experiment compared Arrow's existing multi-column
`filter_record_batch` helper with `FilterBuilder::new(mask).build()` without
materialized indices/slices. Both are existing Arrow APIs. The comparison covers
INT/BIGINT, two/eight columns, nullable/nonnullable data, five mask distributions
and both batch sizes. Independent expected rows check values, order, schema and
NULLs; boundary checks cover empty/all/none/nullable masks, sliced arrays, bitmap
offsets and oversized-mask errors.

Skipping materialization helps some nonnullable shapes but does not support a
broad replacement in the NULL path. In calibrated two-column nullable INT cases
at batch 256, alternating masks slow 22.6% and fragmented sparse masks slow
15.2%. Whole-process user instructions increase 23.5% at batch 8,192 and 18.0%
at batch 256. Those totals include input generation, validation, warmups and
all timed cases; they are not per-query latency.

The frozen ABBA protocol retains all eight processes and every sample.
Calibration passes for 29/40 large-batch and 30/40 small-batch filter cases.
The runtime keeps the existing Arrow strategy, with no new density or width
heuristic. The rejected experiment remains in the evidence archive.

## Correctness and query measurements

Correctness matches the preceding checkpoint: 616/616 focused observations and
131 byte-identical error payloads; unchanged status and successful values/types
in 6,784 regression observations, with 6,403 agreement; 116 unchanged Delta
observations; and 18 passing adapter checks. The 38 planner and 28 runner tests
pass. Existing strict Delta/Spark results remain at 47 matches, 58 differences
and 11 pending host cases.

The unchanged 48-case query benchmark uses the preceding frozen 32-process
protocol: four warmups, 15 samples, CPU 2, separate planning/execution timing and
four same-binary control pairs per case. Every control ratio must stay within
+/-5% for calibration. All outputs are checked before timing.

| Query case | Batch | Before ms | After ms | Median paired change | Calibration |
| --- | ---: | ---: | ---: | ---: | --- |
| ANSI INT scalar addition | 256 | 4.907 | 4.648 | -5.0% | Passes |
| ANSI INT nested NULL addition | 256 | 15.041 | 14.297 | -6.0% | Passes |
| ANSI BIGINT nested NULL addition | 256 | 17.796 | 17.164 | -3.7% | Passes |
| ANSI INT array addition | 256 | 5.341 | 5.000 | -7.0% | Fails |
| ANSI-off INT nullable multiplication | 256 | 4.417 | 4.674 | +5.1% | Passes |

Execution calibration passes for 5/48 large-batch and 18/48 small-batch cases;
planning passes for 28/48 and 31/48. The flagged multiplication query uses native
`BinaryExpr`, and the recorded plans are identical before and after. Planning of
ANSI-off nullable BIGINT multiplication also has a calibrated +24.7% median
paired ratio. Its individual change ratios are 2.094, 0.995, 1.499 and 0.811,
while the median process times are 1.267 and 1.269 ms. Those mixed directions
do not establish a consistent planning slowdown, but the flag is retained.

The separate eight-process counter comparison reports whole-process user
instructions down 0.024% at batch 8,192 and 0.491% at batch 256. All counters
run for 100% of enabled time. These totals include setup, validation, warmups,
planning, execution and serialization; they do not clear the query-level flags.

## Follow-up on the native control

A new bounded comparison uses the existing physical diagnostic to change mode
order within each case. Forward order evaluates wrapping arithmetic before the
Spark functions; reverse order evaluates it afterward. Case order stays fixed.
This changes the question from whole-query latency to prebuilt expression cost
and possible mode-order association. The source, schedule and all observations
are retained; no unchanged timing rerun was used.

The 16 processes cover both variants, orders and batch sizes. Four
same-variant/same-order control ratios must all fall within +/-5%. Calibration
passes for 56/100 large-batch and 45/100 small-batch case/modes. Small-batch native
nullable INT multiplication passes calibration with a median paired ratio of
0.987. Forward change ratios are 1.028 and 0.968; reverse ratios are 0.980 and
0.994. This does not reproduce the complete-query increase or identify a unique
cause. The physical diagnostic has an inactive allocator hook and different
output lifetimes, and does not time planning or collection.

The broad capacity rewrite is therefore retained as a rejected experiment,
including its allocation gains and performance flags. The next runtime candidate
will only reserve capacity in the partial-NULL branch. The two general UDF
argument vectors, required selection/scatter and native checked-kernel work
remain open in issue 195.

## Recheck the evidence

[argument-capacity-results.json](argument-capacity-results.json) summarizes the
decision and exact source/binary identities.
[The archive](argument-capacity-runs.json.gz) contains the rejected patch, frozen
protocols, all query and physical samples, both filter strategies, allocation
captures, hardware counters, regression observations and build/restoration logs.
It references the [cold-format baseline](checked-overflow-format-runs.json.gz).

```sh
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/argument-capacity-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

The checker recomputes the comparisons from frozen records without a build or
private cache. The recorded runtime uses Sail 0.7.1 provenance, DataFusion
54.1.0, Arrow 58.4.0, Spark 4.2.0 and Rust 1.98.1.
