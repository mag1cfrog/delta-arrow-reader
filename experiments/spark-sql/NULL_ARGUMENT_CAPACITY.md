# Reserve only the partial-NULL argument vector

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
narrowed follow-up to the [rejected capacity experiment](ARGUMENT_CAPACITY.md).
The candidate changes only the partial-NULL branch: reserve the known function
arity before pushing the selected left operand and evaluating the remaining
arguments. It removes one vector growth and 192 requested bytes per affected
batch. Ordinary argument and field collection stay at the selected `c4bef63`
runtime; the rejected broad rewrite is not part of this candidate. This narrower
allocation improvement is retained for further experiments. Full-query and
planning performance acceptance remains open.

## Change and scope

[datafusion-null-argument-capacity.patch](datafusion-null-argument-capacity.patch)
replaces one vector initialization with a capacity reservation and a push. It
applies after the column-selection reuse patch in DataFusion physical-expr
54.1.0. Argument values, evaluation order, first-error returns, NULL short
circuiting and result validation are unchanged.

The baseline source and binary identities are those of the cold-format runtime.
Commit `b67f2fd` adds rejected-experiment evidence without selecting a runtime
change. Of the 85 recorded source/lock identities, only `scalar_function.rs`
changes here. The build restores all paths and three executable slots.

## Allocation checks

The unchanged physical-expression diagnostic validates every value and type
over 1,048,576 rows and checks the NULL short-circuit boundary. One before and
one after process run at each batch size. All ordinary Spark and native modes
have identical allocation calls, requested bytes and peak live bytes. Only
nested nullable Spark modes change, and their repeated evaluations agree.

| Nested NULL case | Batch | Allocation calls, before -> after | Requested bytes, before -> after | Peak live bytes, before -> after |
| --- | ---: | ---: | ---: | ---: |
| INT addition | 8,192 | 4,864 -> 4,736 | 27,323,608 -> 27,299,032 | 117,776 -> 117,776 |
| BIGINT addition | 8,192 | 4,864 -> 4,736 | 45,901,232 -> 45,876,656 | 234,816 -> 234,816 |
| INT addition | 256 | 135,168 -> 131,072 | 35,157,720 -> 34,371,288 | 4,576 -> 4,448 |
| BIGINT addition | 256 | 135,168 -> 131,072 | 53,898,672 -> 53,112,240 | 8,128 -> 8,096 |

Counts include output and temporary allocations, exclude prebuilt inputs and
plans, and return to zero measured live bytes after execution. Large-batch peaks
remain unchanged because the peak occurs elsewhere. These are requested heap
bytes, separate from process RSS and the benchmark's output-buffer sizes.
Incidental counting-pass timings are retained but excluded from latency claims.

## Correctness and query measurements

The candidate retains 616/616 focused agreements and 131 byte-identical error
payloads. All 6,784 regression observations retain their status and successful
values/types, with 6,403 agreement and no previously agreeing regression. All
116 Delta observations match the selected baseline; 18 adapter checks, 38
planner tests and 28 runner tests pass. Strict Delta/Spark results remain at
47 matches, 58 differences and 11 pending host cases.

The unchanged 48-case complete-query protocol uses 32 processes, separate
planning/execution measurements, four warmups and 15 samples per case. All four
same-binary control pairs must stay within +/-5% for calibration. The flagged
controls from the rejected experiment remain part of this comparison.

| Query case | Batch | Before ms | After ms | Median paired change | Calibration |
| --- | ---: | ---: | ---: | ---: | --- |
| ANSI INT nested NULL addition | 8,192 | 5.595 | 5.718 | +1.5% | Passes |
| ANSI INT nested NULL addition | 256 | 14.621 | 13.815 | -6.1% | Passes |
| ANSI BIGINT nested NULL addition | 256 | 17.777 | 17.712 | -1.9% | Passes |
| ANSI-off INT nullable multiplication | 256 | 4.463 | 4.456 | -0.2% | Passes |
| ANSI-off INT array multiplication | 256 | 3.871 | 3.904 | +6.6% | Passes |

Small-batch INT nested addition improves in all four change pairs, with ratios
0.940, 0.930, 0.938 and 0.954. The prior nullable-multiplication flag is not
reproduced by this narrower implementation. That does not clear all performance
questions: native array multiplication has a +6.55% median paired flag. Its
individual ratios are 1.011, 1.184, 1.120 and 0.991, while median process times
differ by 0.8%. Four small-batch planning cases also have flagged median paired
increases of 9.0%-31.4%, with uneven change ratios. All are retained in the
summary and raw records; they are not treated as resolved or assigned a cause.

Execution calibration passes for 1/48 large-batch and 21/48 small-batch cases;
planning passes for 29/48 and 23/48. The small calibrated differences within the
5% control tolerance do not establish speedups or regressions. Most large-batch
execution comparisons remain inconclusive. No unchanged reruns were used.

The separate eight-process hardware comparison runs all counters for 100% of
enabled time. Median paired user instructions decrease 0.004% at batch 8,192
and 0.158% at batch 256. Cycles change by -0.028% and +2.253%, respectively.
These are whole-process totals, including setup, validation, warmups, planning,
execution and serialization; they do not establish per-query latency or clear
the retained flags.

## Evidence and remaining work

[null-argument-capacity-results.json](null-argument-capacity-results.json)
identifies the sources, binaries, allocation change and every calibrated flag.
[The archive](null-argument-capacity-runs.json.gz) retains the complete protocol,
all samples, regression observations, counter CSV files and build/restoration
records. Its baseline is the [cold-format archive](checked-overflow-format-runs.json.gz).

```sh
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/null-argument-capacity-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

The checker recomputes the frozen comparisons without rebuilding or requiring a
private cache. This removes the extra NULL-path vector growth; ordinary UDF
argument vectors, checked-kernel loops, required selection/scatter and complete
performance acceptance stay in issue 195. Default-build integration remains
separate under issue 165.
