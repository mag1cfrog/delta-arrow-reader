# Reuse first-argument evaluation before reserving value capacity

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
follow-up to the [checked-buffer checkpoint](CHECKED_BUFFER_LENGTH.md). Reuse
the existing first-argument evaluation and known-arity collection loop for
ordinary scalar-function calls. An ordinary binary call requests 128 fewer
heap bytes, without changing its allocation count or adding work when the
first argument fails. The experimental checkpoint retains this allocation
improvement; complete query/planning performance acceptance remains open.

## Runtime change

[datafusion-argument-value-collection.patch](datafusion-argument-value-collection.patch)
changes only `ScalarFunctionExpr::evaluate` in DataFusion physical-expr 54.1.0.
It deletes six source lines by sharing the existing loop. Zero-argument calls
still invoke with an empty vector. Arguments retain their evaluation order,
first-error return, metadata and NULL short circuit. Partial-NULL filtering,
scattering and argument-field collection remain unchanged. No new API,
physical-expression type or dependency is introduced.

The earlier eager-reservation candidate allocated the vector before evaluating
the first argument. It passed the SQL regressions, but added an unnecessary
allocation when that argument failed. For the two-argument control, the old
path uses 3 allocations and 316 bytes; eager reservation uses 4 and 444. The
shared-first-argument candidate keeps 3 and 316, while normal calls retain the
128-byte reduction. The eager candidate is rejected and its complete results
are retained, including its inconsistent calibrated INT multiplication flag.

The selected baseline is optional runtime `70422df`. Only `scalar_function.rs`
changes among 86 source/lock identities; the lock and dependency graph stay
identical. All source paths and three executable slots are restored after the
build. These patches are not yet in the default vendored build.

## Validation

Twenty-seven untimed module observations cover 0/1/2/4 arguments, preserved
metadata, and first/later argument errors. All outcomes match, and measured
live bytes return to zero. Embedded modules establish these contracts and
allocation counts; they are not used for latency comparisons.

The full runtime retains 616/616 focused agreements, including 131 identical
error payloads. All 6,784 existing observations retain status and successful
values/types, with 6,403 agreement and no previously agreeing regressions.
All 116 Delta observations match the selected baseline; 18 adapter checks,
38 planner tests and 28 runner tests pass. Strict Delta/Spark results remain
47 matches, 58 differences and 11 pending host cases.

The unchanged physical diagnostic validates values/types over 1,048,576 rows.
At each batch size, all 60 native observations keep identical allocation
calls, requested bytes and peak live bytes. The 40 Spark observations each
save 128 bytes per ordinary binary invocation, with unchanged call counts
and no increased peak. Nested NULL expressions benefit through their
ordinary inner expression; the partial-NULL collection itself is unchanged.
For example, small-batch INT array addition drops from 6,062,080 to 5,537,792
requested bytes, with 20,480 allocations in either variant. All live bytes
return to zero after evaluation. Counting-pass timings are not latency evidence.

## Query performance

The frozen complete-query comparison uses 32 processes, 48 case/modes,
1,048,576 rows, four warmups and 15 samples. Planning and execution are
measured separately. Every same-binary pair must be within +/-5% for
calibration; all samples and failed controls are retained without unchanged
reruns. The separate eight-process counter comparison measures whole-process
work, including setup, validation, planning, execution and serialization.

Execution calibration passes for 7/48 large-batch and 26/48 small-batch cases.
Every calibrated execution change is within the 5% control tolerance, so this
comparison establishes neither a query speedup nor complete absence of
regression. Most large-batch execution comparisons remain inconclusive.

Planning calibration passes for 36/48 and 29/48 cases. The small-batch ANSI-off
Decimal control retains a +11.1% median paired planning flag, with change ratios
0.983, 1.030, 1.403 and 1.193. Its mixed directions remain recorded without
assigning a cause or repeating the unchanged comparison.

All hardware counters run for 100% of enabled time. Whole-process instructions
change by -0.011% at batch 8,192 and -0.297% at batch 256; cycles change by
+0.630% and -0.521%. These totals do not establish per-query latency or clear
the retained planning flag.

## Evidence and remaining work

[argument-value-collection-results.json](argument-value-collection-results.json)
records both candidates, source/binary identities, allocation changes and every
calibrated flag. [The archive](argument-value-collection-runs.json.gz) contains
all original measurements, rejected-candidate results, contracts, protocols,
and build/restoration records. Its baseline is the
[checked-buffer archive](checked-buffer-length-runs.json.gz).

```sh
python3 - <<'PYTHON'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/argument-value-collection-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PYTHON
```

The checker needs no private cache or rebuild. The argument and field vectors
still allocate; scalar-function dispatch, checked-kernel work, required NULL
selection/scatter and full-query/planning acceptance remain in issue 195.
Default-build integration remains separate under issue 165.
