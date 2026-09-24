# Set the checked output buffer length once

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
follow-up to the [partial-NULL capacity checkpoint](NULL_ARGUMENT_CAPACITY.md).
The selected experimental candidate removes two instructions from each
successful iteration of the observed INT array loop: an output byte-offset
increment and a buffer-length store. Overflow checking and the first-error
branch remain. One calibrated large-batch query improves 8.6%; broader
performance acceptance stays open.

## Runtime change

[arrow-checked-buffer-length.patch](arrow-checked-buffer-length.patch) changes
only `try_binary_no_nulls` in Arrow arith 58.4.0. Write into its existing output
allocation by index, then set the initialized byte length after every callback
succeeds. The private helper keeps its signature, allocation size, input order
and error returns. The public caller still validates lengths and handles empty
or nullable arrays. No dependency version or public API changes.

The pointer uses the same capacity and alignment as the former
`push_unchecked` calls. If a callback returns an error or panics, the buffer has
length zero and releases its allocation without reading partially initialized
values. Arrow native values are `Copy`; there are no element destructors to
skip. Checks cover sliced inputs, bitmap boundaries, wider output values,
callback order, first-error termination and zero live allocated bytes after
success, error and panic.

The baseline is the selected optional runtime at `e17be5f`. Of its recorded
sources, only Arrow arity changes. The additional local crate override removes
Arrow arith's registry identity and checksum from the lock; package versions
and the dependency graph stay unchanged. The build restores 86 source/lock
paths and three executable slots. Default vendored sources remain unchanged.

## Alternatives and native measurements

The existing trusted-length buffer constructor was tried first. Its inline
prototype improved addition but slowed multiplication. Assembly showed that
its baseline had already hoisted the length store out of the loop, unlike the
actual runtime, so it could not isolate the intended change.

The next diagnostic calls compiled Arrow numeric entry points, with matched
release profiles and identical dependencies. Its baseline loop matches the
selected runtime instruction for instruction after normalizing branch targets.
The trusted-length constructor still slows calibrated INT multiplication by
17.6% at batch 8,192. It is rejected, with both prototypes retained.

The smaller direct-write candidate passes calibration for 18/24 large-batch
and 20/24 small-batch library cases. INT array addition/subtraction improve
32.5%-32.9% at batch 8,192 and 16.5%-16.9% at batch 256. No calibrated
multiplication slowdown appears. These are library-level timings, not complete
SQL queries. Scalar and wrapping paths are controls. In particular, scalar
INT addition becomes faster despite identical unrelocated instruction bytes;
that timing difference is not attributed to the changed array loop.

Each native prototype retains eight processes, 31 samples after four warmups,
1,048,576 rows and both same-variant controls within +/-5% for calibration.
The original inline prototype has 12 cases; library comparisons have 24,
including scalar and wrapping controls. No unchanged runs were repeated to
obtain favorable measurements.

Because other types call the same private helper, a separate eight-process
comparison covers 52 combinations of smaller signed integers, unsigned
integers, Decimal128/256, duration/date/timestamp arithmetic and BIGINT
division/remainder. Every output matches. Calibration passes for 42/52
large-batch and 33/52 small-batch cases, with no calibrated slowdown above 5%.
Uncalibrated cases remain inconclusive. This comparison reuses the exact two
compiled libraries; it introduces no further runtime change.

## Correctness and full-query validation

The candidate passes all 214 existing Arrow tests and 288 callback, error,
panic, value and allocation checks. The unchanged SQL suites retain 616/616
focused agreements, including 131 byte-identical error payloads. All 6,784
regression observations keep their status and successful values/types, with
6,403 agreement and no previously agreeing regression. All 116 Delta
observations match the selected baseline; 18 adapter checks, 38 planner tests
and 28 runner tests pass. Existing strict Delta/Spark results remain 47 matches,
58 differences and 11 pending host cases.

All 100 diagnostic case/mode allocation observations at each batch size retain
identical allocation calls, requested bytes and peak live bytes. The counting
pass's incidental timings are excluded from latency claims.

The unchanged 48-case complete-query comparison retains 32 processes, four
warmups and 15 samples per case. Planning and execution are measured separately;
all four same-binary pairs must remain within +/-5% for calibration.

| ANSI query | Batch | Before ms | After ms | Median paired change | Calibration |
| --- | ---: | ---: | ---: | ---: | --- |
| INT array addition | 8,192 | 1.691 | 1.549 | -8.6% | Passes |
| INT array subtraction | 8,192 | 0.998 | 0.832 | -29.2% | Fails |
| INT array subtraction | 256 | 5.377 | 5.162 | -3.2% | Passes |
| INT array multiplication | 256 | 5.385 | 5.384 | +0.9% | Passes |
| INT nested NULL addition | 256 | 13.847 | 13.755 | -0.3% | Passes |
| BIGINT array multiplication | 256 | 6.802 | 6.725 | -1.4% | Passes |

The large-batch addition change-pair ratios are 0.904, 0.913, 1.003 and 0.915.
Small calibrated differences within the 5% control tolerance do not establish
speedups or regressions. Execution calibration passes for 5/48 large-batch and
20/48 small-batch cases; most large-batch timing results remain inconclusive.
There is no calibrated execution increase above 5% in this comparison.

Planning calibration passes for 32/48 and 31/48 cases. ANSI-off BIGINT nested
addition at batch 8,192 retains a +6.7% planning flag, with change-pair ratios
1.056, 1.078, 1.081 and 0.966. This query does not use the changed checked
kernel. The flag remains open without assigning a cause or repeating the
unchanged comparison.

The separate eight-process hardware comparison runs all counters for 100% of
enabled time. Median paired user instructions fall 0.411% at batch 8,192 and
0.304% at batch 256; cycles change by +0.178% and +0.096%. These whole-process
totals include setup, validation, planning, execution and serialization. They
do not clear the planning flag or establish general latency parity.

## Evidence and remaining work

[checked-buffer-length-results.json](checked-buffer-length-results.json)
records source/binary identities, all results and calibration failures.
[The archive](checked-buffer-length-runs.json.gz) includes the three native
prototypes, shared-caller controls, upstream tests, assembly, build/restoration
records and every raw sample. Its runtime baseline is the
[partial-NULL capacity archive](null-argument-capacity-runs.json.gz).

```sh
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/checked-buffer-length-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

The checker needs no private cache or rebuild. Remaining checked-kernel work,
ordinary UDF argument vectors, required NULL selection/scatter and full-query
performance acceptance stay in issue 195. This does not establish wrapping
parity, complete Spark compatibility or default-build integration.
