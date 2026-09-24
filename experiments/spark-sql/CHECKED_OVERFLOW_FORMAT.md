# Keep overflow formatting out of checked arithmetic loops

[Issue 195](https://github.com/mag1cfrog/delta-arrow-reader/issues/195) owns this
follow-up to column field reuse at `1b5cb6f`. Moving error text formatting to a
cold function improves several calibrated complete-query cases. At batch size
8,192, INT array addition improves 18.2%, scalar addition 32.5% and array
multiplication 28.9%. Remaining checked-kernel, argument and NULL-selection costs
stay open; this does not establish parity with wrapping arithmetic.

## Runtime change

[arrow-checked-overflow-format.patch](arrow-checked-overflow-format.patch) changes
the existing Arrow 58.4.0 checked `+`, `-` and `*` implementations. A private
`#[cold]`, `#[inline(never)]` function returns the formatted error text. The
caller still performs the primitive checked operation and constructs the same
`ArrowError::ArithmeticOverflow` variant. Wrapping arithmetic, public APIs and
dependencies are unchanged.

The recorded INT32 array-add assembly retains the overflow branch but removes
the two operand stack stores from each successful iteration. Those operands
were being saved for possible error formatting. The loop remains scalar, with
other stores and per-row checking still present. This is not a SIMD change.

The patch uses a local copy of the published Arrow crate. Only
`src/arithmetic.rs` differs. Cargo.lock removes that package's registry identity
and checksum for the local path override; versions and dependency edges stay
the same. The previous 84 recorded source identities remain unchanged except
for this lock identity; Arrow arithmetic is the 85th recorded source. The build
restores all 85 source/lock paths and three executable slots. Shared registry and
default vendored sources are unchanged. Normal-build integration belongs to
issue 165.

## Rejected prototype and selected implementation

The first prototype moved construction of the whole `ArrowError` into a cold
function. It was rejected: calibrated small-batch nullable BIGINT kernels slowed
about 8%-10%, and whole-process user instructions increased 12.4%-15.6%. Its
assembly includes additional result-discriminant handling. The evidence does
not attribute every instruction change to one compiler decision.

Returning only `String` keeps the error variant known at the call site. The
second prototype passes calibration in 21/24 kernel cases, with calibrated
improvements of about 8%-49% and no repeat of the nullable BIGINT regression.
Both prototypes use Arrow's existing `try_binary`, validate output against the
native kernel and check overflow boundaries and masked errors. Their four-process
ABBA protocol uses 1,048,576 rows, batch sizes 8,192 and 256, four warmups and 31
samples. Both same-variant process ratios must fall within +/-5%.

All source, samples, counters, assembly and unfavorable results from both
prototypes are retained. The full runtime validation below, rather than the
prototype alone, supports retaining the text-only change.

## Complete-query measurements

The unchanged 48-case query benchmark compares the preceding checked runtime
with the patched checked runtime. The frozen protocol uses 32 processes across
both batch sizes, CPU 2, four warmups and 15 samples per case. Planning and
execution are measured separately. Each case must pass all four same-binary
control pairs within +/-5% before its elapsed-time comparison is considered
calibrated. Every output is validated outside timing.

| ANSI case | Batch | Before ms | After ms | Median paired change | Calibration |
| --- | ---: | ---: | ---: | ---: | --- |
| INT array addition | 8,192 | 2.130 | 1.732 | -18.2% | Passes |
| INT scalar addition | 8,192 | 1.209 | 0.821 | -32.5% | Passes |
| INT array multiplication | 8,192 | 1.416 | 1.012 | -28.9% | Passes |
| INT nested NULL addition | 8,192 | 6.272 | 5.651 | -9.9% | Passes |
| BIGINT array addition | 8,192 | 3.963 | 3.712 | -6.1% | Passes |
| INT array addition | 256 | 5.642 | 5.304 | -5.3% | Fails |
| INT nested NULL addition | 256 | 15.676 | 14.932 | -4.7% | Passes |
| BIGINT nested NULL addition | 256 | 18.366 | 18.003 | -1.9% | Fails |

Execution calibration passes for 7/48 large-batch and 13/48 small-batch cases;
planning passes for 39/48 and 22/48. No calibrated execution case exceeds a 5%
slowdown. Most cases remain inconclusive, including several unaffected controls.
The small-batch 4.7% difference is within the control tolerance. These results
do not establish a general absence of regression. All original samples and
calibration failures are retained, without unchanged reruns.

A separate eight-process ABBA hardware-counter comparison uses the same query
benchmark and original allocator. All counters run for 100% of enabled time.
Median paired user instructions fall 0.882% at batch 8,192 and 0.593% at batch
256; cycles fall 2.69% and 2.01%. These totals include input creation, validation,
warmups, planning, execution and serialization across all cases. They measure
whole-process work, not per-query latency or isolated arithmetic instructions.

## Correctness and allocation checks

The existing regression contract is unchanged: 616/616 focused observations
agree with Spark, including 131 error payloads byte-identical to the preceding
Rust runtime. All 6,784 regression observations retain their status and successful
values/types, with 6,403 agreement and no previously agreeing regression. The
116 Delta observations match the preceding runtime; 18 adapter checks pass.
The 38 planner and 28 runner tests pass, including four Delta lifecycle tests.
Strict Delta/Spark results remain at 47 matches, 58 differences and 11 pending
host cases; this change does not claim complete Spark compatibility.

The ten existing tests in Arrow's arithmetic module also pass when compiled
against the candidate source and its recorded dependencies. This is a standalone
module test run, not the complete Arrow test suite.

The unchanged physical-expression diagnostic validates values, types and the
NULL short-circuit boundary in four separate allocation captures. All 100
case/mode observations per process have identical allocation calls, requested
bytes and peak live bytes before and after. Non-overflowing execution benefits
from less compiled work, not fewer allocations. Incidental timing from these
counting passes is retained but excluded from speed claims.

## Recheck the evidence

[checked-overflow-format-results.json](checked-overflow-format-results.json)
records source and binary identities and the result summary.
[The raw archive](checked-overflow-format-runs.json.gz) retains both prototypes,
the complete runtime records, frozen protocols and every original sample. It
also retains an initial rejected Cargo configuration attempt, corrected before
the successful build. The baseline is the
[column field reuse archive](column-field-reuse-runs.json.gz).

```sh
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/checked-overflow-format-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

The checker recomputes comparisons from the frozen evidence without a runtime
build or private cache. Build records identify Sail 0.7.1 provenance, DataFusion
54.1.0, Arrow 58.4.0, Spark 4.2.0 and Rust 1.98.1. The optional runtime retains
this change for the next investigation. Required NULL selection/scatter, the
remaining argument vectors and conclusive performance acceptance remain in
issue 195.
