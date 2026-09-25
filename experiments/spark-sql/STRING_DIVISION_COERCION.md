# String-peer division coercion

[Issue 145](https://github.com/mag1cfrog/delta-arrow-reader/issues/145) owns
implicit string conversion for `/` and `DIV` with numeric, string and NULL
peers. The optional candidate layers on integration `de78c19`, after the
reviewed BIGINT DIV overflow patch. Explicit CAST and other arithmetic
operators keep their existing behavior.

## Conversion rules

Spark 4.2.0 does not apply one string-to-DOUBLE rule in both modes.

| Operands | ANSI | Non-ANSI |
| --- | --- | --- |
| String and integer, `/` | Convert through BIGINT, then divide as DOUBLE | Convert to DOUBLE; invalid strings become NULL |
| String and integer, `DIV` | Convert to BIGINT, then use integral division | Reject incompatible operand types |
| String and Decimal/FLOAT/DOUBLE, `/` | Convert to DOUBLE | Convert to DOUBLE; invalid strings become NULL |
| String and Decimal/FLOAT/DOUBLE, `DIV` | Reject the resulting floating operands | Reject the resulting floating operands |
| Two strings, `/` | Reject string operands | Convert both to DOUBLE |
| Two strings, `DIV` | Reject string operands | Reject the resulting DOUBLE operands |

For example, `'7' DIV 2` returns BIGINT 3 in ANSI mode and fails type
checking in non-ANSI mode. `'7.5' / 2` raises CAST_INVALID_INPUT in ANSI mode
but returns DOUBLE 3.75 in non-ANSI mode. The original Decimal seed,
`CAST(2 AS DECIMAL(10,2)) / '3'`, returns DOUBLE in both modes.

The [patch](sail-string-division-coercion.patch) adds one shared conversion
function at the existing division builders. It uses native BTRIM and
CAST/TRY_CAST, then reuses the accepted division expressions and kernels.
Integer conversion trims ASCII control/space bytes and DEL; ordinary DOUBLE
conversion trims bytes through SPACE. Query ANSI mode determines the casts
and type rejection without changing session settings. There is no new
dependency, Python execution or physical node.

The implementation follows Spark's
[ANSI string promotion](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiStringPromotionTypeCoercion.scala),
[legacy string promotion](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/StringPromotionTypeCoercion.scala)
and operator-specific coercion. Pinned source files and their hashes are
archived alongside independent Spark captures. Only selected `math.rs` and
library `sail_plan` change among 414 recorded libraries. Normal-build adoption
remains with its [existing owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

## Correctness and retained differences

The [generator](string_division_coercion.py) produces
[240 queries](string-division-coercion.jsonl), each run in both ANSI modes.
It covers both operand positions, literals and columns, integer widths,
Decimal and floating peers, whitespace, fractional/exponent strings,
malformed and out-of-range values, NaN/infinity, NULL masks, zero, empty
inputs, aliases and conditional branches. These are bounded checks, not a
claim that the native casts implement every JVM numeric-string format.

| Check | Before | After |
| --- | ---: | ---: |
| Owned conversion targets | 111/444 | 444/444 |
| General CAST evaluation references | 1/20 | 12/20 |
| Explicit CAST and numeric controls | 16/16 | 16/16 |
| Complete new matrix | 128/480 | 472/480 |
| Historical numeric replay | 6,445/6,784 | 6,449/6,784 |

The comparator checks represented values/types or specific error causes.
It distinguishes incompatible types from unsupported operator types and
keeps error phase and nullability separate. Its harness test rejects unknown
errors, different conditions and successful NULLs as substitutes for an error.

Eight ANSI observations retain a general CAST evaluation difference under
the [existing diagnostic](https://github.com/mag1cfrog/delta-arrow-reader/issues/148):
an invalid operand paired with NULL, or a CAST inside an unused CASE branch,
can be evaluated when Spark returns NULL or the other branch. Exact IDs remain
in `ownership.json`. Eight explicit-CAST reductions, run in both modes, have
identical complete before/after captures. They reproduce the existing
evaluation boundary without implicit string coercion. Four column-input
controls with a literal NULL already agree; retaining NULL in a column
reproduces the error. Both shapes remain in the archive.

Two dead-branch queries were originally tagged as targets. Their four
observations are assigned to that evaluation group after the reductions;
`category-transfers.json` records the change. The original SQL, categories,
captures and all 480 observations remain intact. No residual is accepted or
removed to produce the 444-target result.

The new native-table Rust test covers Utf8, LargeUtf8 and Utf8View, an empty
batch, NULL masking, aliases, malformed input and both query/host ANSI
settings. Suites pass 312 function, 43 planner and 28 runner tests, plus the
new harness test. Integer arithmetic retains 616 agreements and 131 complete
error payloads. The accepted modulo/ROUND/BROUND comparisons are unchanged,
including their existing residuals. Delta retains all 116 outcomes and passes
18 adapter checks; strict Spark comparison stays 47 matches, 58 differences
and 11 pending host cases.

The historical replay gains four agreements without losing one: both modes
of the Decimal/string seed, and two repeated observations of the ANSI
string/integer DIV seed. Of 933 retained failures, ten error texts now name
the specific string-DIV type rejection. Two other CAST errors report a
different first invalid input, a variation already tracked by the evaluation
owner. The raw payload audit preserves every change.

The separate accepted BIGINT matrix records 228/234 here, versus 229/234 in
its archived baseline. Its mixed-error ANSI query contains both MIN / -1
and a zero divisor: the original candidate run reports divide-by-zero instead
of overflow. All eight fixed diagnostic runs, including four on that same
candidate binary, report overflow. Its numeric source and logical/physical
plans are unchanged. This demonstrates variation within the candidate;
it does not make the two causes equivalent or establish that the old binary
also varies. The original mismatching run remains visible under the
evaluation owner. Complete ANSI payload preservation is not claimed.

Schema flags and error-phase review remain with the
[arithmetic reference owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
Frozen inputs, queries, references and probe source are unchanged.

## Bounded cost check

Thirteen forms use native string/numeric columns, with and without independent
NULL masks. Spark supplies 10,000 reference rows per configuration. Every
candidate output and the 24 comparable before outputs are checked outside
timing. New string DIV has no successful old execution baseline; its candidate
time is reported separately. The explicit-string-CAST DIV control remains
comparable.

The fixed schedule uses 1,048,576 rows, batches of 8,192, one partition and
CPU 2, eight warmups and 41 samples. Eight processes run in
before/after/after/before/after/before/before/after order. Planning and
execution are separate. The competing-job guard records its wait for another
Cargo test. No measurement is discarded or repeated. There is no same-binary
calibration or allocator-policy change, and the host is not fully isolated.

| Form | Before / after execution, ms, no NULLs | Before / after execution, ms, NULL masks |
| --- | ---: | ---: |
| ANSI string dividend | 8.668 / 18.922 | 8.963 / 19.589 |
| ANSI string divisor | 12.092 / 20.580 | 12.904 / 21.112 |
| Legacy string dividend | 8.748 / 18.803 | 9.013 / 19.148 |
| Legacy string divisor | 6.226 / 20.683 | 7.140 / 21.070 |
| ANSI DOUBLE peer | 8.439 / 19.501 | 8.374 / 19.677 |
| Legacy DOUBLE peer | 8.442 / 18.413 | 8.345 / 18.610 |
| ANSI constant divisor | 7.840 / 17.851 | 7.695 / 18.351 |
| ANSI scalar string | 1.253 / 1.155 | 1.564 / 1.457 |
| Numeric `/` control | 1.758 / 1.743 | 2.235 / 2.266 |
| Numeric DIV control | 1.599 / 1.600 | 1.472 / 1.481 |
| Explicit string CAST/DIV | 7.582 / 7.563 | 7.284 / 7.293 |
| Decimal DIV control | 16.418 / 16.441 | 16.014 / 16.018 |
| New string DIV | planning error / 18.765 | planning error / 18.910 |

String-column execution increases 63.6%-232.2%, adding roughly 8.2-14.5 ms
per million rows. The added trim and conversion expressions are confined to
string operands; numeric operator kernels are unchanged. ANSI column planning
falls 16.1%-18.9%; legacy column planning rises 10.6%-16.0%. The constant-divisor
form adds 4.9%-6.0% planning time. The scalar-string form folds the conversion
and improves 6.8%-7.9% in execution.

Numeric, explicit-CAST and Decimal controls range from -0.9% to +1.4% in
execution and -0.8% to +0.7% in planning. New string DIV takes 18.765/18.910 ms,
with 0.398/0.404 ms planning time. These are local measurements, not zero-cost
claims or a calibrated attribution of every small control movement. A separate
trim pass and the ANSI BIGINT-to-DOUBLE conversion are concrete candidates
for the performance review; no faster conversion kernel is adopted here.


The [existing performance leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/158)
owns these costs and final acceptance. They are not added to percentages from
older builds. Extended optimization follows the agreed compatibility-first
working order.

## Recheck the evidence

The [result record](string-division-coercion-results.json) and
[archive](string-division-coercion-runs.json.gz) retain exact sources,
build/library hashes, references, full captures, tests, all residual IDs and
every timing sample. Nine committed baseline archives provide earlier
sources/references. Build and Delta scripts restore shared files and executable
slots.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/string_division_coercion.py spark /tmp/string-spark.json
"$STRING_DIVISION_PROBE" experiments/spark-sql/string-division-coercion.jsonl /tmp/string-native.json --physical-plans
python3 experiments/spark-sql/string_division_coercion.py compare \
  /tmp/string-spark.json /tmp/string-native.json /tmp/string-check.json
```

The comparator returns nonzero for the eight retained evaluation differences.
To verify the archived evidence without Spark or a Rust build, run from the
repository root:

```bash
python3 - <<'PYCODE'
import gzip, json
from pathlib import Path
root = Path("experiments/spark-sql")
archive = json.loads(gzip.decompress((root / "string-division-coercion-runs.json.gz").read_bytes()))
exec(compile(archive["files"]["check-archive.py"], "check-archive.py", "exec"))
PYCODE
```
