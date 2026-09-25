# Legacy numeric-string CAST modes

[Issue 230](https://github.com/mag1cfrog/delta-arrow-reader/issues/230) repairs
explicit numeric-string CASTs with ANSI disabled. On the selected optional
runtime, `CAST('bad' AS BIGINT)` and `CAST('bad' AS DOUBLE)` raised errors;
they now return typed NULLs. Fractional integer strings truncate toward zero
and width overflow returns NULL. ANSI and TRY_CAST retain separate paths.

The [Rust patch](sail-legacy-numeric-cast.patch) changes three files. The
existing string-to-INT parser now writes Int8, Int16, Int32 or Int64 arrays
directly. It uses the existing `num` dependency and adds no intermediate
integer array. Its existing ROUND caller uses the same shared parser.
Legacy FLOAT/DOUBLE string conversion uses DataFusion BTRIM and TRY_CAST.
This native composition adds measurable cost, recorded below.

The patch applies to the accepted optional runtime at `c5181cf2`, following
the [Decimal arithmetic repair](DECIMAL_ARITHMETIC_TYPES.md). It is not a
standalone patch for the default vendor. Python generates independent Spark
references and compares captures; query execution remains Rust.

## Coverage and remaining owners

All 63 observations assigned by the [division/CAST diagnostic](DIVISION_CAST_CLASSIFICATION.md)
now agree with Spark: 25 original observations, 13 supplements and 25
reductions. Their 63 opposite-mode controls retain 26 agreements. The 37
existing differences remain with the scalar CAST, early arithmetic and
discarded-subquery leaves: 19, 4 and 14 respectively. These are repeated
observations, not 63 independent defects or 37 newly introduced failures.

The expanded [281-query corpus](legacy-numeric-cast.jsonl) records 562
observations across both ANSI modes:

| Comparison | Before | After |
| --- | ---: | ---: |
| Values, types and classified error causes | 381/562 | 492/562 |
| Newly repaired observations | 0 | 111 |
| Previously agreeing controls retained | 381 | 381 |

The controls cover literals and columns, empty/all-NULL inputs, signed integer
boundaries, overflow, fractional text, signs, whitespace, invalid grammar,
FLOAT/DOUBLE precision, signed zero, nonfinite values, Decimal and numeric
peers, TRY_CAST and NULL guards. This finite corpus does not establish full
Spark CAST compatibility. The remaining 70 observations have independent
implementation owners:

- [Strict/TRY integer whitespace](https://github.com/mag1cfrog/delta-arrow-reader/issues/237):
  36 observations. Spark trims these inputs before strict integer parsing;
  the retained native fallback does not. The legacy parser already handles
  the tested whitespace, including DEL.
- [Floating-string grammar](https://github.com/mag1cfrog/delta-arrow-reader/issues/238):
  34 observations, including `1.25f`, `1.25D`, hexadecimal floating literals
  and strict/TRY whitespace. Spark's Java parser accepts inputs the native
  parser still rejects or maps to NULL.

The two native sibling issues are assigned to `mag1cfrog` and depend on this
slice. Their exact IDs, SQL, settings and before/after outcomes are in the
[result map](legacy-numeric-cast-results.json) and raw archive. No residual
is reclassified as accepted compatibility.

Spark's [UTF8String integer parsing](https://github.com/apache/spark/blob/v4.2.0/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java)
and [CAST dispatch](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala)
provide the independent semantics. The pinned source copies and hashes are
retained alongside the Spark 4.2.0 observations.

## Retained behavior

The 37 historical groups improve from 6,483/6,784 to 6,508/6,784 agreements.
Exactly the 25 original observations owned by this issue change outcome;
none of the earlier agreements is lost. This comparator uses historical
Decimal-text/coarse-stage rules, so the count is not interchangeable with
full value, schema or error-parameter acceptance.

| Dedicated suite | Candidate agreement |
| --- | --- |
| Decimal arithmetic | 575/578 |
| String division | 472/480 |
| BIGINT DIV | 232/234, three repaired |
| Remainder NULL guards | 427/442, two repaired |
| Decimal ROUND extreme scales | 1,194/1,322 |
| FLOAT ROUND | 398/398 targets, 16/18 controls |
| ROUND arguments | 388/388, one repaired |
| Integer ROUND | 1,254/1,254 |
| Earlier Decimal ROUND | 234/234 |
| FLOAT BROUND boundaries | 246/246 targets, 8/8 controls |
| FLOAT BROUND types | 80/80 targets, 8/8 controls, 10/10 boundaries |
| Decimal BROUND | 506/506 |

All earlier agreements in these suites remain. The previously observed
mixed-error BIGINT result is unchanged, so its apparent agreement is not a
new repair. Integer overflow remains 616/616, with all 131 complete error
payloads unchanged. The real-Delta capture matches the previous runtime in
116/116 observations and passes all 18 adapter checks. Its strict Spark
comparison remains 47 matches, 58 differences and 11 pending observations.

Of 908 retained historical errors, 12 only change recorded plan text and
two change the first invalid value reported:
`existing-cast/high_scale_batch1_true` and
`existing-string/max_integer_cast_batch4_true`. Their complete payloads stay
with [the error/schema reference leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
No error-equivalence waiver or repeat-until-matching run was added.

The expanded corpus records logical-nullability differences of 122 before
and 64 after, and physical-nullability differences of 139 before and 176
after. More queries now return successfully, changing which schemas can be
compared. Harness phase-label differences fall from 157 to 36; a
`collect()` error label alone does not identify the internal Spark phase.
These dimensions remain separate from the 492 value/type/error-cause
agreements and stay open with the same reference leaf.

## Bounded performance check

The prespecified schedule runs before, after, after, before, after, before,
before, after. Each process measures 15 forms with and without NULLs:
1,048,576 rows, batches of 8,192, one partition, CPU 2, eight execution
warmups and 41 samples per planning/execution phase. Planning includes
parsing through physical-plan creation. Execution consumes an existing plan;
input creation and independent reference validation are outside timing.
Tables report the median of the four process medians. All samples and all
four medians per variant are retained, including outliers.

For row pattern `i = row % 10000`, `a = i % 199 - 99`; `s` is its decimal
integer string, `f` appends `.25`, and `w` is the signed 19-digit integer
`(9007199254740993000 + a) * (1 if i % 2 == 0 else -1)`. `sp` surrounds `f`
with SPACE/TAB/LF. `bad` substitutes `bad` where `i % 17 == 16`. The NULL
configuration masks every column when `i % 10 == 9`. All SQL expressions,
input schemas and plans are retained in the benchmark source/captures.

Spark supplies the independent 10,000-pattern reference for all 30
configurations. Every successful timed query checks every output against it,
including exact floating bits, Decimal coefficients, integer width and NULLs.
All 24 successful before/after pairs have matching output digests and types.
The other six baseline configurations fail: fractional BIGINT strings,
invalid DOUBLE strings and padded DOUBLE strings, each with and without
NULLs. Only candidate times are reported for those cases; an error is not a
throughput baseline.

Execution time:

| Configuration | Before (ms) | After (ms) | After / before |
| --- | --- | --- | --- |
| tiny_nullsfalse | 4.855457 | 6.487144 | 1.3361x (+33.61%) |
| small_nullsfalse | 4.815117 | 6.775700 | 1.4072x (+40.72%) |
| int_nullsfalse | 6.942274 | 6.322457 | 0.9107x (-8.93%) |
| long_nullsfalse | 4.679095 | 6.497663 | 1.3887x (+38.87%) |
| wide_long_nullsfalse | 14.617424 | 20.687703 | 1.4153x (+41.53%) |
| float_nullsfalse | 8.821644 | 18.843809 | 2.1361x (+113.61%) |
| double_nullsfalse | 8.776986 | 18.884634 | 2.1516x (+115.16%) |
| ansi_long_nullsfalse | 4.656778 | 4.758508 | 1.0218x (+2.18%) |
| try_long_nullsfalse | 4.276192 | 4.277735 | 1.0004x (+0.04%) |
| decimal_nullsfalse | 73.389180 | 74.183625 | 1.0108x (+1.08%) |
| numeric_nullsfalse | 0.447852 | 0.455686 | 1.0175x (+1.75%) |
| fraction_long_nullsfalse | error | 8.493570 | n/a |
| bad_double_nullsfalse | error | 18.539288 | n/a |
| space_double_nullsfalse | error | 23.581863 | n/a |
| native_double_nullsfalse | 8.770785 | 8.798542 | 1.0032x (+0.32%) |
| tiny_nullstrue | 4.926886 | 7.924288 | 1.6084x (+60.84%) |
| small_nullstrue | 4.896830 | 8.136332 | 1.6616x (+66.16%) |
| int_nullstrue | 8.591392 | 7.734946 | 0.9003x (-9.97%) |
| long_nullstrue | 4.785312 | 7.732426 | 1.6159x (+61.59%) |
| wide_long_nullstrue | 13.409395 | 20.800743 | 1.5512x (+55.12%) |
| float_nullstrue | 8.389762 | 18.504317 | 2.2056x (+120.56%) |
| double_nullstrue | 8.376733 | 18.504303 | 2.2090x (+120.90%) |
| ansi_long_nullstrue | 4.756790 | 4.756905 | 1.0000x (+0.00%) |
| try_long_nullstrue | 4.371409 | 4.317143 | 0.9876x (-1.24%) |
| decimal_nullstrue | 67.484918 | 67.420635 | 0.9990x (-0.10%) |
| numeric_nullstrue | 0.812129 | 0.802992 | 0.9887x (-1.13%) |
| fraction_long_nullstrue | error | 8.941232 | n/a |
| bad_double_nullstrue | error | 18.603307 | n/a |
| space_double_nullstrue | error | 22.531697 | n/a |
| native_double_nullstrue | 8.391821 | 8.445996 | 1.0065x (+0.65%) |

Planning time:

| Configuration | Before (ms) | After (ms) | After / before |
| --- | --- | --- | --- |
| tiny_nullsfalse | 0.285104 | 0.300242 | 1.0531x (+5.31%) |
| small_nullsfalse | 0.283957 | 0.302252 | 1.0644x (+6.44%) |
| int_nullsfalse | 0.296365 | 0.299105 | 1.0092x (+0.92%) |
| long_nullsfalse | 0.284238 | 0.298936 | 1.0517x (+5.17%) |
| wide_long_nullsfalse | 0.282680 | 0.297147 | 1.0512x (+5.12%) |
| float_nullsfalse | 0.288521 | 0.339045 | 1.1751x (+17.51%) |
| double_nullsfalse | 0.287093 | 0.341028 | 1.1879x (+18.79%) |
| ansi_long_nullsfalse | 0.288877 | 0.285706 | 0.9890x (-1.10%) |
| try_long_nullsfalse | 0.285254 | 0.283347 | 0.9933x (-0.67%) |
| decimal_nullsfalse | 0.305507 | 0.306655 | 1.0038x (+0.38%) |
| numeric_nullsfalse | 0.341139 | 0.342081 | 1.0028x (+0.28%) |
| fraction_long_nullsfalse | error | 0.298971 | n/a |
| bad_double_nullsfalse | error | 0.345632 | n/a |
| space_double_nullsfalse | error | 0.342992 | n/a |
| native_double_nullsfalse | 0.166509 | 0.164601 | 0.9885x (-1.15%) |
| tiny_nullstrue | 0.268284 | 0.284925 | 1.0620x (+6.20%) |
| small_nullstrue | 0.269471 | 0.283602 | 1.0524x (+5.24%) |
| int_nullstrue | 0.284833 | 0.283782 | 0.9963x (-0.37%) |
| long_nullstrue | 0.286908 | 0.300102 | 1.0460x (+4.60%) |
| wide_long_nullstrue | 0.285831 | 0.300153 | 1.0501x (+5.01%) |
| float_nullstrue | 0.290235 | 0.339901 | 1.1711x (+17.11%) |
| double_nullstrue | 0.289543 | 0.340647 | 1.1765x (+17.65%) |
| ansi_long_nullstrue | 0.288140 | 0.287544 | 0.9979x (-0.21%) |
| try_long_nullstrue | 0.285345 | 0.285040 | 0.9989x (-0.11%) |
| decimal_nullstrue | 0.306389 | 0.308519 | 1.0070x (+0.70%) |
| numeric_nullstrue | 0.342751 | 0.343714 | 1.0028x (+0.28%) |
| fraction_long_nullstrue | error | 0.302547 | n/a |
| bad_double_nullstrue | error | 0.344205 | n/a |
| space_double_nullstrue | error | 0.345361 | n/a |
| native_double_nullstrue | 0.168553 | 0.172906 | 1.0258x (+2.58%) |

Canonical TINYINT/SMALLINT/BIGINT strings take 33.61%-66.16% longer
in this run, including short and 19-digit inputs. Their plans now use the
shared Spark-compatible integer parser instead of the native strict parser.
The existing INT path measures 8.93%-9.97% faster; this bounded run does not
attribute that change to a specific instruction or allocation difference.

Canonical FLOAT/DOUBLE strings take 2.1361-2.2090 times the old execution
time, adding about 10 ms per million rows. Planning rises 17.11%-18.79%.
The candidate plan adds native BTRIM before TRY_CAST even for unpadded
strings. Eliminating unnecessary scans/materialization is a concrete
follow-up question; this measurement does not isolate the cost of each
component. The correct richer floating grammar is still pending separately,
so a subsequent parser change must carry these same cost controls forward.

ANSI BIGINT, TRY_CAST BIGINT, Decimal, numeric arithmetic and native DOUBLE
controls vary -1.24% to +2.18% in execution. Their planning changes range
from -1.15% to +2.58%. The large observed costs apply to the changed legacy
string CAST paths; these controls do not show a comparable global slowdown.
They also do not prove a global absence of regression.

The complete cost record belongs to the existing [CAST performance leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/159).
Both integer parsing and floating trim/conversion remain open there. This
compatibility slice neither accepts those costs nor starts an unbounded
optimization loop. The later performance decision must retain these exact
inputs and reconcile any newer compatibility implementation.

The host is an AMD Ryzen 7 8845HS on Linux with Rust 1.98.1/LLVM 22.1.8.
The process-start guard recorded 14 waits for Cargo/rustc before process 6.
No builds or Spark captures were scheduled by this task during timing, but
the guard does not establish an isolated host or exclude interference
within a process. For example, one no-NULL TRY_CAST process median is
7.420 ms while its other three are 4.195-4.339 ms; it remains in the data.
This run has no same-binary calibration or allocation/instruction/memory
profile. Small control movements are not attributed to code or dismissed as
noise. No extra timing round was used to select a favorable result.

## Verification and reproduction

The candidate passes 313 function tests, 45 planner tests, 28 runner tests
and the existing comparator regression test. The added Rust matrix checks
all four integer widths over Utf8, LargeUtf8 and Utf8View, boundaries,
fractional strings, malformed inputs and NULLs. All 414 selected libraries
were hash-verified; only `sail_function` and `sail_plan` differ from baseline.
Shared sources, temporary inputs and executable slots were restored.

Candidate SHA-256:

- Probe: `5b377a0803ba365f5610cfa31cee3b0123e21c7768c675fca4154e5982fc30e4`.
- Runner: `f1b1d49dac5af3624cf53e52b37ee4ba38af05dc854475b2b4bf45cbdef6894d`.

The [archive](legacy-numeric-cast-runs.json.gz) contains source snapshots,
build/test logs, the initial rejected compile attempt, reference captures,
all focused and historical outcomes, the fixed timing protocol and raw
timings. Its baseline archives are pinned by hash. The initial 28-configuration
benchmark preparation was expanded to 30 before any timing; both source and
reference revisions are retained. No measurements from that preparation are
substituted into the final schedule.

From the repository root, verify the evidence without Rust or Spark:

```sh
python - <<'PY'
import gzip, hashlib, json
from pathlib import Path
root = Path('experiments/spark-sql')
record = json.loads((root / 'legacy-numeric-cast-results.json').read_text())
packed = (root / record['archive']['path']).read_bytes()
assert hashlib.sha256(packed).hexdigest() == record['archive']['sha256']
archive = json.loads(gzip.decompress(packed))
exec(compile(archive['files']['check-archive.py'], 'check-archive.py', 'exec'))
PY
python -m unittest discover -s experiments/spark-sql -p test_division_cast_classification.py
```

To repeat the focused capture, use the pinned Spark environment with
`legacy_numeric_cast.py spark OUT`, the selected Rust probe with
`legacy-numeric-cast.jsonl OUT --physical-plans`, and
`legacy_numeric_cast.py compare SPARK NATIVE REPORT`. Archived `prepare.py`,
`build.py`, `link-bench.py`, `bench-reference.py` and `measure.py` record the
source reconstruction, override paths, exact build commands and timing
schedule. Their cache paths describe the measured environment, not a
portable default build. Default-build adoption remains with its existing
integration owner; this evidence does not publish a Python package or finish
Spark compatibility/performance acceptance.
