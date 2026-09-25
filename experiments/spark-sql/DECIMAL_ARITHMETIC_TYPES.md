# Decimal arithmetic result types

[Issue 143](https://github.com/mag1cfrog/delta-arrow-reader/issues/143) owns
Decimal remainder precision and precision-38 adjustment for `+`, `-`, `*`
and `%`. This optional candidate layers on integration `689100f`, after
string-peer division coercion. It reuses Arrow arithmetic and casts. Runtime
execution stays in Rust.

## Result rules and implementation

For operand precision/scale `(p1,s1)` and `(p2,s2)`, Spark derives these
unbounded results before adjusting them to precision 38:

| Operator | Precision | Scale |
| --- | --- | --- |
| `+`, `-` | `max(p1-s1,p2-s2) + max(s1,s2) + 1` | `max(s1,s2)` |
| `*` | `p1+p2+1` | `s1+s2` |
| `%`, `mod` | `min(p1-s1,p2-s2) + max(s1,s2)` | `max(s1,s2)` |

The retained configuration uses `allowPrecisionLoss=true`. When precision
exceeds 38, the existing Spark adjustment helper reduces fractional scale,
preserving at least six fractional digits where available. The exact result
is then rounded HALF_UP; an unrepresentable result raises an error in ANSI
mode and becomes NULL otherwise. These rules follow
[Spark 4.2 arithmetic](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala).

The [Sail planner patch](sail-decimal-arithmetic-types.patch) uses native
Decimal128 arithmetic when declared bounds fit. Wider intermediate values
use Decimal256 arithmetic followed by native CAST or TRY_CAST to the Spark
result type. The existing Arrow Decimal cast performs the rounding. Legacy
remainder's NULLIF guard now uses a zero with the divisor's Decimal type,
so guarding a Decimal(1,0) divisor does not expand it to Decimal(10,0).

Two examples:

- `d % 1`, with `d DECIMAL(8,2)`, returns DECIMAL(3,2) in both modes.
- `d + 1`, with `d DECIMAL(38,18)` equal to `0.123456789012345675`, returns
  DECIMAL(38,17) with value `1.12345678901234568`.

The first benchmark candidate exposed an additional optimizer defect. For a
non-nullable Decimal column, DataFusion's `% 1` rule replaced the expression
with zero of the dividend type. A value of `0.1255` therefore lost its
fraction. The [DataFusion patch](datafusion-decimal-modulo-one.patch) restricts
that identity to integer operands. Decimal and floating inputs keep the
native remainder expression. The original
[DataFusion 54.1 rule](https://github.com/apache/datafusion/blob/54.1.0/datafusion/optimizer/src/simplify_expressions/expr_simplifier.rs)
and the failed benchmark assertion identify the cause; a Rust regression
checks Decimal32/64/128/256, floats and the retained integer identity.

## Correctness and remaining observations

The [generator](decimal_arithmetic_types.py) produces
[289 queries](decimal-arithmetic-types.jsonl) in both ANSI modes. Coverage
includes fractional and integer peers, both operand orders, literals and
columns, signs, precision 37/38, scale sums through 76, rounding ties,
overflow, NULLs, empty inputs, zero and the `mod` alias. Batch sizes 1, 2 and
64 exercise live and masked zeros. Benchmark tables additionally verify
actual non-nullable Arrow fields against independent Spark results.

| Scope | Before | After |
| --- | --- | --- |
| Arithmetic targets | 271/557 | 557/557 |
| Existing-function controls | 18/18 | 18/18 |
| VALUES input-type references | 0/3 | 0/3 |
| Complete corpus | 289/578 | 575/578 |

All 22 named remainder observations and 12 precision-38 observations from
the original arithmetic report now agree. `original-targets.json` preserves
their exact IDs. Frozen corpora and references remain unchanged.

Three new non-ANSI observations retain a VALUES type-coercion difference,
owned by the [relational coverage leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/141).
Combining a Decimal(38,38) value with an INT zero in VALUES yields
Decimal(38,28) in Spark, while the selected build retains Decimal(38,38).
The resulting NULL remainder values agree but their schemas differ.

Removing remainder reproduces the cause in both modes. The bare fractional
VALUES query also preserves more fractional digits than Spark because its
input type was not reduced. Its complete before/after captures are identical.
Explicitly typing both VALUES rows alike makes all four remainder
observations agree, including live-zero errors. `values-cases.jsonl`, all
captures and `observation-transfers.json` preserve this distinction. The
original 578 observations stay visible; the three differences are not waived.

The 37 historical groups retain all previously accepted observations:
6,449/6,784 agreements become 6,483/6,784. All 34 changed outcomes are the
original arithmetic targets. This historical comparator has coarser
Decimal-text/error-stage rules than the focused checks; its count is not
interchangeable with represented-value or error-cause acceptance.

The dedicated prior suites retain their accepted results:

| Suite | Candidate agreement |
| --- | --- |
| String division | 472/480 |
| BIGINT DIV | 229/234 |
| Remainder NULL guards | 425/442 |
| Decimal ROUND extreme scales | 1,194/1,322 |
| FLOAT ROUND | 398/398 targets, 16/18 controls |
| ROUND arguments | 387/388 |
| Integer ROUND | 1,254/1,254 |
| Earlier Decimal ROUND | 234/234 |
| FLOAT BROUND boundaries | 246/246 targets, 8/8 controls |
| FLOAT BROUND types | 80/80 targets, 8/8 controls, 10/10 boundaries |
| Decimal BROUND | 506/506 |

The BIGINT count was 228/234 in the previous capture. Its one changed case,
`scalar_array_-9223372036854775808_true`, now reports overflow instead of
division by zero. Both causes exist in that query; prior fixed diagnostics
already recorded first-error variation. This is not claimed as a repair.

Of 933 retained historical failures, three report different first-invalid
input text: `existing-cast/high_scale_batch1_true`,
`existing-string/max_integer_cast_batch4_true` and
`existing-unicode/round_overflow_cast_batch1_true`. Their exact payloads and
the BIGINT observation remain with the
[evaluation-order leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/148).
The archive preserves these changes without equating their errors.

Integer overflow remains 616/616 with all 131 raw error payloads unchanged.
The Delta capture matches its previous runtime in all 116 observations and
passes all 18 adapter checks. Its strict Spark result stays at 47 matches,
58 differences and 11 pending observations.

Schema flags and error phase remain separately owned by the
[arithmetic reference leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
In the focused corpus, logical-nullability differences stay at 2;
physical-nullability differences change from 162 to 174. Error-phase
differences fall from 76 to 0. Exact IDs and both schemas remain in the raw
checks; arithmetic value/type agreement does not close schema acceptance.

## Bounded cost check

Thirteen query forms use 1,048,576 rows, batches of 8,192 and one partition,
with and without independent NULL masks. Each final process checks every
output against 10,000 repeating Spark reference patterns outside timing.
The schedule is before/after/after/before/after/before/before/after, with eight
warmups and 41 samples per process on CPU 2. Results below are medians of four
process medians, in milliseconds.

The original candidate failed the non-nullable remainder assertion after
one complete before process and an incomplete after process. Those artifacts,
its exact build and rejection reason remain under `initial-candidate/`.
The corrected runtime has a new fixed eight-process schedule. No old sample
is included in the corrected schedule or discarded from the archive.

Execution, before / after:

| Form | No NULLs (ms) | NULL masks (ms) |
| --- | --- | --- |
| Small Decimal addition | 4.851 / 4.717 | 4.846 / 4.700 |
| Small Decimal multiplication | 2.625 / 2.616 | 2.931 / 2.903 |
| ANSI Decimal remainder | 0.186 / 9.006 | 8.601 / 8.507 |
| Legacy Decimal remainder | 0.185 / 8.977 | 8.622 / 8.514 |
| Precision-38 addition | 4.866 / 62.666 | 4.829 / 57.267 |
| Precision-38 multiplication | 2.636 / 46.042 | 2.938 / 43.299 |
| Legacy precision-38 multiplication | 2.632 / 44.522 | 2.948 / 42.162 |
| Scale-38 multiplication | error / 87.024 | error / 84.020 |
| Wide-scale remainder | error / 78.301 | error / 66.748 |
| Numeric division control | 1.776 / 1.778 | 2.309 / 2.303 |
| Decimal division control | 13.968 / 13.681 | 11.589 / 11.636 |
| Explicit native wide multiplication | 46.394 / 46.702 | 44.240 / 43.513 |
| Explicit native wide remainder | 85.805 / 85.420 | 73.301 / 72.205 |

Planning, before / after:

| Form | No NULLs (ms) | NULL masks (ms) |
| --- | --- | --- |
| Small Decimal addition | 0.333 / 0.333 | 0.335 / 0.338 |
| Small Decimal multiplication | 0.330 / 0.329 | 0.333 / 0.336 |
| ANSI Decimal remainder | 0.309 / 0.332 | 0.334 / 0.335 |
| Legacy Decimal remainder | 0.338 / 0.358 | 0.361 / 0.361 |
| Precision-38 addition | 0.332 / 0.362 | 0.335 / 0.367 |
| Precision-38 multiplication | 0.329 / 0.362 | 0.334 / 0.366 |
| Legacy precision-38 multiplication | 0.328 / 0.361 | 0.334 / 0.363 |
| Scale-38 multiplication | error / 0.389 | error / 0.392 |
| Wide-scale remainder | error / 0.354 | error / 0.358 |
| Numeric division control | 0.339 / 0.342 | 0.349 / 0.348 |
| Decimal division control | 0.323 / 0.324 | 0.331 / 0.332 |
| Explicit native wide multiplication | 0.235 / 0.238 | 0.238 / 0.238 |
| Explicit native wide remainder | 0.231 / 0.229 | 0.235 / 0.236 |

Old wide-scale multiplication fails planning; old wide remainder overflows
its intermediate arithmetic. Those rows have candidate times only. Several
other old expressions return incorrect types or values, including the
non-nullable `% 1` shortcut. Their before/after costs describe different
work and cannot establish the overhead relative to a correct implementation.

Two explicit native DataFusion queries widen multiplication and remainder,
then cast to the required Spark result types. Their complete outputs match
the Spark expressions on these inputs. They provide a correct execution
comparison; their planning times use the native SQL frontend and do not
isolate Spark frontend overhead. SQL, plans, all output checks and samples
are archived.

The precision-38 addition/multiplication paths now take
11.9-17.5 times the old execution time. The old scale and
coefficients are wrong, so this is a substantial cost of the current correct
path, not a like-for-like regression estimate. Planning rises
8.9%-10.0%.
Native widening, intermediate buffers and the final rounding cast are
concrete places to investigate; their individual costs are not isolated here.

Candidate wide multiplication is within 1.5% of the correct explicit native
control in both NULL configurations. Candidate wide remainder is 7.6%-8.3%
faster than its explicit native control on these inputs. These comparisons
do not prove that either native wide path is optimal.

Non-nullable remainder now takes about 9 ms instead of 0.19 ms because the
old optimizer skipped all remainder arithmetic and incorrectly returned
zero. Nullable remainder stays around 8.5 ms; legacy mode additionally gets
the correct precision. Preserve the old costs as rejected semantics, not as
a target that requires restoring the wrong shortcut.

Small Decimal and prior division controls vary
-3.0% to +0.4% in execution and
-0.1% to +1.0% in planning. The
guard waited eleven times for Cargo/rustc before the first final process.
No local build, Spark capture or regression replay was scheduled during the
final timings. The host was not fully isolated, and small movements are not
calibrated proof of either overhead or noise.

[Arithmetic/final Decimal evaluation](https://github.com/mag1cfrog/delta-arrow-reader/issues/163)
owns the added wide arithmetic paths and their conversion costs.
[Remainder evaluation](https://github.com/mag1cfrog/delta-arrow-reader/issues/158)
owns the remainder costs. This checkpoint does not close either performance
leaf. There is no same-binary calibration or allocator change.

## Reproduction and review

Validation passes: 312 function tests, 45 planner tests, 28 runner
tests and the comparator's one regression test. The two new planner checks
cover exact Decimal results/overflow and the non-nullable modulo-one
optimizer rule. All 414 selected libraries are accounted for; eight hashes
change through the planner/optimizer dependency chains. Runtime source
changes are limited to `math.rs` and `expr_simplifier.rs`.

Tested candidate SHA-256:

- Probe: `357d3923f6138055d487855bf54ae33e4f66cc68510e627f00565c4af268c4b8`.
- Runner: `e8b6bcdb911d096afafdc83caa164192e07e183ee860b18d60eab21927adb130`.

Shared experimental source files, input files and executable slots were
restored after validation. Frozen repository inputs remain unchanged.

The [result record](decimal-arithmetic-types-results.json) pins both runtime
binaries, source/library hashes, reference sources and archive hashes.
[The archive](decimal-arithmetic-types-runs.json.gz) contains complete changed
sources, build commands, raw captures, comparisons, benchmark code/protocol,
failed initial evidence and the verifier. Spark is 4.2.0, retained Sail is
0.7.1, DataFusion is 54.1.0 and Arrow is 58.4.0.

From the repository root, verify the checked-in evidence without Rust or Spark:

```sh
python - <<'PY'
import gzip, hashlib, json
from pathlib import Path
root = Path('experiments/spark-sql')
record = json.loads((root / 'decimal-arithmetic-types-results.json').read_text())
packed = (root / record['archive']['path']).read_bytes()
assert hashlib.sha256(packed).hexdigest() == record['archive']['sha256']
archive = json.loads(gzip.decompress(packed))
exec(compile(archive['files']['check-archive.py'], 'check-archive.py', 'exec'))
PY
python -m unittest discover -s experiments/spark-sql -p test_decimal_arithmetic_types.py
```

The archived `build.py` resolves the ten accepted baseline archives, installs
the two changed source files, builds/tests the optional runtime and restores
every shared source and executable slot. The Sail patch applies from the
repository root; the DataFusion patch applies from the selected
`datafusion-optimizer` crate root. Reproduction still uses the declared
experimental build paths. Clean-checkout and CI adoption remain with
[the build leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

This evidence describes the optional runtime. The default vendor remains a
different build. Issue 143 records review, integration merge and manual
closure; the separate owners retain the unresolved observations and costs.
