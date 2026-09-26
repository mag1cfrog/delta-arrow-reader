# Decimal CAST error preservation

[Issue 246](https://github.com/mag1cfrog/delta-arrow-reader/issues/246) repairs
strict STRING-to-DECIMAL errors lost during optimization. For example,
SELECT CAST('bad' AS DECIMAL(10,2)) AS r WHERE false must raise Spark's
CAST_INVALID_INPUT in ANSI mode. Dead conditional branches and scalar NULL
propagation must still skip an unreachable cast.

The optional [Rust patch](sail-decimal-cast-errors.patch) layers on the
accepted [scalar CAST reachability fix](DEAD_CAST.md), merged through PR 250
at integration 5764ade264a98ada8c901f401c932dbdea3ca1c5. It changes three
selected Sail files: string CAST, arithmetic planning and the existing
analyzer. The default vendored checkpoint remains unchanged. Planning and
execution are Rust; Python records the independent Spark reference.

## Shared implementation

The Decimal conversion function gains the same literal-simplification
entry point as floating CAST. It calls the existing converter, preserving
parsing, HALF_UP rounding, precision/scale, safe-mode NULLs and error text.
The analyzer recognizes Decimal casts alongside existing integer/floating
casts. No parser or arithmetic kernel is replaced.

The existing CASE guard now recognizes column Decimal casts and fused
Decimal division. Division/remainder check a nullable divisor first;
ordinary arithmetic checks the left operand before a fallible right-hand
column CAST. Literal CAST failures are not moved into runtime-only guards.
The fused division simplifier folds scalar typed NULLs before failing
children, while scalar subqueries retain their analyzer checks and the
existing untyped-NULL decorrelation sentinel.

Decimal DIV reuses native live-row zero checks, as integer division and
remainder already do. Removing its separate divisor CASE preserves typed
NULL results and makes NULL propagation visible before CAST folding.
Expression rewrites use DataFusion's existing NamePreserver so aggregate
and window output names remain valid for their parent projections.

## Validation and remaining owners

All six assigned ANSI observations and their six opposite-mode controls
pass, improving from 6/12 to 12/12. The [227-query corpus](decimal-cast-errors.jsonl)
improves from 413/454 to 445/454 across both ANSI modes, without losing an
agreement. Those are observations, including repeated query shapes, rather
than counts of independent defects. The matrix covers precision/scale,
valid/malformed/overflow/NULL strings, CAST/TRY_CAST, literals/columns,
conditional branches, NULL arithmetic, zeros, VALUES/range, empty/unused
plans and batches of 1, 2 and 64.

Nine preexisting differences remain visible with existing owners:

- Seven error-identity observations belong to
  [issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
  Five Decimal overflow/range cases report CAST_INVALID_INPUT instead of
  Spark's two numeric-range conditions. Two modulo-zero cases report
  DIVIDE_BY_ZERO instead of REMAINDER_BY_ZERO. Both engines reject them.
- Two observations use a CASE mixing DECIMAL(10,2) and INT before division.
  Native type coercion widens the CASE to DECIMAL(12,2), after the fused
  function has captured an exact DECIMAL(10,2) signature, causing a planning
  rejection. They are handed to the existing conditional-type review in
  [issue 150](https://github.com/mag1cfrog/delta-arrow-reader/issues/150).
  A separate three-query control with both CASE branches explicitly typed
  DECIMAL(10,2) passes 6/6 before and after. The original failures are retained.

The prior dead-CAST corpus improves from 310/346 to 316/346. Strict integer,
floating grammar and explicit numeric CAST retain 496/496, 488/488 and
562/562. The earlier selected CAST/control set retains 122/126; the reduced
diagnostic stays 107/128. These are bounded suites, not complete Spark support.

All 37 historical groups retain 6,542/6,784 agreements, with no changed
value/type/status outcomes. All 12 dedicated arithmetic/coercion/rounding
suites retain their case-level agreements. Integer-overflow checks retain
616/616 and all 131 complete errors. Rust tests pass: 314 function, 48 planner
and 28 runner tests. The existing planner test now covers 28 SQL shapes with
ANSI on/off/on in one session, including aggregate/window name preservation.
The unchanged comparator self-check passes.

Real-Delta validation retains all 116 previous outcomes and passes 18 adapter
checks. Its frozen Spark split remains 47 matches, 58 differences and 11
pending adapters. Default-build adoption remains with
[issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

The new corpus retains two logical-nullability and 52 physical-nullability
differences before and after. Harness phase-label differences decrease from
29 to one; these labels do not identify Spark's internal optimizer phase.
There are 34 changed retained error payloads, including 28 changed texts.
Historical replay retains 890 error outcomes; 70 full payloads and 48 error
texts change. Most text changes add the optimizer's failure context; the
archive also retains a changed first invalid row in a multi-error Decimal
query and the Decimal DIV zero diagnostic. Full payloads and plans remain
available to issue 149; they are not normalized into exact error compatibility.

## Bounded cost check

Both binaries use the same benchmark source and private hash-checked link
sets. Each has 505 artifacts and 414 named libraries, with identical package
versions/features; only the sail_function and sail_plan library hashes differ.
The build manifest pins all 95 selected source entries and both binaries.

The fixed process order is before, after, after, before, after, before,
before, after. Each process uses CPU 2, 262,144 rows, batches of 8,192, one
partition, four warmups and 21 samples per phase. Reported times are the
median of four process medians. This smaller matrix is a bounded check for
this slice and must not be compared directly with the preceding one-million-row
experiment. All samples, process medians, physical plans, commands, source
hashes and library hashes are retained. The machine was not reserved; no
same-binary calibration, allocation counts, hardware counters or memory
measurement was collected. No samples or processes were discarded.

Fourteen queries run with nonnullable and 10%-NULL divisors. Numerator
strings are valid and nonnullable except guarded_invalid, which places bad
text only on NULL-divisor rows. Cases that ignore the divisor keep identical
input values across these variants. Every successful process verifies every
row against Spark using exact Decimal coefficients/precision/scale, Int64
values or Float64 bits, plus NULLs and full-output digests. There are 25
equal-output timing pairs and three baseline-error/candidate-only cases.

The three newly guarded nullable Decimal operations increase planning time:
division 0.330730 -> 0.444391 ms (+34.37%), remainder 0.330128 -> 0.437347 ms
(+32.48%), and addition 0.328831 -> 0.438289 ms (+33.29%). Their execution
changes are -0.30%, +0.05% and -0.04%. Across equal-output pairs, execution
changes range from -3.12% to +0.70%; that does not establish performance
equivalence. Decimal DIV planning improves in this run after removing its
old divisor CASE. Earlier integer/floating CAST guard costs are not resolved
by these observations; their paths remain measured controls here.

| Query / divisor NULLs | Planning before / after ms | Change | Execution before / after ms | Change |
| --- | ---: | ---: | ---: | ---: |
| decimal_cast_nullsfalse | 0.279714 / 0.251947 | -9.93% | 19.341194 / 19.390821 | +0.26% |
| decimal_try_nullsfalse | 0.292629 / 0.265107 | -9.41% | 19.376660 / 19.346314 | -0.16% |
| decimal_legacy_nullsfalse | 0.293786 / 0.265628 | -9.58% | 19.351539 / 19.385006 | +0.17% |
| decimal_divide_nullsfalse | 0.339271 / 0.339515 | +0.07% | 23.045970 / 22.987507 | -0.25% |
| decimal_integer_div_nullsfalse | 0.540304 / 0.358306 | -33.68% | 24.408777 / 24.269323 | -0.57% |
| decimal_modulo_nullsfalse | 0.347946 / 0.347155 | -0.23% | 22.780768 / 22.765154 | -0.07% |
| decimal_add_nullable_nullsfalse | 0.348542 / 0.346849 | -0.49% | 21.590398 / 21.741538 | +0.70% |
| decimal_column_case_nullsfalse | 0.516039 / 0.511931 | -0.80% | 19.512287 / 19.571081 | +0.30% |
| decimal_guarded_invalid_nullsfalse | 0.340322 / 0.340873 | +0.16% | 21.991186 / 21.944835 | -0.21% |
| decimal_null_literal_nullsfalse | error / 0.332773 | candidate only | error / 0.057367 | candidate only |
| integer_cast_div_nullsfalse | 0.364222 / 0.361056 | -0.87% | 4.631142 / 4.595921 | -0.76% |
| float_cast_div_nullsfalse | 0.326191 / 0.323971 | -0.68% | 4.522054 / 4.522921 | +0.02% |
| numeric_nullsfalse | 0.321041 / 0.316488 | -1.42% | 0.163103 / 0.160428 | -1.64% |
| native_numeric_nullsfalse | 0.180240 / 0.177555 | -1.49% | 0.070356 / 0.069880 | -0.68% |
| decimal_cast_nullstrue | 0.278016 / 0.247696 | -10.91% | 19.329032 / 19.347040 | +0.09% |
| decimal_try_nullstrue | 0.275312 / 0.248862 | -9.61% | 19.388543 / 19.376971 | -0.06% |
| decimal_legacy_nullstrue | 0.274234 / 0.244264 | -10.93% | 19.351012 / 19.343229 | -0.04% |
| decimal_divide_nullstrue | 0.330730 / 0.444391 | +34.37% | 22.961053 / 22.891479 | -0.30% |
| decimal_integer_div_nullstrue | 0.531187 / 0.450752 | -15.14% | 24.536760 / 24.468724 | -0.28% |
| decimal_modulo_nullstrue | 0.330128 / 0.437347 | +32.48% | 22.520189 / 22.530989 | +0.05% |
| decimal_add_nullable_nullstrue | 0.328831 / 0.438289 | +33.29% | 21.631077 / 21.622336 | -0.04% |
| decimal_column_case_nullstrue | 0.494564 / 0.488742 | -1.18% | 19.440028 / 19.513284 | +0.38% |
| decimal_guarded_invalid_nullstrue | error / 0.452079 | candidate only | error / 21.839545 | candidate only |
| decimal_null_literal_nullstrue | error / 0.321487 | candidate only | error / 0.057432 | candidate only |
| integer_cast_div_nullstrue | 0.390095 / 0.387730 | -0.61% | 6.011601 / 6.000971 | -0.18% |
| float_cast_div_nullstrue | 0.444400 / 0.441029 | -0.76% | 6.032725 / 6.026504 | -0.10% |
| numeric_nullstrue | 0.321958 / 0.315912 | -1.88% | 0.163624 / 0.158525 | -3.12% |
| native_numeric_nullstrue | 0.182429 / 0.177324 | -2.80% | 0.070666 / 0.069574 | -1.55% |

[Issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) retains
the new planning cost together with earlier CAST costs. Correctness acceptance
and final performance acceptance remain separate.

## Reproduction

[Results](decimal-cast-errors-results.json) pin the source artifacts and
[compressed evidence](decimal-cast-errors-runs.json.gz). The archive includes
raw captures, exact error changes, all timing samples, baseline/final sources,
tests and rejected prototypes. Prior accepted archives provide historical
references and predecessor captures. Every archive and member is hashed.

From the repository root:

```sh
python - <<'PY'
import gzip, json
from pathlib import Path
root = Path('experiments/spark-sql')
a = json.loads(gzip.decompress((root/'decimal-cast-errors-runs.json.gz').read_bytes()))
Path('/tmp/check-decimal-cast-errors.py').write_text(a['files']['check-archive.py'])
PY
python /tmp/check-decimal-cast-errors.py
python -m unittest discover -s experiments/spark-sql -p test_division_cast_classification.py
```

The offline verifier recomputes comparisons and timings without Spark, Rust
compilation or network access. The generator reuses the existing CAST
comparator and the frozen six-observation ownership map. The archive's
prepare.py, source-paths.json, override.toml and build.py preserve the live
build recipe and its original scratch paths; they are not a clean-checkout
installer. Install the three final candidate snapshots rather than rerunning
older candidate/refinement scripts. Shared sources, executable slots and
Delta inputs were restored and checked after the run.

Attempt 1 fixed the assigned errors but introduced a Decimal DIV/NULL error
in the 186-query initial matrix. Attempt 2 fixed that ordering and the wider
NULL checks, but lost eight existing aggregate/window agreements because
rewrites changed generated field names. Both attempts are rejected and
archived. The final candidate uses the shared guard and NamePreserver; all
prior agreements are retained.
