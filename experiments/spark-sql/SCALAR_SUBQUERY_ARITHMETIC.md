# Scalar-subquery arithmetic preparation

[Issue 256](https://github.com/mag1cfrog/delta-arrow-reader/issues/256) owns
missing early arithmetic errors inside scalar-subquery plans. The baseline is
PR 255, integration `4cd258ad5d53cef57d2edee62a55e2ed10ff5cc2`.
The [optional Rust patch](sail-scalar-subquery-arithmetic.patch) changes only
`sail-plan/src/decimal_null.rs`. Default-build adoption remains with
[issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

## Behavior

With ANSI enabled, Spark rejects this query during optimization:

```sql
SELECT (SELECT CAST(7 AS INT) DIV CAST(0 AS INT)
        FROM range(3) WHERE false) AS r;
```

The accepted runtime returns NULL. The same gap affects TINYINT, SMALLINT and
BIGINT. Non-ANSI and `FROM range(3) LIMIT 0` must still return NULL.

The analyzer already discovers arithmetic in subqueries, but its actual
preparation walks ordinary plan inputs. The patch reuses that preparation
separately for each scalar-subquery plan, before pruning the outer query.
Each plan keeps its local VALUES conversion, unused-column and LIMIT rules.
No arithmetic kernel changes and no range/table scan, join or aggregate runs
during preparation.

Two boundaries matter. EXISTS can discard output expressions, so the new
check applies only to scalar subqueries. OneRowRelation projections are
inlined before subquery preparation; the existing inlining helper identifies
these and leaves their arithmetic subject to outer NULL/CASE rules. The full
replay caught six NULL-parent regressions in an intermediate prototype; all
are preserved by the final patch. Both rejected prototypes remain archived.

## Validation

The four assigned errors improve 0/4 -> 4/4. Their entire error-aware DIV
regression group improves 346/350 -> 350/350. Fresh Spark phase captures
confirm all four optimization errors and the opposite-mode/LIMIT controls.
These are the previously masked division errors, not the unrelated Arrow field
errors repaired in PR 255.

The [293-query matrix](scalar-subquery-arithmetic.jsonl), run in both ANSI
modes, improves 437/586 -> 525/586 under the unchanged comparator. No previous
passing observation is lost. Coverage includes four integer widths, checked
addition, modulo, floating/Decimal division, local VALUES versus range, empty
filters, LIMIT, one-row inlining, nesting, CASE/COALESCE, NULL parents, ROUND,
unused outputs, cardinality and predicate-subquery controls.

The remaining 61 comparator differences have separate owners:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 24 | Both engines report ARITHMETIC_OVERFLOW, but the unchanged comparator misses the native explicit tag | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 24 | Spark REMAINDER_BY_ZERO versus native Divide by zero error | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 8 | A NULL CASE condition still executes an inlined scalar-subquery branch | [150](https://github.com/mag1cfrog/delta-arrow-reader/issues/150) |
| 3 | Missing early arithmetic errors in IN/EXISTS inputs | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |
| 2 | Approved projected-IN NULL policy | Accepted policy |

The 11 value/error-behavior gaps have identical before/after payloads. They
remain review inputs with those existing owners. A matching status alone is
not acceptance. The 24 explicitly tagged overflow errors are retained as
comparator limitations, not mislabeled runtime failures. Schema differences
on successful observations remain 8 logical / 56 physical, owned by the
schema review; this patch does not change nullable contracts.

All 37 historical groups retain their previous passes: coarse agreement
improves 6,599/6,784 -> 6,603/6,784. All prior dedicated numeric, CAST and
subquery passes remain, including 616/616 integer-overflow observations.
The error audit retains 907 paired failures. Four Decimal-CAST messages gain
the analyzer prefix, and two captures select another failing Decimal input;
complete before/after errors remain archived. This does not establish a new
ordering guarantee for competing errors.

Rust tests pass: 314 function, 51 planner and 28 runner. The new SQL regression
also fails against the accepted private libraries, proving it detects the
missing error. Real-Delta results retain all 116 outcomes and 18 adapters.
These are bounded checks, not full Spark conformance.

## Performance

The same Rust benchmark source is linked to before/after private libraries:
505 artifacts / 414 named libraries each, identical package versions/features,
with only `sail_plan` changing. All 28 configurations retain identical physical
plans and outputs. Every output row is checked against the Spark reference.

The 14 queries cover ordinary column arithmetic, scalar arithmetic, NULL and
LIMIT controls, nesting and native controls, with nonnullable and 10%-NULL
inputs. Each process uses 262,144 rows, batch size 8,192, one partition, four
warmups and 21 samples, pinned to CPU 2. Eight processes run in fixed balanced
order: before, after, after, before, after, before, before, after.

The largest observed planning increase is scalar LIMIT 0 with nullable inputs:
0.4051180 -> 0.4259260 ms, +5.136% (+0.0208080 ms). The largest execution
increase is scalar Decimal division with nullable inputs:
0.0862405 -> 0.0893705 ms, +3.629% (+0.0031300 ms). Unchanged native controls
also move. This bounded run has no same-binary calibration, instruction,
allocation or memory measurements; it cannot attribute every difference to
the patch or establish that performance work is complete.
[Issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) retains
cost acceptance, with every SQL statement, input pattern, sample, process
median, source/build/link identity and plan archived.

## Reproduce the evidence

`scalar-subquery-arithmetic-results.json` identifies the hashed
`scalar-subquery-arithmetic-runs.json.gz` archive. Its `files` map contains all
frozen sources, corpora, Spark/native plans, exact errors, scripts and timings;
`sha256` verifies each member. Extract it to a scratch directory and run
`python check-archive.py` from this checkout to verify frozen comparisons,
patch application/reversal, source/link identities and raw timing medians.
No Spark process or rebuild is needed for that verification.

`prepare.py` reconstructs the 99-entry accepted source manifest and verifies
its archives. `build.py` installs the single candidate file, performs the
locked/offline release build and tests, then restores shared source and binary
slots. `check.py`, `replay.py`, `validate.py`, `retained-casts.py` and
`capture-delta.py` capture the scoped and retained checks. The Spark helpers
provide references only; production execution and the regression are Rust.
`link-bench.py` and `measure.py` preserve private artifact identity and raw
samples. Rebuilding requires the recorded dependencies/toolchains and adapting
the archived local cache paths.
