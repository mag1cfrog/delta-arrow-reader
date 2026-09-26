# Scalar CAST subquery preparation

[Issue 260](https://github.com/mag1cfrog/delta-arrow-reader/issues/260) owns
CAST failures that should disappear when a scalar subquery is inlined or
prepared as an empty input. This optional Rust patch builds on PR 259,
integration `ca5836f87589b19111b72ac2263ad3b9d637f9a1`.
Default-build adoption remains with
[issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

## Behavior and implementation

With ANSI enabled, these queries return 7 and NULL in Spark:

```sql
SELECT CASE WHEN CAST(NULL AS BOOLEAN)
       THEN (SELECT CAST('bad' AS INT)) ELSE CAST(7 AS INT) END AS r;
SELECT (SELECT CAST('bad' AS INT) FROM range(3) LIMIT 0) AS r;
```

The accepted runtime raises CAST_INVALID_INPUT while simplifying the
subquery. The [Rust patch](sail-scalar-cast-preparation.patch) extends the
existing analyzer's traversal to subqueries. It reuses `one_row_exprs` to
inline physical row expressions over a single empty-schema row, including
projection/alias chains. Remaining subqueries use the existing arithmetic
precheck and CAST preparation before their parent expressions are pruned.
Expressions that still require their own subquery/aggregate context remain
outside this inlining path.

Local VALUES string-column casts must still fail before LIMIT 0 removes
their rows. The shared CAST recognizer now includes these columns as well
as constants. The existing NULL-scale ROUND handling also applies after
inlining removes the scalar-subquery wrapper. Failed prototypes that lost
these error/short-circuit boundaries are retained in the archive.

Only `sail-plan/src/decimal_null.rs` changes in the 99-entry selected source
manifest. No numerical kernel, executor, dependency or feature changes.
Both private builds have 505 artifact records and 414 named libraries;
only the `sail_plan` library hash changes. Production execution remains
Rust. Python captures Spark references and checks evidence.

## Validation and remaining differences

All 24 assigned ANSI observations improve 0/24 -> 24/24. With opposite-mode
controls, agreement improves 24/48 -> 48/48. Fresh Spark results agree with
all 48 frozen references from the preceding investigation.

The [375-query corpus](scalar-cast-preparation.jsonl), run in both ANSI modes,
improves 643/750 -> 730/750. It covers direct, CASE, COALESCE and NULL parents,
aliased/nested one-row projections, live/empty VALUES and range inputs,
LIMIT/filter order, live casts, correlated inputs and cardinality controls.
No previously passing value/type/error-cause observation is lost. The
comparator is unchanged; schema metadata and complete errors are audited
separately.

Twenty comparator differences remain:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 16 | Spark rejects an untyped NULL CASE condition; native accepts it | [150](https://github.com/mag1cfrog/delta-arrow-reader/issues/150) |
| 1 | EXISTS folds an unused failing CAST projection | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |
| 3 | Approved projected-IN NULL policy, including an invalid legacy CAST producing NULL | Accepted policy |

Successful comparisons have 45 logical / 233 physical nullable-field
differences before and 66 / 285 after. There are 85 newly successful native
outcomes. Four existing successful observations, dynamic CASE/IF in both
modes, change the result's physical nullable flag from true to false after
inlining a nonnullable constant. Spark's analyzed field remains nullable.
Rows and types agree, but these metadata differences remain explicit review
inputs rather than full schema acceptance.

All prior dedicated arithmetic/CAST/subquery passes remain. The preceding
scalar-arithmetic matrix stays 533/586, NULL-discarded subqueries 816/862,
and the error-aware DIV group 350/350. Historical replay remains
6,603/6,784 across 37 groups, with no changed value/type/status outcome.
Integer overflow retains 616/616 and all 131 complete error payloads.

The main matrix has 35 changed paired error payloads. Among 911 retained
historical errors, 58 full payloads and 43 error texts change. These include
earlier analyzer errors, optimizer context and competing invalid Decimal
inputs. Exact before/after errors and plans are archived for
[issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149);
matching error categories do not establish identical diagnostics.

Rust suites pass: 314 function, 52 planner and 28 runner tests. The added
planner regression also runs independently against both private library
sets: it fails on baseline and passes on candidate. It exercises INT,
DOUBLE and DECIMAL while switching ANSI on/off/on in one session.
Real-Delta retains 116 outcomes and 18 adapter checks. Its Spark comparison
remains 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Seventeen queries run on nonnullable and 10%-NULL input patterns. Twenty-four
configurations have equivalent successful outputs; ten fail on baseline
and have candidate-only timings. No ratio compares failure with success.
Every successful output row is checked against the same Spark reference.
Eighteen paired physical plans remain identical; six change after inlining.

Each process uses 262,144 rows, batch size 8,192, one partition, four warmups
and 21 samples per phase on CPU 2. The process order is before, after,
after, before, after, before, before, after. Reported results are medians
of four process medians per variant.

The largest planning increase is the local string-column scalar CAST:
0.5578065 -> 0.5826925 ms (+4.461%, +0.0248860 ms). All four candidate
process medians exceed all four baseline medians for that configuration.
The same query on nullable inputs changes +3.999%. This path now performs
the required local CAST precheck. Ordinary CAST planning changes +0.851%
and +1.522%; ordinary numeric projection changes +0.458% and +0.041%.

One-row scalar CAST planning decreases 17.492% and 19.325%; nested one-row
planning decreases 27.773% and 29.295%. The nullable dynamic CASE execution
changes 2.4154820 -> 1.6559260 ms (-31.445%). The largest execution increase
is an unchanged ordinary local-string plan, 0.1687435 -> 0.1700710 ms
(+0.787%); its process ranges overlap.

[Issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) owns
the full CAST cost record. All SQL, settings, raw samples, process medians,
plans and source/build/link hashes are retained. No same-binary calibration
or instruction/allocation/memory measurements were collected. This bounded
comparison does not resolve earlier costs or establish final performance
acceptance.

## Reproduce

`scalar-cast-preparation-results.json` pins the compressed evidence archive.
Extract its `files` map to a scratch directory and run
`python check-archive.py` from this checkout. The verifier checks member and
repository hashes, the 99-entry source manifest, patch application/reversal,
comparisons, regression identity and every timing median. It needs neither
Spark nor a Rust rebuild.

The archive retains `prepare.py`, `candidate.py`, `build.py`,
`run-native-test.py`, reference/comparison scripts and benchmark commands.
Rebuilding needs the recorded toolchains/dependencies and adapted cache
paths. Shared source files and executable slots are restored after the
build and Delta capture. This remains a selected experimental runtime.
