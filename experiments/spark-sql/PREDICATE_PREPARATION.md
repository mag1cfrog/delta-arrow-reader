# Predicate subquery preparation

[Issue 262](https://github.com/mag1cfrog/delta-arrow-reader/issues/262) owns
four IN/EXISTS preparation failures reproduced on PR 261 integration
`126b424e9f837fb0b458617c74e70dfa1df0e38d`. All four now agree with Spark.
This is an optional Rust experiment; default-build adoption stays with
[issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

## Behavior and implementation

With ANSI enabled, Spark raises DIVIDE_BY_ZERO for these queries. The
accepted runtime returned false:

```sql
SELECT 7L IN (SELECT 7 DIV 0 FROM range(3) WHERE false) AS r;
SELECT EXISTS(SELECT 7 DIV 0 FROM VALUES(1) t(k) LIMIT 0) AS r;
SELECT 7 IN(SELECT 7 DIV 0 FROM VALUES(1) t(k) LIMIT 0) AS r;
```

For `SELECT EXISTS(SELECT CAST('bad' AS INT) FROM range(1))`, Spark returns
true while the accepted runtime raises CAST_INVALID_INPUT.

The [Rust patch](sail-predicate-preparation.patch) prepares uncorrelated
predicate subqueries in the existing analyzer. EXISTS supplies a constant
projection so native column pruning can discard unused outputs. Local VALUES
arithmetic/CAST checks and remaining scalar-subquery checks run before those
outputs disappear. IN retains its output values. Native empty propagation
handles LIMIT 0 beneath projection/alias nodes; an empty IN input retains a
typed head expression required by predicate lowering.

Only `sail-plan/src/decimal_null.rs` changes in the 99-entry selected source
set. Execution nodes, numerical kernels and dependencies remain the same.
Both builds have 505 artifact records and 414 named libraries; only the
`sail_plan` library hash changes. Spark's
[EXISTS rewrite](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/finishAnalysis.scala)
provides source context; the captured Spark 4.2.0 results define this
experiment's reference behavior.

## Validation and boundaries

The [208-query corpus](predicate-preparation.jsonl) covers both ANSI modes,
one-row/range/VALUES inputs, empty and live inputs, LIMIT/filter order,
conditional parents, negation, predicate position, grouping, DISTINCT,
set operations and nested/correlated controls. Four assigned ANSI failures
improve 0/4 -> 4/4; with opposite-mode controls, 4/8 -> 8/8. Fresh Spark agrees
with all eight frozen observations. The full matrix improves 285/416 ->
370/416 without losing a previous value/type/error-cause agreement.

Every remaining strict-comparator difference has a recorded disposition:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 31 | Approved IN/NOT IN NULL semantics, including legacy Spark returning NULL for an empty subquery | Existing policy |
| 13 | Spark REMAINDER_BY_ZERO versus native Divide by zero diagnostics | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 2 | DISTINCT EXISTS and correlated EXISTS still fold an unused invalid CAST | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |

The two EXISTS failures have identical complete before/after payloads.
`classification.json` preserves every SQL statement, reference and actual
result. NULL-policy observations remain differences in the strict totals;
independent expected outputs are checked separately. The focused comparator
supports Boolean/integer rows, preserves duplicates and ordering, and reuses
the existing error classifier. Earlier corpora retain their own comparators.

There are 61 newly successful native observations. Logical/physical nullable
field differences change from 110/18 to 126/19 across successful comparisons.
Already-successful observations have no nullable-field changes. Thirty paired
main-matrix error payloads change. Historical replay retains 911 errors with
one changed message: competing positive and negative Decimal overflow inputs
report a different first failing value. Full payloads remain available;
value/type agreement does not establish full schema or diagnostic agreement.

Prior arithmetic/CAST/scalar passes remain. The scalar-arithmetic matrix
improves 533/586 -> 536/586, scalar CAST 730/750 -> 731/750, and NULL-subquery
stays 816/862. Strict DIV remains 350/350. Historical replay remains
6,603/6,784 over 37 groups with no changed represented value/type/status
outcomes. Integer overflow remains 616/616 with all 131 complete errors
unchanged. Rust suites pass 314 function, 53 planner and 28 runner tests.
The same added planner test fails against baseline private libraries and
passes against candidate libraries, including ANSI on/off/on in one session.

Real-Delta retains all 116 outcomes and 18 adapter checks. Its Spark
comparison stays 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Sixteen queries use nonnullable and 10%-NULL input patterns. Twenty-six
configurations have equivalent successful outputs; six fail on baseline
and have candidate-only timings. Sixteen paired physical plans are unchanged
and ten change. Every successful output row matches the same Spark reference.

The balanced eight-process schedule uses 262,144 rows, batch 8,192, one
partition, four warmups and 21 samples per phase/process on CPU 2. Reported
numbers are medians of four process medians. DataFusion's `reset_plan_states`
runs before repeated execution, outside the execution timer. An initial
harness without that reset failed its row-count check; its diagnostic files
are retained and excluded from these measurements.

The largest planning increase is local VALUES CAST inside IN on the nullable
input fixture: 1.3419425 -> 1.3744425 ms (+2.422%, +0.0325000 ms). All four
candidate process medians exceed all four baseline medians. The nonnullable
fixture changes +2.296%. Ordinary numeric planning changes -0.107%/+0.293%,
and ordinary CAST planning -1.101%/-1.007%.

EXISTS with a nested scalar projection plans 15.438%/15.179% faster. The
largest execution increase is the unchanged ordinary nullable numeric plan:
0.1762170 -> 0.1791575 ms (+1.669%, +0.0029405 ms), with overlapping process
ranges. These measurements do not attribute that variation to the analyzer.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) owns
the complete cost record: SQL, input/settings, raw samples, all process
medians, plans and build/link hashes. No same-binary calibration or
instruction/allocation/memory counters were collected. Previous costs and
final performance acceptance remain open.

## Reproduce

`predicate-preparation-results.json` pins `predicate-preparation-runs.json.gz`.
Extract the archive's `files` map into a scratch directory and run its
`check-archive.py` from the repository root. It verifies source/patch hashes,
frozen comparisons, retained regressions, Delta outcomes and every raw timing
median without rebuilding Rust or starting Spark.

The archive retains reconstruction/build scripts, the exact native regression,
Spark captures, comparisons and benchmark commands. Rebuilding needs the
recorded toolchains/dependencies and adapted cache paths. Shared sources and
executable slots are restored after building, and Delta fixture files are
restored after capture. The first rejected runtime prototype and its lost
controls remain available in `attempt-1/`.
