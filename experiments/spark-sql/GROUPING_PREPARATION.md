# Foldable GROUP BY preparation

[Issue 270](https://github.com/mag1cfrog/delta-arrow-reader/issues/270) owns
ordinary foldable grouping keys in the selected optional Rust runtime. Its
baseline is PR 269 integration `6102bf7c3d7fbee2d31ff3da2fa12aa0e211a8ba`.
[Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) still
owns default-build adoption.

## Behavior and implementation

With ANSI enabled, Spark returns true for both queries below. The accepted
runtime raises a CAST error while evaluating a grouping key whose value is
unused:

```sql
SELECT EXISTS(
  SELECT CAST('bad' AS INT) FROM range(3) GROUP BY CAST('bad' AS INT)
) AS r;
SELECT EXISTS(
  SELECT CAST('bad' AS INT) FROM range(3)
  GROUP BY CAST('bad' AS INT) HAVING count(*) > 0
) AS r;
```

The [Rust patch](sail-grouping-preparation.patch) lifts foldable grouping outputs
into a projection before the existing expression checks and pruning. Live output
expressions remain available and retain their errors. Other grouping keys and
aggregate expressions remain in the aggregate. The shared analyzer uses its
existing borrowed scan and row-expression classifier to select this preparation;
grouping sets are left to their existing path.

If every grouping key is foldable, a constant is projected into a private key
column. The aggregate groups on that column, preserving zero groups on empty
input. The generated name avoids input-field collisions. Simply deleting all
keys would create a global aggregate, which returns one row even when its input
is empty. A literal directly in the aggregate is insufficient because
DataFusion's later constant-group elimination can delete it.

Spark's
[RemoveLiteralFromGroupExpressions](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)
provides the source behavior. The archive retains that source and the inspected
DataFusion 54.1.0 rule. Spark 4.2.0 captures define the reference. This slice uses
native DataFusion projections/aggregates and changes only `decimal-null.rs` in
the selected 99-entry source set. Both builds retain the same package/features,
505 artifact records and 414 named libraries; only `sail_plan` changes.

## Validation and remaining work

The [1,227-query corpus](grouping-preparation.jsonl) preserves all 865 preceding
queries and adds 362. It covers both ANSI modes, empty/live range and VALUES,
one/multiple/mixed keys, selected outputs, COUNT/EXISTS/scalar/IN contexts,
HAVING, live aggregate arguments, nested scalar expressions, LIMIT/OFFSET,
nullable and duplicate inputs, name collisions, volatile keys and grouping-set
boundaries. Batch 1/64 controls accompany the default batch 2 checks.

Strict agreement improves **2,099/2,454 -> 2,397/2,454**, with no lost agreement.
All five assigned ANSI observations are fixed; including opposite modes,
5/10 -> 10/10. There are 288 newly successful observations and ten corrected
empty-group row-count observations. The preceding corpus improves
1,676/1,730 -> 1,681/1,730. All 1,730 retained Spark observations keep their
status, rows, types and error condition in a fresh capture.

The 57 remaining strict differences retain explicit owners:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 39 | Approved IN/NOT IN three-valued NULL policy, independently checked against expected values | Existing policy |
| 13 | REMAINDER_BY_ZERO versus native Divide by zero diagnostics | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 2 | Correlated EXISTS and EXISTS over a row-dependent division grouping key | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |
| 3 | Empty grouping-set output in both modes and unused ROLLUP key error preparation | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) |

The four newly added IN observations use the already approved NULL policy.
The other four added differences reproduce unchanged before/after payloads.
The empty grouping-set discrepancy still needs a source/semantic decision; it
is not automatically accepted as behavior to copy. No new exception is waived.
Full reference/native rows, types, errors and plans remain in `classification.json`.

Logical/physical nullable differences remain 180/76. Already-successful results
have no nullable changes, and paired main-matrix errors have no payload changes.
The numeric comparator is reused for the added floating/Decimal aggregate
controls; Boolean/integer duplicate/order checks and strict error-cause checks
are retained.

All 16 retained CAST/subquery comparisons lose no agreement. Twelve numeric
comparisons are unchanged. Historical replay stays 6,603/6,784 across 37 groups,
with no changed represented value/type/status outcomes. Of 911 historical
errors, one Decimal(38,38) case reports negative instead of positive overflow
from competing failing inputs in its two-partition plan. Both full payloads
remain archived; deterministic failure ordering is not claimed. Strict DIV
stays 350/350 and integer overflow 616/616, with all 131 integer errors unchanged.

Rust suites pass 314 function, 57 planner and 28 runner tests. One added
36-query regression runs ANSI on/off/on in one session; the exact embedded test
fails against baseline private libraries and passes against candidate libraries.
Real-Delta retains 116 outcomes and 18 adapters. Its Spark comparison remains
47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

The unchanged Rust harness measures 16 queries with nonnullable and 10%-NULL
fixtures: 28 equivalent successful configurations and four candidate-only
configurations whose baseline raises. Every successful output row matches Spark.
Printed physical plans are equal for 18 pairs and change for ten. Candidate-only
timings have no speedup ratio.

The balanced eight-process schedule uses 262,144 rows, batch 8,192, one partition,
four warmups and 21 samples per phase/process on CPU 2. Results are medians of
four process medians. DataFusion plan-state resets stay outside execution timing.

Local grouping costs remain unresolved. For nonnullable/nullable fixtures:

- Pure constant-group planning increases 8.554%/8.868%.
- HAVING planning increases 13.290%/13.595%; empty-input HAVING planning
  increases 13.732%/13.835%.
- HAVING execution changes 0.3076810 -> 0.3229845 ms (+4.974%, +0.0153035 ms)
  and 0.3066190 -> 0.3215075 ms (+4.856%, +0.0148885 ms).
- Grouped COUNT execution changes 0.0577125 -> 0.0657070 ms
  (+13.852%, +0.0079945 ms) and 0.0578675 -> 0.0656670 ms
  (+13.478%, +0.0077995 ms).

All four candidate medians exceed all four baseline medians for these listed
increases. Their printed plans change. The added key preserves required empty
input behavior, but this comparison does not establish an unavoidable cost.
Selected-key planning also increases 8.759%/9.476%.

Ordinary numeric planning increases 1.141%/0.117%; nullable execution increases
1.035%. Native CAST execution increases 0.152%/0.464%; these control ranges
overlap. IN execution decreases 2.261%/2.612% in this comparison, which does not
resolve earlier IN costs measured against different parent builds.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) owns
the detailed costs and attribution, including smaller control increases. Every
SQL statement, absolute/relative phase median, process median, raw sample, plan,
output check and source/binary/library identity is retained. No same-binary
calibration or instruction/allocation/memory counters were collected. Earlier
costs and final performance acceptance remain open.

## Reproduce

`grouping-preparation-results.json` pins `grouping-preparation-runs.json.gz`.
Extract its `files` map into a scratch directory and run the archived
`check-archive.py` from the repository root. It verifies source and patch
identities, frozen comparisons, native regression records, Delta outcomes and
every raw timing median without rebuilding Rust or starting Spark.

`prepare.py` reconstructs the accepted source set from the preceding hashed
archive. Build, native-test, Spark-reference and benchmark commands remain in
the evidence. Rebuilding requires the recorded toolchains/dependencies and
adapted cache paths. Shared sources, executable slots and Delta fixtures are
restored after validation.
