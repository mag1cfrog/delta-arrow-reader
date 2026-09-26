# IN subquery output binding

[Issue 274](https://github.com/mag1cfrog/delta-arrow-reader/issues/274) owns
the NULL-check output binding for value-producing IN/NOT IN subqueries in the
selected optional Rust runtime. Its baseline is PR 273 integration
`9815372c4013b73ac9cc1f4047df3a93cb2055b9`.
[Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns
default-build adoption.

## Behavior and implementation

The accepted runtime fails to find an internal grouping-expression field for
this query instead of evaluating the division:

```sql
SELECT 7 IN (
  SELECT id DIV 0 AS v FROM range(3)
  GROUP BY id DIV 0 ORDER BY v NULLS FIRST LIMIT 1
) AS r;
```

The existing three-valued IN preparation uses mark joins to determine whether
a value matches, whether the subquery contains NULL and whether it is empty.
Its NULL-presence filter sits above the subquery. Reusing the subquery's output
expression there can reference columns hidden by an intervening projection.
Some inputs fail during decorrelation; others fail in later simplification.

The [Rust patch](datafusion-in-output.patch) builds that filter from the actual
first output column, including its qualifier. The existing head-output check
still validates that a value is available. The match predicate consumes its
expression without an unnecessary clone. Every selected IN/NOT IN caller uses
this shared helper. Filter-only queries keep their existing path.

ANSI division/CAST inputs now raise their required errors. Empty subqueries
return the proper IN/NOT IN result. NULL-containing subqueries retain the
approved SQL three-valued semantics where Spark differs. The repair adds no
join, execution node, dependency or Python UDF.

Only `decorrelate_predicate_subquery.rs` changes in the selected 99-entry
source set. The archive records the existing DataFusion `head_output_expr`,
logical builder and correlation code used to trace the binding. Both builds
retain the same package/features, 505 artifact records and 414 named libraries.
Eight library hashes change through the DataFusion optimizer dependency chain;
`library-check.json` lists them. No Sail source changes in this slice.

## Validation and remaining work

The [1,796-query corpus](in-subquery-output.jsonl) preserves all 1,624 preceding
queries and adds 172. Both ANSI modes cover computed and renamed outputs,
grouping, ordering, LIMIT/OFFSET, subquery aliases, aggregates, selected
IN/NOT IN, WHERE controls, NULL/empty inputs, live errors and correlation.
Batch 1/64 controls accompany the default batch 2 checks.

Strict agreement improves **3,275/3,592 -> 3,445/3,592**, with no lost agreement
and 184 newly successful observations. All 16 assigned missing-field failures
are removed: 12 now agree with Spark and four return the independently expected
NULL under the existing approved policy. The preceding corpus improves
3,155/3,248 -> 3,167/3,248. All 3,248 retained Spark observations keep their
status, rows, types and error condition in a fresh capture.

An independent SQL truth-table check evaluates 168 added legacy-mode cases
from their explicit values and ordered limits. It agrees in 164 cases. Four
WHERE NOT IN controls incorrectly return a row when the subquery contains
NULL. Their complete native payloads are unchanged before/after, so they remain
separate C02 findings. They are not accepted NULL-policy exceptions.

The 147 remaining strict differences retain their existing owners:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 105 | Approved three-valued NULL policy, independently checked | Existing policy |
| 13 | REMAINDER_BY_ZERO versus native Divide by zero diagnostics | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 17 | Correlated EXISTS, correlated IN with LIMIT, UNION error ordering and WHERE NOT IN NULL handling | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |
| 12 | Grouping sets/ROLLUP, HAVING error pruning and volatile grouped-output analysis | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) |

The policy observations comprise 43 retained cases, four newly executable
assigned cases and 58 added controls. All remaining non-policy native payloads
are identical before/after. Full SQL, errors, rows and plans remain in
`classification.json`. No new exception is approved.

Logical nullable differences change 220 -> 303, and physical nullable
differences 144 -> 214, as additional queries succeed. Already-successful
results have no nullable changes. Thirty paired errors change from missing
fields to the expected division/CAST failure. Complete schema/diagnostic
acceptance remains open.

All 16 retained CAST/subquery comparisons lose no agreement. Twelve numeric
comparisons are unchanged. Historical replay stays 6,603/6,784 across 37 groups,
with no changed represented value/type/status outcomes. Strict DIV stays
350/350 and integer overflow 616/616, with all 131 integer errors unchanged.

The 911 historical errors retain their status; 1 complete payload(s) change in the final comparison. The archive preserves the exact errors and plans, including the previously observed competing positive/negative Decimal(38,38) overflow failures. Deterministic failure ordering is not claimed.

Rust suites pass 314 function, 58 planner and 28 runner tests. One embedded
optimizer regression covers 16 grouping/alias/IN/NOT IN plan shapes through
both decorrelation and expression simplification. The exact test fails with
FieldNotFound against baseline libraries and passes against candidate
libraries. It is compiled independently because the patched optimizer is a
dependency outside the host Cargo workspace. Initial test-setup failures are
retained separately. Real-Delta retains 116 outcomes and 18 adapters; its Spark
comparison stays 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

The unchanged Rust harness measures 16 queries with nonnullable and 10%-NULL
fixtures. Every successful output row matches Spark. The fixed schedule uses
262,144 rows, batch 8,192, one partition, four warmups and 21 samples per
phase/process on CPU 2. Results are medians of four process medians, in
before/after/after/before/after/before/before/after order. DataFusion plan-state
resets stay outside execution timing.

There are 26 equivalent successful configurations and 6 candidate-only configurations whose baseline raises. Printed plans are equal for 26 pairs and change for 0. Candidate-only timings have no speedup ratio.

Final measurements still contain unresolved increases. Selected controls and IN paths are shown below; the full tables retain every configuration. All 26 comparable printed plans are equal.

| Query/input pattern | Plan before ms | Plan after ms | Change | Execute before ms | Execute after ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ordinary_numeric_nullsfalse | 0.3219280 | 0.3485725 | +8.277% | 0.1157250 | 0.1157755 | +0.044% |
| in_local_cast_nullsfalse | 1.5475995 | 1.4517470 | -6.194% | 0.2886010 | 0.3130515 | +8.472% |
| native_numeric_nullsfalse | 0.1755360 | 0.1900330 | +8.259% | 0.0860095 | 0.0930585 | +8.196% |
| not_in_computed_nullsfalse | 1.0518640 | 1.1214435 | +6.615% | 3.4921920 | 3.4943705 | +0.062% |
| in_distinct_nullsfalse | 1.4589450 | 1.4449690 | -0.958% | 0.2952285 | 0.3153350 | +6.810% |
| in_local_cast_nullstrue | 1.4182040 | 1.3938140 | -1.720% | 0.2851445 | 0.2941515 | +3.159% |

The final schedule has 2 increases where every candidate process median exceeds every baseline median: `native_in_computed_nullsfalse execution`, `in_local_cast_nullstrue execution`. Other ranges overlap; this does not establish that their increases are noise.

The earlier candidate schedule measured ordinary nonnullable numeric execution +54.691%, native nonnullable CAST planning +35.399%, and native numeric execution +14.330%/+40.936% (nonnullable/nullable fixtures). Those measurements remain open alongside the final schedule. The control variation is substantial; this comparison does not attribute it to the binding change or the removed clone.

The completed comparison before removing the redundant expression clone is
also retained under `before-clone-removal/`, with its source/build identities,
all samples and retained binary hashes. The final source uses the same fixed
schedule. Both schedules remain evidence; changes between them do not establish
that the removed clone caused changes in unrelated numeric/native controls.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains
every increase, absolute cost, SQL statement, process median, raw sample, plan,
output check and source/binary/library identity from both schedules. Equal
plans or overlapping process ranges do not dismiss a measurement. No
same-binary calibration or instruction/allocation/memory counters were
collected. Earlier costs, attribution and final performance acceptance remain
open.

## Reproduce

`in-subquery-output-results.json` pins `in-subquery-output-runs.json.gz`.
Extract its `files` map into a scratch directory and run the archived
`check-archive.py` from the repository root. It verifies source/patch identities,
retained comparisons, NULL-policy expectations, native regression records,
Delta outcomes and every raw timing median in both schedules without rebuilding
Rust or starting Spark.

Apply `datafusion-in-output.patch` at the selected DataFusion optimizer crate
root after the accepted patch chain. `prepare.py` reconstructs the baseline
from the preceding pinned archive. Build, native-test, Spark-reference and
benchmark commands remain in the evidence. Rebuilding requires the recorded
toolchains/dependencies and adapted cache paths. Shared sources, executable
slots and Delta fixtures are restored after validation.
