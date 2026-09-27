# Correlated EXISTS preparation

[Issue 278](https://github.com/mag1cfrog/delta-arrow-reader/issues/278) owns this optional Rust repair. Its baseline is PR 277 integration `cb5bf70bc875884b2fd8db475ea45e11e815035b`.

## Behavior

```sql
SELECT k, EXISTS(SELECT CAST('bad' AS INT) FROM range(3) WHERE id=k) AS r
FROM VALUES(0),(4) t(k) ORDER BY k;
```

The baseline folds the unused CAST and fails. The candidate returns `(0,true),(4,false)`, matching Spark. The [Rust patch](sail-correlated-exists.patch) extends the existing preparation path to correlated EXISTS that still requires an inner column. Correlation filters stay in the plan; ordinary native decorrelation executes the result.

The guard performs literal AND/OR/NOT reductions before selecting those references. It intentionally does not fold constant comparisons or typed NULL. Spark's pull-up and subquery optimizer order explains why `AND false` preserves an early CAST error, while `AND (1=2)` and `AND CAST(NULL AS BOOLEAN)` can discard the SELECT value first. This inference was checked against the captured Spark 4.2.0 results. Primary-source identities are in `spark-source-inspection.json`.

Correlated DISTINCT retains its ordinary optimization boundary. Reusing the entire uncorrelated preparation would incorrectly discard its CAST/arithmetic errors. The three rejected candidates preserve evidence for unrestricted pruning, a root-only correlation check, and premature constant folding. No rejected candidate was benchmarked.

The literal Boolean guard has a recorded ceiling: it does not reproduce all Spark BooleanSimplification rewrites. Broader relational coverage remains with [issue 141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141). No new execution node, dependency or Python UDF is introduced. One of the 101 selected Rust source entries changes.

## Validation

The corpus retains all 1,952 accepted queries and adds 266, including equality/non-equality/NULL-safe correlation, unused CAST/arithmetic, aliases, local inputs, DISTINCT/grouping, LIMIT/OFFSET, UNION, nested scalar and live-expression controls. Both ANSI modes produce 4,436 observations. Native batches are specified per case: new cases default to 1, retained cases include 2, and explicit controls use 2/64; two native partitions are used. Spark runs local[2] with two shuffle partitions, UTC and allowPrecisionLoss=true.

Strict Spark agreement changes **4,042/4,436 -> 4,147/4,436**, with no lost strict agreement. The assigned correlated EXISTS failure is fixed. All 156 independent row-presence observations pass, together with 320 retained legacy expectations and eight NULL boundaries. Literal-false ANSI error cases are checked separately from row-presence expectations; they are not waived as correct rows. Fresh Spark status/rows/types/condition retain all 3,904 preceding observations.

| Remaining observations | Owner |
| ---: | --- |
| 102 | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) - relational boundary review |
| 14 | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) - grouping boundary review |
| 24 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - modulo error condition |
| 2 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - stage error condition |
| 147 | Approved SQL NULL policy |

Correlated LIMIT/OFFSET and UNION execution, existing error-ordering findings, grouping and diagnostics remain open with their owners. Added queries expose existing limitations; this table does not imply newly introduced regressions. `classification.json` retains each SQL query and complete before/after/Spark payload. Agreement is not complete Spark compatibility.

Logical nullable differences are 125 -> 129; physical differences are 222 -> 225. The archive retains 51 changed paired-error payloads, including 51 changed error strings. Successful nullable changes are recorded separately.

Rust suites pass 314 function, 59 planner and 28 runner tests. The exact embedded native regression checks 60 shapes, fails against baseline libraries and passes against candidate libraries. Retained numeric/subquery checks and the 37-group historical replay preserve their comparators. Strict DIV remains 350/350; integer overflow remains 616/616. Real-Delta retains 116 outcomes and 18 adapters. Exact retained error changes, if any, remain in `shared-controls.json`.

## Bounded performance comparison

Thirteen queries use nonnullable and 10%-NULL inputs. Each successful run checks every output row against independent expectations before timing. There are 262,144 rows, batch 8,192, one partition, four warmups and 21 samples per phase/process. The fixed schedule is before/after/after/before/after/before/before/after on CPU 2. Reported values are medians of four process medians; plan-state resets stay outside execution timing.

There are 18 equivalent-output configurations and 8 candidate-only configurations. Baseline failures receive no ratio. Comparable printed plans are equal for 18 and change for 0. Both builds retain 505 artifact records and 414 named libraries with identical package/features; changed library names: sail_plan.

| Query/input pattern | Plan before ms | Plan after ms | Change | Execute before ms | Execute after ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| correlated_derived_false_nullsfalse | baseline error | 0.6062960 | candidate only | baseline error | 0.2892830 | candidate only |
| ordinary_numeric_nullsfalse | 0.3611810 | 0.3759680 | +4.094% | 0.3562620 | 0.3589365 | +0.751% |
| ordinary_cast_nullsfalse | 0.2997415 | 0.3000020 | +0.087% | 3.9076375 | 3.8752280 | -0.829% |
| native_numeric_nullsfalse | 0.1897920 | 0.1897425 | -0.026% | 0.0849775 | 0.0851275 | +0.177% |
| uncorrelated_exists_nullsfalse | 0.5310010 | 0.5342375 | +0.610% | 0.3028175 | 0.3059380 | +1.030% |
| correlated_valid_nullsfalse | 0.6120815 | 0.6153275 | +0.530% | 3.5980830 | 3.5840620 | -0.390% |
| correlated_bad_nullsfalse | baseline error | 0.6153730 | candidate only | baseline error | 3.5885600 | candidate only |
| correlated_legacy_nullsfalse | 0.5826275 | 0.5735250 | -1.562% | 3.5987840 | 3.5790730 | -0.548% |
| correlated_not_nullsfalse | baseline error | 0.6264785 | candidate only | baseline error | 3.5803250 | candidate only |
| correlated_row_cast_nullsfalse | 0.6993845 | 0.6794170 | -2.855% | 3.5979130 | 3.5752255 | -0.631% |
| correlated_empty_nullsfalse | baseline error | 0.6034460 | candidate only | baseline error | 3.2326745 | candidate only |
| correlated_local_false_nullsfalse | 0.5558875 | 0.5596190 | +0.671% | 0.2896980 | 0.2907845 | +0.375% |
| correlated_dead_case_nullsfalse | 0.6926920 | 0.6844065 | -1.196% | 3.5918970 | 3.5687430 | -0.645% |
| correlated_derived_false_nullstrue | baseline error | 0.5950800 | candidate only | baseline error | 0.2948575 | candidate only |
| ordinary_numeric_nullstrue | 0.5012865 | 0.5054590 | +0.832% | 1.8184375 | 1.8202355 | +0.099% |
| ordinary_cast_nullstrue | 0.2800150 | 0.2770345 | -1.064% | 3.9172160 | 3.8854465 | -0.811% |
| native_numeric_nullstrue | 0.1913310 | 0.1918160 | +0.253% | 0.0861200 | 0.0856445 | -0.552% |
| uncorrelated_exists_nullstrue | 0.5059700 | 0.5060400 | +0.014% | 0.3065940 | 0.3050865 | -0.492% |
| correlated_valid_nullstrue | 0.5908875 | 0.6037115 | +2.170% | 3.7542225 | 3.7846950 | +0.812% |
| correlated_bad_nullstrue | baseline error | 0.5968135 | candidate only | baseline error | 3.8242980 | candidate only |
| correlated_legacy_nullstrue | 0.5660365 | 0.5712160 | +0.915% | 3.8365415 | 3.7566625 | -2.082% |
| correlated_not_nullstrue | baseline error | 0.6211535 | candidate only | baseline error | 3.7525795 | candidate only |
| correlated_row_cast_nullstrue | 0.6911580 | 0.6895515 | -0.232% | 3.8068460 | 3.7691810 | -0.989% |
| correlated_empty_nullstrue | baseline error | 0.6038870 | candidate only | baseline error | 3.2488990 | candidate only |
| correlated_local_false_nullstrue | 0.5542545 | 0.5595640 | +0.958% | 0.2960155 | 0.2905195 | -1.857% |
| correlated_dead_case_nullstrue | 0.6703800 | 0.6886590 | +2.727% | 3.7549895 | 3.7922535 | +0.992% |

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains every absolute/relative cost, raw sample, process median, SQL, output check, plan and source/binary/library identity. Smaller increases and overlapping ranges remain recorded. No same-binary calibration or instruction/allocation/memory counters were collected. This comparison does not settle attribution, earlier costs or final performance acceptance.

## Reproduce

`correlated-exists-results.json` pins `correlated-exists-runs.json.gz`. Extract its `files` map to a scratch directory and run `check-archive.py` from this repository checkout for offline verification. The script checks the exact Rust patch, regressions, independent expectations, retained captures, Delta outcomes and every raw timing median.

`prepare.py` reconstructs all 101 accepted source entries from the pinned predecessor archive. Apply `sail-correlated-exists.patch` at the selected Sail root. `build.py` temporarily installs the candidate, tests it, then restores shared sources and executable slots; private libraries preserve linkage. Two fresh Spark capture parts retain their original inputs and results. [Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns default-build and clean-checkout adoption.
