# Scalar children of empty filters and aggregates

[Issue 286](https://github.com/mag1cfrog/delta-arrow-reader/issues/286) owns this optional Rust repair, based on PR 285 integration `f3ed4f4fb5c03614643434ee2e1aba51b8c4e8ac`.

## Behavior

```sql
SELECT k, EXISTS(
  SELECT count((SELECT CAST('bad' AS INT) FROM range(1)))
  FROM VALUES(0),(1) t(id) WHERE id=o.k AND false GROUP BY id
) AS r FROM VALUES(0),(4) o(k) ORDER BY k;
```

The baseline prepares the invalid scalar CAST before recognizing the empty keyed group. The candidate returns `(0,false),(4,false)`, matching Spark. The same issue occurs with a scalar HAVING predicate. Four retained ANSI EXISTS/NOT EXISTS observations are repaired, with empty WHERE, SELECT, IN and scalar consumers covered as sibling paths.

The [Rust patch](sail-empty-input-scalar.patch) extends the existing preorder guard from Projection to Filter and Aggregate scalar children. It reuses the local evaluator and the selected DataFusion empty-relation rule. The Filter evaluator recognizes zero input rows before asking whether its unused predicate can execute locally. Empty Filter/Aggregate nodes retain their schema in an EmptyRelation; their parent output projection still supplies IN/scalar expressions. Projection nodes retain the existing typed output treatment. Global and empty-grouping-set totals remain intact. No aggregate, range or table scan is executed at planning time.

The first candidate used NULL output projections for all three node kinds. Row checks passed, but three successful COUNT controls changed physical nullability from false to true. That candidate is retained in `rejected-nullable-projection/` and was not timed. The final native regression checks COUNT nullability as well as rows, fails against the rejected libraries, and passes against the final libraries.

One of 101 selected Rust source entries changes, `decimal-null.rs`. No dependency, helper, execution node or Python UDF is added. Execution remains Rust; default-build adoption remains separate.

## Validation

The corpus retains all 2,631 accepted queries and adds 362. Controls cover scalar HAVING/WHERE predicates and aggregate arguments, CAST/arithmetic, EXISTS/NOT EXISTS, SELECT/IN/scalar consumers, local/range and empty/live inputs, aggregate DISTINCT, global/grouping-set totals and batches 2/64. Both ANSI modes produce 5,986 observations. Spark uses local[2], two shuffle partitions, UTC and allowPrecisionLoss=true. Native uses two partitions and per-case batches: 1 by default, with retained 2 and explicit 2/64 controls.

Strict agreement improves **5,367/5,986 -> 5,513/5,986**, with no lost agreement. The preceding matrix improves 4,869/5,262 -> 4,873/5,262, fixing all four assigned observations. Fresh Spark status/rows/types/condition preserve all 5,262 preceding observations.

All 252 new independent expectations pass. Of 298 retained independent expectations, 286 still pass and twelve nonempty-local/range correlated-scalar analysis failures remain explicitly pending with identical native payloads. The retained 156 row-presence checks, 320 legacy expectations and eight NULL boundaries pass too. Pending controls receive no passing-test credit.

| Remaining observations | Owner |
| ---: | --- |
| 164 | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) - relational boundary review |
| 38 | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) - grouping boundary review |
| 98 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - modulo error condition |
| 2 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - stage error condition |
| 171 | Approved SQL NULL policy |

The 126 retained relational observations gain 36 correlated-scalar cardinality controls and two direct-WHERE controls. A false conjunct and a scalar predicate in the same Filter are not the same proof as a Filter over an already empty input; those two native errors remain visible. The grouping count falls from 32 to 28 for retained observations, then gains four ROLLUP argument observations and six empty grand-total count differences. Modulo diagnostics add 36 controls. The 171 approved SQL NULL-policy observations are unchanged. Every remaining native payload is identical before/after; all added findings also exist on the baseline.

The six new mixed grouping-set/ROLLUP/CUBE count controls independently require one row containing zero, which both native builds return. Spark returns no rows. They remain explicit grouping-review differences and are not included in the approved NULL-policy category. General correlated cardinality, grouping/window coverage and diagnostics stay open.

There are 146 changed represented outcomes, all strict improvements, with no paired-error payload/text changes. Shared-success logical and physical nullable fields are unchanged. Logical nullable differences against Spark are 153 -> 165 and physical differences are 249 -> 261: the twelve added comparisons are newly successful ANSI SELECT controls projecting the VALUES `id` column through HAVING/WHERE. Native preserves its nullable field; Spark reports nonnullable. These were previously uncomparable because native execution failed. Their complete schemas remain recorded with diagnostic/schema review. The three COUNT field regressions from the rejected draft are absent from the final candidate.

Rust suites pass 314 function, 63 planner and 28 runner tests. The exact embedded regression covers 228 combinations, fails with baseline libraries, rejects the first draft's COUNT nullability, and passes with the final libraries. Twelve numeric comparisons are unchanged; all 16 retained CAST/subquery comparisons preserve prior strict agreements. Historical replay remains 6,603/6,784 across 37 groups with no changed represented outcomes and all 911 complete errors unchanged. Strict DIV remains 350/350; integer overflow remains 616/616 with 131 identical complete errors. Real-Delta preserves 116 outcomes and 18 adapters; its Spark comparison remains 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Fifteen queries each use nonnullable and 10%-NULL inputs. Every successful run verifies every output row against independent expectations before timing. The schedule uses 262,144 rows, batch 8,192, one partition, CPU 2, four warmups and 21 samples per phase/process. Before/after/after/before/after/before/before/after yields four process medians per build; the table reports their median. Plan-state reset stays outside execution timing.

There are 20 equivalent-output and 10 candidate-only configurations. Failing baselines receive no ratio. Comparable printed physical plans are equal for 20 and changed for 0. Only sail_plan changes among 414 named libraries; both builds retain 505 artifact records and the same package/features.

| Query/input pattern | Plan before ms | Plan after ms | Change | Execute before ms | Execute after ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| having_empty_nullsfalse | baseline error | 0.7344690 | candidate only | baseline error | 0.2939410 | candidate only |
| argument_not_empty_nullsfalse | baseline error | 0.7669495 | candidate only | baseline error | 0.2961700 | candidate only |
| where_empty_nullsfalse | baseline error | 0.7421185 | candidate only | baseline error | 0.2963655 | candidate only |
| argument_arithmetic_in_empty_nullsfalse | baseline error | 1.0932500 | candidate only | baseline error | 0.2372660 | candidate only |
| having_empty_scalar_null_nullsfalse | baseline error | 0.7189400 | candidate only | baseline error | 0.0236135 | candidate only |
| having_valid_live_nullsfalse | 1.1848005 | 1.2084595 | +1.997% | 0.4344815 | 0.4443555 | +2.273% |
| argument_valid_live_nullsfalse | 0.9971120 | 1.0118895 | +1.482% | 0.4284410 | 0.4331645 | +1.102% |
| where_valid_live_nullsfalse | 1.1253905 | 1.1478875 | +1.999% | 0.4307700 | 0.4343165 | +0.823% |
| scalar_grand_total_nullsfalse | 0.9067140 | 0.9273125 | +2.272% | 0.0393780 | 0.0393480 | -0.076% |
| ordinary_numeric_nullsfalse | 0.3777515 | 0.3810875 | +0.883% | 0.3584255 | 0.3567325 | -0.472% |
| ordinary_cast_nullsfalse | 0.3009945 | 0.3052015 | +1.398% | 3.8983205 | 3.8685800 | -0.763% |
| native_numeric_nullsfalse | 0.1892320 | 0.1910950 | +0.985% | 0.0854690 | 0.0849425 | -0.616% |
| uncorrelated_exists_nullsfalse | 0.5344775 | 0.5354700 | +0.186% | 0.3026720 | 0.3074110 | +1.566% |
| group_scalar_valid_empty_nullsfalse | 0.7172325 | 0.7225270 | +0.738% | 0.2887765 | 0.2911960 | +0.838% |
| argument_legacy_nullsfalse | 0.9674175 | 0.9839675 | +1.711% | 0.4308750 | 0.4307150 | -0.037% |
| having_empty_nullstrue | baseline error | 0.7203135 | candidate only | baseline error | 0.2941165 | candidate only |
| argument_not_empty_nullstrue | baseline error | 0.7427150 | candidate only | baseline error | 0.2976480 | candidate only |
| where_empty_nullstrue | baseline error | 0.7112510 | candidate only | baseline error | 0.2950375 | candidate only |
| argument_arithmetic_in_empty_nullstrue | baseline error | 1.1042060 | candidate only | baseline error | 0.5697985 | candidate only |
| having_empty_scalar_null_nullstrue | baseline error | 0.6938585 | candidate only | baseline error | 0.0235240 | candidate only |
| having_valid_live_nullstrue | 1.1651595 | 1.1797565 | +1.253% | 0.5408845 | 0.5380045 | -0.532% |
| argument_valid_live_nullstrue | 0.9666750 | 0.9812225 | +1.505% | 0.5304050 | 0.5268690 | -0.667% |
| where_valid_live_nullstrue | 1.1067560 | 1.1181265 | +1.027% | 0.5315820 | 0.5243890 | -1.353% |
| scalar_grand_total_nullstrue | 0.8821530 | 0.9215665 | +4.468% | 0.0392630 | 0.0398790 | +1.569% |
| ordinary_numeric_nullstrue | 0.5018220 | 0.5239030 | +4.400% | 1.9554870 | 1.8137840 | -7.246% |
| ordinary_cast_nullstrue | 0.2821595 | 0.3034690 | +7.552% | 3.9206770 | 3.8631050 | -1.468% |
| native_numeric_nullstrue | 0.1906845 | 0.1902230 | -0.242% | 0.0862395 | 0.0863555 | +0.135% |
| uncorrelated_exists_nullstrue | 0.5146810 | 0.5346530 | +3.880% | 0.3079620 | 0.3057225 | -0.727% |
| group_scalar_valid_empty_nullstrue | 0.6924960 | 0.7224075 | +4.319% | 0.2921830 | 0.2899540 | -0.763% |
| argument_legacy_nullstrue | 0.9705580 | 0.9853900 | +1.528% | 0.5227460 | 0.5237830 | +0.198% |

Largest measured planning increase: `ordinary_cast_nullstrue`, 0.2821595 -> 0.3034690 ms (+7.552%). Largest measured execution increase: `having_valid_live_nullsfalse`, 0.4344815 -> 0.4443555 ms (+2.273%). All smaller changes and process medians remain recorded too.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains every SQL input, mode, output check, raw sample, process median, plan and source/binary/library identity. Identical printed physical plans do not establish equal execution cost. There is no same-binary calibration or instruction/allocation/memory measurement. Attribution, earlier costs and final performance acceptance remain open. Only the final correct candidate was timed, with one schedule and no performance-driven rewrite.

## Reproduce

`empty-input-scalar-results.json` pins `empty-input-scalar-runs.json.gz`. Extract its `files` map to a scratch directory and run `check-archive.py` from this repository checkout for offline verification. It checks the exact patch, embedded regression, rejected schema behavior, independent expectations, retained pending cases/comparisons, Delta and every raw timing median.

`prepare.py` reconstructs the 101 accepted source entries from the preceding pinned archive. Apply `sail-empty-input-scalar.patch` at the selected Sail root. `build.py` installs and tests the candidate, then restores shared sources and executable slots; private libraries preserve linkage. `spark-phases.py` captures the fresh reference. [Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns default-build and clean-checkout adoption.
