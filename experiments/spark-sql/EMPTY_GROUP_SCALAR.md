# Empty keyed groups before scalar preparation

[Issue 284](https://github.com/mag1cfrog/delta-arrow-reader/issues/284) owns this optional Rust repair, based on PR 283 integration `44737db84e4640eebfdd73a6043efee655eb188a`.

## Behavior

```sql
SELECT k, EXISTS(
  SELECT (SELECT CAST('bad' AS INT) FROM range(1))
  FROM VALUES(0),(1) t(id) WHERE id=o.k AND false GROUP BY id
) AS r FROM VALUES(0),(4) o(k) ORDER BY k;
```

The baseline prepares the child scalar CAST and fails. The candidate recognizes that this ordinary keyed group is empty and returns `(0,false),(4,false)`, matching Spark. NOT EXISTS returns true. The same shared check preserves empty SELECT, scalar and IN output behavior.

The [Rust patch](sail-empty-group-scalar.patch) adds an Aggregate case to the existing local-input evaluator. After that evaluator proves the input has zero rows, it substitutes a temporary EmptyRelation input and asks the selected DataFusion `PropagateEmptyRelation` rule whether the aggregate itself has zero output rows. This reuses the already selected guard for global aggregates and empty grouping sets. Those forms retain their grand-total row. No aggregate is executed during planning, and grouping-set expansion is not reimplemented.

This supplies the empty-input proof to the existing typed projection preparation from PR 283. Live inputs retain preparation errors; range/table scans are not evaluated locally. One of 101 selected Rust source entries changes, `decimal-null.rs`, with 22 production lines and one embedded regression. There is no new dependency, execution node or Python UDF. Default-build adoption remains separate.

## Validation

The corpus retains all 2,468 accepted queries and adds 163. Controls cover invalid CAST and arithmetic scalar children, EXISTS/NOT EXISTS, SELECT/IN/scalar consumers, local/range and empty/live inputs, aliases, DISTINCT, HAVING, keys, aggregate arguments, LIMIT, global/grouping-set totals and batches 2/64. Both ANSI modes produce 5,262 observations. Spark uses local[2], two shuffle partitions, UTC and allowPrecisionLoss=true. Native uses two partitions and per-case batches: 1 by default, with retained 2 and explicit 2/64 controls.

Strict agreement improves **4,795/5,262 -> 4,869/5,262**, with no lost agreement. The preceding matrix improves 4,603/4,936 -> 4,605/4,936, fixing both assigned observations. Fresh Spark status/rows/types/condition preserve all 4,936 preceding observations.

All 114 new independent expectations pass, including global, GROUPING SETS, ROLLUP and CUBE count results over empty input. Of the preceding 136 scalar expectations, 124 still pass and 12 correlated-scalar analysis failures remain explicitly pending with identical before/after payloads. All 48 earlier direct-projection expectations, 156 row-presence checks, 320 legacy expectations and eight NULL boundaries pass.

| Remaining observations | Owner |
| ---: | --- |
| 126 | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) - relational boundary review |
| 32 | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) - grouping boundary review |
| 62 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - modulo error condition |
| 2 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - stage error condition |
| 171 | Approved SQL NULL policy |

The grouping count falls from 16 to 14 for retained observations, then gains 18 new controls: four scalar-preparation differences in HAVING/aggregate arguments, eight scalar-output observations above ROLLUP/grouping sets, and six empty grand-total row differences. Eighteen new correlated-scalar cardinality observations join the 108 retained relational observations. Modulo diagnostics add 14 controls. Twelve new IN/NOT IN observations follow the approved SQL NULL policy, with independently checked NULL rows. Every remaining native payload is identical before/after; these findings are additional coverage, not regressions.

Spark returns no rows for the six mixed grouping-set/ROLLUP/CUBE count controls, while both native builds return the independently required count row of zero. They remain explicit grouping-review differences; they are not hidden in the approved NULL-policy category. HAVING scalar children and aggregate scalar arguments remain outside the proven-empty output-projection repair. Broader grouping, scalar cardinality and error-diagnostic acceptance stays open.

There are 74 changed represented outcomes, all strict improvements. No paired-error payload/text or shared-success nullable field changes occur in the main corpus. Logical nullable differences remain 129 and physical differences remain 225. Full SQL, plans, schemas and errors remain in `classification.json`.

Rust suites pass 314 function, 62 planner and 28 runner tests. The exact embedded native regression covers 108 combinations, fails with baseline libraries and passes with candidate libraries. Twelve numeric comparisons are unchanged; all 16 retained CAST/subquery comparisons preserve prior strict agreements. Historical replay remains 6,603/6,784 across 37 groups, with no changed represented outcomes and all 911 complete errors unchanged. Strict DIV remains 350/350; integer overflow remains 616/616 with 131 identical complete errors. Real-Delta preserves 116 outcomes and 18 adapters; its Spark comparison remains 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Fifteen queries each use nonnullable and 10%-NULL inputs. Every successful run verifies every output row against independent expectations before timing. The schedule uses 262,144 rows, batch 8,192, one partition, CPU 2, four warmups and 21 samples per phase/process. Before/after/after/before/after/before/before/after yields four process medians per build; the table reports their median. Plan-state reset stays outside execution timing.

There are 20 equivalent-output and ten candidate-only configurations. Failing baselines receive no ratio. All 20 comparable printed physical plans are identical. Only sail_plan changes among 414 named libraries; both builds retain 505 artifact records and the same package/features.

| Query/input pattern | Plan before ms | Plan after ms | Change | Execute before ms | Execute after ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| group_scalar_bad_empty_nullsfalse | baseline error | 0.6986625 | candidate only | baseline error | 0.2901935 | candidate only |
| group_scalar_not_empty_nullsfalse | baseline error | 0.7351750 | candidate only | baseline error | 0.2920580 | candidate only |
| group_scalar_arithmetic_empty_nullsfalse | baseline error | 0.6893705 | candidate only | baseline error | 0.2881900 | candidate only |
| group_scalar_in_empty_nullsfalse | baseline error | 1.0815135 | candidate only | baseline error | 0.2369505 | candidate only |
| group_scalar_null_nullsfalse | baseline error | 0.6817265 | candidate only | baseline error | 0.0234535 | candidate only |
| group_scalar_valid_live_nullsfalse | 0.9609550 | 0.9734935 | +1.305% | 0.4275290 | 0.4292465 | +0.402% |
| group_scalar_valid_empty_nullsfalse | 0.8344795 | 0.7150080 | -14.317% | 0.2888815 | 0.2901635 | +0.444% |
| group_grand_total_nullsfalse | 0.7319300 | 0.7317990 | -0.018% | 0.0376345 | 0.0369735 | -1.756% |
| ordinary_numeric_nullsfalse | 0.3741450 | 0.3793295 | +1.386% | 0.3581850 | 0.3590120 | +0.231% |
| ordinary_cast_nullsfalse | 0.2978080 | 0.2998120 | +0.673% | 3.8844900 | 3.8824660 | -0.052% |
| native_numeric_nullsfalse | 0.1924575 | 0.1896975 | -1.434% | 0.0848375 | 0.0848330 | -0.005% |
| uncorrelated_exists_nullsfalse | 0.5331400 | 0.5311760 | -0.368% | 0.3016350 | 0.3057780 | +1.374% |
| correlated_valid_nullsfalse | 0.6139805 | 0.6144915 | +0.083% | 3.5629125 | 3.5610095 | -0.053% |
| local_scalar_valid_nullsfalse | 0.8605885 | 0.8573175 | -0.380% | 0.4188530 | 0.4223145 | +0.826% |
| group_scalar_legacy_nullsfalse | 0.9406820 | 0.9402815 | -0.043% | 0.4270835 | 0.4286315 | +0.362% |
| group_scalar_bad_empty_nullstrue | baseline error | 0.6846165 | candidate only | baseline error | 0.2926140 | candidate only |
| group_scalar_not_empty_nullstrue | baseline error | 0.7053495 | candidate only | baseline error | 0.2961750 | candidate only |
| group_scalar_arithmetic_empty_nullstrue | baseline error | 0.6643290 | candidate only | baseline error | 0.2901635 | candidate only |
| group_scalar_in_empty_nullstrue | baseline error | 1.0920585 | candidate only | baseline error | 0.5656155 | candidate only |
| group_scalar_null_nullstrue | baseline error | 0.6561335 | candidate only | baseline error | 0.0232330 | candidate only |
| group_scalar_valid_live_nullstrue | 0.9442290 | 0.9471845 | +0.313% | 0.5267985 | 0.5242185 | -0.490% |
| group_scalar_valid_empty_nullstrue | 0.8083710 | 0.6884485 | -14.835% | 0.2932750 | 0.2873485 | -2.021% |
| group_grand_total_nullstrue | 0.7122935 | 0.7045640 | -1.085% | 0.0378150 | 0.0373745 | -1.165% |
| ordinary_numeric_nullstrue | 0.4992675 | 0.5197200 | +4.097% | 1.8183420 | 1.7986610 | -1.082% |
| ordinary_cast_nullstrue | 0.2784775 | 0.3004280 | +7.882% | 3.8886125 | 3.8819000 | -0.173% |
| native_numeric_nullstrue | 0.1945815 | 0.1924725 | -1.084% | 0.0862200 | 0.0852435 | -1.133% |
| uncorrelated_exists_nullstrue | 0.5089255 | 0.5341325 | +4.953% | 0.3048865 | 0.3069650 | +0.682% |
| correlated_valid_nullstrue | 0.5935775 | 0.6174765 | +4.026% | 3.7127260 | 3.7342605 | +0.580% |
| local_scalar_valid_nullstrue | 0.8344500 | 0.8345205 | +0.008% | 0.5169455 | 0.5153225 | -0.314% |
| group_scalar_legacy_nullstrue | 0.9126500 | 0.9127700 | +0.013% | 0.5187690 | 0.5225605 | +0.731% |

Largest measured planning increase: `ordinary_cast_nullstrue`, 0.2784775 -> 0.3004280 ms (+7.882%). Largest measured execution increase: `uncorrelated_exists_nullsfalse`, 0.3016350 -> 0.3057780 ms (+1.374%). All smaller increases and process medians remain recorded too. Planning for the successful empty-group controls decreases 14.317% and 14.835% on the nonnullable and nullable fixtures, respectively.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains every SQL input, mode, output check, raw sample, process median, plan and source/binary/library identity. The largest planning increase is an ordinary CAST control with no aggregate, so this schedule does not establish that the new Aggregate branch caused it. Identical printed physical plans do not prove equal execution cost. There is no same-binary calibration or instruction/allocation/memory measurement. Attribution, earlier costs and final performance acceptance remain open; this slice ran one schedule and made no performance-driven rewrite.

## Reproduce

`empty-group-scalar-results.json` pins `empty-group-scalar-runs.json.gz`. Extract its `files` map to a scratch directory and run `check-archive.py` from this repository checkout for offline verification. It checks the exact patch, embedded regression, independent expectations, retained pending cases/comparisons, Delta and every raw timing median.

`prepare.py` reconstructs the 101 accepted source entries from the preceding pinned archive. Apply `sail-empty-group-scalar.patch` at the selected Sail root. `build.py` installs and tests the candidate, then restores shared sources and executable slots; private libraries preserve linkage. `spark-phases.py` captures the fresh reference. [Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns default-build and clean-checkout adoption.
