# Empty local input before scalar preparation

[Issue 282](https://github.com/mag1cfrog/delta-arrow-reader/issues/282) owns this optional Rust repair. Its baseline is PR 281 integration `210f8b68f59a610467b2c8cd2ba5b411e273318a`.

## Behavior

```sql
SELECT k, EXISTS(
  SELECT (SELECT CAST('bad' AS INT) FROM range(1))
  FROM VALUES(0),(1) t(id) WHERE id=o.k AND false
) AS r FROM VALUES(0),(4) o(k) ORDER BY k;
```

The baseline prepares the child scalar CAST and fails. The candidate returns `(0,false),(4,false)`, matching Spark. NOT EXISTS returns true. The same shared change handles proven empty local projections used by ordinary SELECT, scalar subqueries and IN. Live local inputs and range inputs retain their earlier preparation failures.

The [Rust patch](sail-empty-local-scalar.patch) uses DataFusion's existing down/up traversal. Before visiting a projection's scalar children, it asks the existing local-input evaluator whether its input is empty. An empty projection keeps typed, named NULL expressions over zero rows so later IN/scalar lowering still has output expressions. Remaining nodes retain the existing postorder preparation. The local evaluator also recognizes empty projections before testing whether their unused expressions can run locally. Range/table scans, joins and aggregates are not executed during this check.

The first candidate used a bare EmptyRelation. EXISTS passed, but IN/scalar lowering then lacked a head expression. `rejected-bare-empty/` retains that source, build and capture evidence; it was not benchmarked. The final regression includes these sibling paths.

The [pinned Spark optimizer source](https://github.com/apache/spark/blob/f5498a9dab976099d83f5230f7600b52960b63ce/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala#L200) places early local conversion and empty propagation before recursive subquery optimization. That ordering informed this repair; exact boundaries are checked against fresh Spark captures. Source identity and the inference are recorded in `spark-source-inspection.json`.

One of the 101 selected Rust source entries changes: `decimal-null.rs`. There is no new helper, dependency, execution node or Python UDF. Empty input is proven by the retained local evaluator; arbitrary runtime emptiness does not suppress errors.

## Validation

The corpus retains all 2,328 accepted queries and adds 140. Controls cover CAST and arithmetic scalar children, empty/live local and range inputs, EXISTS/NOT EXISTS, SELECT/IN/scalar wrappers, aliases, DISTINCT, LIMIT placement, nested scalars, CASE and explicit batches 2/64. Both ANSI modes produce 4,936 observations. Spark uses local[2], two shuffle partitions, UTC and allowPrecisionLoss=true. Native uses two partitions and per-case batches: 1 by default, with retained 2 and explicit 2/64 controls.

Strict agreement improves **4,535/4,936 -> 4,603/4,936**, with no lost agreement. The preceding matrix improves 4,365/4,656 -> 4,367/4,656, fixing both assigned observations. Fresh Spark status/rows/types/condition preserve all 4,656 preceding observations.

Of 136 independent new target expectations, 124 pass. The remaining 12 are unchanged native analysis failures for nonempty-local or range correlated scalar wrappers; both binaries reject their unaggregated cardinality. Their expected NULL rows and complete failures remain recorded, with no passing-test credit. All new proven-empty local expectations pass. All 48 preceding direct-projection target checks, 156 row-presence checks, 320 legacy expectations and eight NULL boundaries also pass.

| Remaining observations | Owner |
| ---: | --- |
| 108 | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) - relational boundary review |
| 16 | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) - grouping boundary review |
| 48 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - modulo error condition |
| 2 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - stage error condition |
| 159 | Approved SQL NULL policy |

The retained relational count falls from 92 to 90; 18 new correlated-scalar analysis observations bring it to 108. These include the 12 pending independent checks and six ANSI CAST error-cause differences. Two new grouped-empty scalar observations join the existing 14 grouping observations. Modulo diagnostics add 12 controls. Twelve new correlated IN/NOT IN observations follow the already approved SQL NULL policy, with independently checked NULL rows. Every added remaining finding also exists on the baseline. Existing LIMIT/OFFSET, UNION, grouping, cardinality and diagnostic work stays open with its owner; broader coverage is not full Spark compatibility.

There are 68 changed represented outcomes, all strict improvements. No paired-error payload/text or shared-success nullable field changes in the main corpus. Logical nullable differences remain 129 and physical differences remain 225. Full SQL, plans, schemas and errors remain in `classification.json`.

Rust suites pass 314 function, 61 planner and 28 runner tests. The exact embedded native regression checks 108 combinations, fails against baseline libraries and passes against candidate libraries. Twelve numeric comparisons are unchanged; all 16 retained CAST/subquery comparisons preserve their preceding strict agreements. Historical replay remains 6,603/6,784 across 37 groups with zero changed represented outcomes and all 911 complete errors unchanged. Strict DIV remains 350/350; integer overflow remains 616/616 with 131 identical complete errors. Real-Delta preserves 116 outcomes and 18 adapters; its Spark comparison remains 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Fifteen queries each use nonnullable and 10%-NULL inputs. Every successful run verifies every output row against independent expectations before timing. The schedule uses 262,144 rows, batch 8,192, one partition, CPU 2, four warmups and 21 samples per phase/process. Before/after/after/before/after/before/before/after yields four process medians per build; the table reports their median. Plan-state reset stays outside execution timing.

There are 20 equivalent-output configurations and 10 candidate-only configurations. Failing baselines receive no ratio. Comparable printed physical plans are identical for 20 and differ for 0. Only sail_plan changes among 414 named libraries; both builds retain 505 artifact records and the same package/features.

| Query/input pattern | Plan before ms | Plan after ms | Change | Execute before ms | Execute after ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| local_scalar_bad_empty_nullsfalse | baseline error | 0.6585535 | candidate only | baseline error | 0.2943070 | candidate only |
| local_scalar_not_empty_nullsfalse | baseline error | 0.6924660 | candidate only | baseline error | 0.2949580 | candidate only |
| local_scalar_arithmetic_empty_nullsfalse | baseline error | 0.6583330 | candidate only | baseline error | 0.2913960 | candidate only |
| local_scalar_in_empty_nullsfalse | baseline error | 1.0502160 | candidate only | baseline error | 0.2401060 | candidate only |
| local_scalar_is_null_nullsfalse | baseline error | 0.6456035 | candidate only | baseline error | 0.0233330 | candidate only |
| local_scalar_valid_nullsfalse | 0.8441585 | 0.8611445 | +2.012% | 0.4208765 | 0.4260715 | +1.234% |
| ordinary_numeric_nullsfalse | 0.3776615 | 0.3763845 | -0.338% | 0.3566980 | 0.3573390 | +0.180% |
| ordinary_cast_nullsfalse | 0.3002880 | 0.3004885 | +0.067% | 3.8727730 | 3.8973340 | +0.634% |
| native_numeric_nullsfalse | 0.1925630 | 0.1889110 | -1.897% | 0.0852385 | 0.0848625 | -0.441% |
| uncorrelated_exists_nullsfalse | 0.5338265 | 0.5292635 | -0.855% | 0.3014300 | 0.3044250 | +0.994% |
| correlated_valid_nullsfalse | 0.6185940 | 0.6219905 | +0.549% | 3.6148840 | 3.6023460 | -0.347% |
| correlated_bad_nullsfalse | 0.6146220 | 0.6108245 | -0.618% | 3.6082020 | 3.6017950 | -0.178% |
| correlated_legacy_nullsfalse | 0.5758290 | 0.5715760 | -0.739% | 3.6005930 | 3.5960245 | -0.127% |
| correlated_local_false_nullsfalse | 0.5756945 | 0.5730795 | -0.454% | 0.2919275 | 0.2912210 | -0.242% |
| local_bad_false_nullsfalse | 0.5947445 | 0.5903970 | -0.731% | 0.2904045 | 0.2920270 | +0.559% |
| local_scalar_bad_empty_nullstrue | baseline error | 0.6531480 | candidate only | baseline error | 0.2905145 | candidate only |
| local_scalar_not_empty_nullstrue | baseline error | 0.6701795 | candidate only | baseline error | 0.2947375 | candidate only |
| local_scalar_arithmetic_empty_nullstrue | baseline error | 0.6297495 | candidate only | baseline error | 0.2907445 | candidate only |
| local_scalar_in_empty_nullstrue | baseline error | 1.0633100 | candidate only | baseline error | 0.5672140 | candidate only |
| local_scalar_is_null_nullstrue | baseline error | 0.6243200 | candidate only | baseline error | 0.0235340 | candidate only |
| local_scalar_valid_nullstrue | 0.8291955 | 0.8346910 | +0.663% | 0.5247700 | 0.5215735 | -0.609% |
| ordinary_numeric_nullstrue | 0.5052890 | 0.5011760 | -0.814% | 1.8108185 | 1.8202055 | +0.518% |
| ordinary_cast_nullstrue | 0.2775405 | 0.2749755 | -0.924% | 3.8990170 | 3.9082090 | +0.236% |
| native_numeric_nullstrue | 0.1912450 | 0.1903335 | -0.477% | 0.0865360 | 0.0865760 | +0.046% |
| uncorrelated_exists_nullstrue | 0.5090205 | 0.5219245 | +2.535% | 0.3047860 | 0.3043655 | -0.138% |
| correlated_valid_nullstrue | 0.6036660 | 0.6145910 | +1.810% | 3.8174655 | 3.7848050 | -0.856% |
| correlated_bad_nullstrue | 0.6036660 | 0.6029300 | -0.122% | 3.8417955 | 3.7851410 | -1.475% |
| correlated_legacy_nullstrue | 0.5706895 | 0.5711710 | +0.084% | 3.7771860 | 3.8048125 | +0.731% |
| correlated_local_false_nullstrue | 0.5789655 | 0.5726485 | -1.091% | 0.2914560 | 0.2887215 | -0.938% |
| local_bad_false_nullstrue | 0.5924005 | 0.5894300 | -0.501% | 0.2916415 | 0.2907100 | -0.319% |

Largest measured planning increase: `uncorrelated_exists_nullstrue`, 0.5090205 -> 0.5219245 ms (+2.535%). Largest measured execution increase: `local_scalar_valid_nullsfalse`, 0.4208765 -> 0.4260715 ms (+1.234%). All smaller increases and overlapping ranges remain recorded too.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains every SQL input, mode, output check, raw sample, process median, plan and source/binary/library identity. There is no same-binary calibration or instruction/allocation/memory measurement. This comparison does not establish causal attribution, resolve earlier costs or complete performance acceptance. No performance schedule ran for the rejected candidate.

## Reproduce

`empty-local-scalar-results.json` pins `empty-local-scalar-runs.json.gz`. Extract its `files` map to a scratch directory and run `check-archive.py` from this repository checkout for offline verification. It checks the exact patch, embedded regression, rejected candidate, independent expectations and pending controls, retained comparisons, Delta and every raw timing median.

`prepare.py` reconstructs the 101 accepted source entries from the preceding pinned archive. Apply `sail-empty-local-scalar.patch` at the selected Sail root. `build.py` installs and tests the candidate, then restores shared sources and executable slots; private libraries preserve linkage. `spark-phases.py` captures the fresh reference. [Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns default-build and clean-checkout adoption.
