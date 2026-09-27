# EXISTS preparation after correlation simplification

[Issue 280](https://github.com/mag1cfrog/delta-arrow-reader/issues/280) owns this optional Rust repair. Its baseline is PR 279 integration `bce0216c43032f01818d92a71f8e28d147f481fe`.

## Behavior

```sql
SELECT k, EXISTS(
  SELECT CAST('bad' AS INT) FROM VALUES(0),(1) t(id)
  WHERE id=o.k AND false
) AS r FROM VALUES(0),(4) o(k) ORDER BY k;
```

The baseline fails on the unused CAST; the candidate returns `(0,false),(4,false)`, matching Spark. Conversely, `EXISTS(SELECT 7 DIV 0 FROM range(3) WHERE id=o.k AND false)` must retain Spark's ANSI arithmetic failure. The baseline incorrectly returns false; the candidate fails with divide by zero.

The [Rust patch](sail-correlation-free-exists.patch) separates whether a subquery needs preparation from whether its SELECT output can be discarded. Every EXISTS reaches the existing preparation helpers. When Boolean simplification removes the inner-column correlation, the output remains available to the existing local-input and arithmetic checks. Live-correlation pruning, DISTINCT and constant-folding order retain their earlier boundaries. No new helper, dependency, execution node or Python UDF is introduced; only `decimal-null.rs` changes among the 101 selected source entries.

All 12 assigned success/failure behaviors are fixed. Ten also become strict Spark agreements. The remaining two correctly fail on modulo by zero but retain the existing divide-by-zero error spelling; [issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) owns that diagnostic difference. The comparator is unchanged.

## Validation

The corpus retains all 2,218 accepted queries and adds 110. New controls cover false on either side, nested Boolean reductions, NOT true, OR true, local/range inputs, EXISTS/NOT EXISTS, aliases, DISTINCT, scalar children and batches 2/64. Both ANSI modes produce 4,656 observations. Spark uses local[2], two shuffle partitions, UTC and allowPrecisionLoss=true. Native uses two partitions and each case's batch size: 1 by default, with retained 2 and explicit 2/64 controls.

Strict agreement improves **4,319/4,656 -> 4,365/4,656**, with no lost agreement. The preceding 4,436 observations improve 4,147 -> 4,157. Fresh Spark status/rows/types/condition preserve all 4,436 preceding observations. All 48 independent target checks, 156 retained row-presence checks, 320 legacy expectations and eight NULL boundaries pass.

| Remaining observations | Owner |
| ---: | --- |
| 92 | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) - relational boundary review |
| 14 | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) - grouping boundary review |
| 36 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - modulo error condition |
| 2 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) - stage error condition |
| 147 | Approved SQL NULL policy |

The relational count falls from 102 to 90 on retained observations; two new nested-scalar observations bring the current count to 92. Their empty local EXISTS input should prevent a child scalar CAST from being prepared, but both baseline and candidate still fail. Full SQL, Spark plans and native errors remain in `classification.json`. The diagnostic count adds the two transferred modulo targets and ten new modulo controls. These are expanded observations of existing boundaries, not newly lost agreements. LIMIT/OFFSET, UNION, grouping, diagnostics and final compatibility acceptance remain open.

There are 54 changed represented outcomes and 22 changed paired-error payloads/texts. Logical nullable differences remain 129 and physical differences remain 225; no shared-success nullable field changes. Error changes are retained without normalization.

Rust suites pass 314 function, 60 planner and 28 runner tests. The exact embedded native regression covers 144 combinations, fails against the baseline libraries and passes against the candidate libraries. Twelve numeric comparisons are unchanged; all 16 retained CAST/subquery comparisons preserve preceding strict agreements. Historical replay remains 6,603/6,784 across 37 groups with no represented outcome change. Strict DIV remains 350/350; integer overflow remains 616/616 with 131 identical complete errors. Real-Delta preserves 116 outcomes and 18 adapters; its Spark comparison remains 47 matches, 58 differences and 11 pending adapters.

One of the 911 retained historical errors changes its full text: positive versus negative DECIMAL overflow in a query containing both values. A separate 12-process correctness repeat reproduces both messages in the unchanged baseline (9 positive, 3 negative); the candidate returns the positive error in all 12 runs. `error-order-summary.json` retains these captures and identities. This establishes existing baseline variation; it does not establish a universal error-order guarantee. The original changed payload remains recorded in `shared-controls.json`.

## Bounded performance comparison

Fifteen queries each use nonnullable and 10%-NULL inputs. Every successful run validates every output row against independent expectations before timing. The schedule uses 262,144 rows, batch 8,192, one partition, CPU 2, four warmups and 21 samples per phase/process. Before/after/after/before/after/before/before/after yields four process medians per build; the table reports their median. Plan-state reset is outside execution timing.

There are 26 equivalent-output configurations and four candidate-only configurations. Failing baselines receive no ratio. All 26 comparable printed physical plans are identical. Only sail_plan changes among 414 named libraries; both builds retain 505 artifact records and the same package/features.

| Query/input pattern | Plan before ms | Plan after ms | Change | Execute before ms | Execute after ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| local_bad_false_nullsfalse | baseline error | 0.5752040 | candidate only | baseline error | 0.2901940 | candidate only |
| local_bad_false_not_nullsfalse | baseline error | 0.6132440 | candidate only | baseline error | 0.2929095 | candidate only |
| correlated_derived_false_nullsfalse | 0.6046430 | 0.6158690 | +1.857% | 0.2908905 | 0.2899780 | -0.314% |
| ordinary_numeric_nullsfalse | 0.3705230 | 0.3776570 | +1.925% | 0.3570935 | 0.3556855 | -0.394% |
| ordinary_cast_nullsfalse | 0.2983290 | 0.3023165 | +1.337% | 3.8694875 | 3.8767755 | +0.188% |
| native_numeric_nullsfalse | 0.1879545 | 0.1902030 | +1.196% | 0.0849280 | 0.0847025 | -0.266% |
| uncorrelated_exists_nullsfalse | 0.5285115 | 0.5342125 | +1.079% | 0.3044550 | 0.3062635 | +0.594% |
| correlated_valid_nullsfalse | 0.6099930 | 0.6157035 | +0.936% | 3.6010235 | 3.6056070 | +0.127% |
| correlated_bad_nullsfalse | 0.6105495 | 0.6151575 | +0.755% | 3.6148690 | 3.6038240 | -0.306% |
| correlated_legacy_nullsfalse | 0.5651600 | 0.5724335 | +1.287% | 3.6091285 | 3.6082370 | -0.025% |
| correlated_not_nullsfalse | 0.6178075 | 0.6230420 | +0.847% | 3.6215270 | 3.5976770 | -0.659% |
| correlated_row_cast_nullsfalse | 0.6719635 | 0.6829485 | +1.635% | 3.6134370 | 3.6163415 | +0.080% |
| correlated_empty_nullsfalse | 0.5953860 | 0.6050190 | +1.618% | 3.2615430 | 3.2614730 | -0.002% |
| correlated_local_false_nullsfalse | 0.5549355 | 0.5745370 | +3.532% | 0.2899335 | 0.2902040 | +0.093% |
| correlated_dead_case_nullsfalse | 0.6780295 | 0.6795170 | +0.219% | 3.6153750 | 3.5915615 | -0.659% |
| local_bad_false_nullstrue | baseline error | 0.5672990 | candidate only | baseline error | 0.2924480 | candidate only |
| local_bad_false_not_nullstrue | baseline error | 0.5905265 | candidate only | baseline error | 0.2947175 | candidate only |
| correlated_derived_false_nullstrue | 0.5902065 | 0.5880575 | -0.364% | 0.2919325 | 0.2911305 | -0.275% |
| ordinary_numeric_nullstrue | 0.5000485 | 0.5008705 | +0.164% | 1.8185735 | 1.8127515 | -0.320% |
| ordinary_cast_nullstrue | 0.2759925 | 0.2818590 | +2.126% | 3.8923495 | 3.9037555 | +0.293% |
| native_numeric_nullstrue | 0.1881895 | 0.1906450 | +1.305% | 0.0862355 | 0.0857140 | -0.605% |
| uncorrelated_exists_nullstrue | 0.5025285 | 0.5089155 | +1.271% | 0.3052965 | 0.3027825 | -0.823% |
| correlated_valid_nullstrue | 0.5944900 | 0.6001150 | +0.946% | 3.8656550 | 3.7643870 | -2.620% |
| correlated_bad_nullstrue | 0.5913735 | 0.5963425 | +0.840% | 3.7859520 | 3.7504865 | -0.937% |
| correlated_legacy_nullstrue | 0.5639580 | 0.5838845 | +3.533% | 3.7927600 | 3.7774760 | -0.403% |
| correlated_not_nullstrue | 0.6143005 | 0.6287625 | +2.354% | 3.7924940 | 3.7833870 | -0.240% |
| correlated_row_cast_nullstrue | 0.6857985 | 0.6818820 | -0.571% | 3.8087190 | 3.7759135 | -0.861% |
| correlated_empty_nullstrue | 0.5996385 | 0.6047585 | +0.854% | 3.2758445 | 3.2462245 | -0.904% |
| correlated_local_false_nullstrue | 0.5549810 | 0.5780085 | +4.149% | 0.2884955 | 0.2923430 | +1.334% |
| correlated_dead_case_nullstrue | 0.6768170 | 0.6873920 | +1.562% | 3.7981395 | 3.7899595 | -0.215% |

The largest planning increase is `correlated_local_false_nullstrue`: 0.5549810 -> 0.5780085 ms (+4.149%). The largest execution increase is the same configuration: 0.2884955 -> 0.2923430 ms (+1.334%). Its planning process-median ranges do not overlap; its execution ranges do. All smaller increases remain recorded too. This query uses a valid CAST over range with a literal false condition; its name does not imply a local VALUES input.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains every SQL input, mode, plan, output digest, raw sample, process median and source/binary/library identity. There is no same-binary calibration or instruction/allocation/memory measurement. This bounded comparison does not establish causal attribution, resolve earlier costs or complete performance acceptance. The overflow-message repeat is a correctness check outside this one timing schedule.

## Reproduce

`correlation-free-exists-results.json` pins `correlation-free-exists-runs.json.gz`. Extract its `files` map to a scratch directory and run `check-archive.py` from this repository checkout for offline verification. It verifies the patch against its accepted baseline, the exact embedded regression, all comparisons, Delta and raw timing medians.

`prepare.py` reconstructs the 101 accepted source entries from the preceding pinned archive. Apply `sail-correlation-free-exists.patch` at the selected Sail root. `build.py` temporarily installs the candidate and tests it, then restores shared sources and executable slots; private libraries preserve linkage. `spark-phases.py` records the complete fresh reference. [Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns default-build and clean-checkout adoption.
