# Arithmetic first-error reference

This slice reviews the four named first-error questions in [arithmetic validation](https://github.com/mag1cfrog/delta-arrow-reader/issues/149). It uses the unchanged PR 287 optional runtime and PR 288's schema-capturing probe, on integration `885cf21575cb2dff651f7de60df0082afc6bd2dc`.

The remaining Decimal-to-Decimal narrowing case still reports different invalid inputs across native executions. It has one runtime implementation owner, [local Decimal narrowing preparation](https://github.com/mag1cfrog/delta-arrow-reader/issues/289). The other three queries consistently select the same input or cause as Spark in this bounded check. Generic string-to-Decimal diagnostics still erase their cause, so stable input selection does not establish full diagnostic equivalence.

## What the reference establishes

Spark 4.2.0's `ConvertToLocalRelation` applies an interpreted projection with `data.map(projection(_).copy())`. These four projections run over an ordered local VALUES collection before the outer sort or physical execution. Fresh captures place all their ANSI failures in optimization and record the condition, SQLSTATE and message parameters. This establishes the expected input for these local queries; ORDER BY does not establish general error order for distributed execution.

Two fixed processes per engine each run four repetitions of every original query and eight single-cause controls, in both ANSI modes. That gives 96 observations per engine and eight ANSI repetitions per original query. All original SQL remains in [the corpus](arithmetic-first-errors.jsonl); the archive also retains the four historical records and pins the earlier repeat evidence.

| Original observation | Spark, 8/8 | Selected native runtime, 8 runs | Disposition |
| --- | --- | --- | --- |
| `existing-cast/high_scale_batch1_true` | Decimal `1` cannot fit `(38,38)` | Positive overflow 5, negative overflow 3 | Runtime correction in issue 289 |
| `existing-string/max_integer_cast_batch4_true` | Fractional 38-digit string ending in `.5` exceeds `(38,0)` | Same input 8 | Input order agrees; cause remains ambiguous |
| `existing-unicode/round_overflow_cast_batch1_true` | Fullwidth positive input, numeric value `9.95`, exceeds `(2,1)` | Same source input 8 | Input order agrees; cause remains ambiguous |
| `bigint/scalar_array_-9223372036854775808_true` | Integral overflow precedes zero division | Integral overflow 8 | Retains the earlier local arithmetic repair |

The eight controls isolate positive/negative Decimal narrowing, string target/source range failures, positive/negative Unicode inputs and BIGINT overflow/zero division. Source range `1e38` reports Spark `NUMERIC_OUT_OF_SUPPORTED_RANGE`; the fractional target-range failure reports `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`. Native string CAST uses the same generic diagnostic format for both, which remains ambiguous under the corrected comparator.

Of 48 paired errors, 24 have matching known cause categories and 24 remain ambiguous. None of these counts proves a native SQLSTATE or message-parameter API. All 48 successful observations preserve types and values under each query's ordering contract. The legacy comparator reports 48/48 and 47/48: the latter flags one different row order in the BIGINT overflow control without ORDER BY. Its row multiset, including duplicates and NULL, is unchanged. Both original comparison reports remain archived.

## Why the remaining case varies

The selected `cast_reachability` implementation calls `string_cast` before evaluating a local projection. The narrowing query's Decimal source column fails that predicate, so its outer CAST remains in the physical plan above `RepartitionExec: RoundRobinBatch(2)`. Competing positive and negative overflow batches can then surface different failures. Single-positive and single-negative controls return their respective range errors.

The string CAST queries already use local preparation, and the BIGINT expression uses the arithmetic precheck. Their current results and selected source explain the changed position since the earlier four-question handoff. Earlier disagreeing captures remain evidence; this report does not replace them or accept a nondeterministic-error exception. The implementation leaf owns the narrowing correction and its regression/cost acceptance. General schema, diagnostic and coverage acceptance stays in the validation leaf.

## Verification

`arithmetic-first-errors-results.json` pins the archive, exact selected source/probe, Spark sources and prior evidence. Spark uses JRE 21, `local[2]`, two shuffle partitions, UTC and `allowPrecisionLoss=true`. Native uses two partitions and the original per-case batch sizes. The Rust runtime is unchanged, so existing cost evidence remains applicable.

Extract the archive's `files` map into a fresh directory and use the existing PyArrow 25.0.1 environment:

```sh
python /path/to/extracted-first-errors/check-archive.py "$PWD/experiments/spark-sql"
```

The checker recomputes every reported count and successful comparison, checks each original query against its retained handoff, and validates source/probe identity against the accepted archives. `spark-reference.py` and `run.py` record the two fixed processes per engine. The report covers these four existing questions and their controls; newer focused corpora and full C04 acceptance remain open.
