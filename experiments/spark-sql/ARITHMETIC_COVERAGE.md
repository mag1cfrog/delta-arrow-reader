# Arithmetic coverage and open schema/error questions

This reference extends [C04 validation](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) from the original 37 replay groups to the 67 groups already captured for PR 291. The selected runtime is the independently reviewed local Decimal CAST build, merged at `2a82385d1d420cbd88461d43ad6015801e3647ea`. That implementation leaf is closed; schema/error acceptance and C04 remain open.

The index contains 15,945 SQL entries and 31,890 observations, with both ANSI modes for each entry. Corpora overlap. Existing value/type/error comparisons remain 30,513/31,890, including the original 6,603/6,784. This slice recomputes additional dimensions from saved captures; it changes neither query execution nor those original comparison results.

## What the index records

`coverage-groups.json` in the archive lists every group, complete observation IDs, SQL/reference/native member paths and hashes, per-case batch sizes, recorded capture metadata, selected probe identity and separate comparison counts. `dimensions/<group>.json` retains each logical/physical schema and error comparison, with its original ID and case position. `coverage-families.json` lists the exact unresolved IDs by dimension. These are evidence locations; acceptance remains in the issue checklist.

Two source-format differences are handled explicitly:

- 3,173 reference schemas are stored at the observation's outer `schema`, rather than `actual.schema`. Comparison copies include these existing schemas. The 148 previously reviewed reference supplements for `existing-filter` and `existing-contexts` are also reused after verifying their unchanged SQL/status/values/types/conditions.
- Decimal BROUND repeats `scale_0_0_literal` at positions 100/102 and `scale_0_0_column` at 101/103. Both copies and all observations remain. Only comparison copies receive an `@case<position>` suffix; the raw corpus and captures are unchanged. The shared comparator continues to reject unqualified duplicate IDs.

The remaining 578 absent Spark schemas are on failed queries: 528 carry `planning_error` and 50 carry `error`. No successful Spark reference lacks a schema after the recorded-schema lookup. Native logical/physical schemas can still be absent when preparation fails; absence receives no match credit.

The following examples map required input dimensions to exact existing cases. They identify coverage, not acceptance of every outcome. Both ANSI modes and the captured schemas/results remain accessible through the group index.

| Dimension | Existing group / representative IDs |
| --- | --- |
| Constants, columns and Decimal precision/scale | `arithmetic-check`: `small_left_add_literal`, `small_left_add_column` |
| NULL data and NULL arguments | `arithmetic-check/small_left_add_column`; `arguments-check/decimal_null_literal` |
| Empty input and pruning | `cast-strict/tinyint_cast_empty`; `existing-div-evaluation/tinyint_range_false`, `tinyint_range_limit0` |
| Invalid CAST and TRY_CAST | `cast-strict`: `tinyint_cast_invalid0`, `tinyint_try_cast_invalid0` |
| Overflow and large scales | `integer/arithmetic_overflow_ansi`; `existing-high/scales_0_32`; `arguments-check/long_overflow` |
| ROUND/BROUND literal and column forms | `integer-check/TINYINT_-128_-2`; `decimal-check/ties_0_literal`, `ties_0_column` |
| String grammar and nonfinite values | `cast-float`: `float_cast_valid_batch1`, `float_try_cast_valid_batch1` |
| Subquery/NULL preparation | `existing-null-order`: `values_bad_many_left`, `values_bad_many_right` |

`coverage-scope.json` makes these links machine-checkable. Per-group metadata preserves which reference settings were recorded; missing settings are not filled by assumption. Native case batches and both-mode IDs are explicit. The selected probe's settings and build identity remain pinned by the accepted runtime evidence.

## Separate schema and diagnostic results

Counts include overlapping groups and available schemas from failures. Different dimensions also overlap.

| Dimension | Match | Difference | Unobserved |
| --- | ---: | ---: | ---: |
| Logical names | 31,292 | 4 | 594 |
| Logical types/precision/scale | 31,236 | 60 | 594 |
| Logical nullability | 28,439 | 2,857 | 594 |
| Logical field metadata | 31,292 | 4 | 594 |
| Physical names | 26,958 | 4 | 4,928 |
| Physical types/precision/scale | 26,943 | 19 | 4,928 |
| Physical nullability | 20,450 | 6,512 | 4,928 |
| Physical field metadata | 26,958 | 4 | 4,928 |

Among comparable logical nullability differences, native fields are broader in 2,064 observations and tighter in 783; ten have different names/types or structure. Physical differences are broader in 2,064, tighter in 4,429, mixed in 15 and structurally different in four. Broader means native permits NULL where the reference declares nonnullable; tighter means the reverse. This categorization does not approve a schema contract or infer a value error from metadata alone.

Of 5,767 paired failures, the existing corrected classifier finds 4,421 matching known cause categories, 576 ambiguous generic Decimal CAST diagnostics, 539 unclassified causes and 231 different classifications. The differences are 227 `REMAINDER_BY_ZERO` versus `DIVIDE_BY_ZERO` observations and four Spark `STAGE_MATERIALIZATION_MULTIPLE_FAILURES` envelopes versus native `DIVIDE_BY_ZERO`. These preserve the earlier diagnostic questions, including overlapping scalar corpora. Unclassified does not establish a runtime defect; it can also expose missing comparator coverage. No SQLSTATE/parameter equivalence is inferred.

Raw status labels differ in 3,646 observations. Of these, 3,177 compare a generic reference `error` label with a native planning/execution label; 12 compare explicit execution/planning labels; 457 have a success/error outcome difference. The index keeps Spark's separately recorded `failure_stage` when available. A label difference is not automatically a new query regression. Existing accepted NULL-policy cases and unresolved findings retain their prior owners and comparisons.

The original four name/field-metadata differences remain the NULLIFZERO controls. Previously assigned conditional and date findings remain with [conditional validation](https://github.com/mag1cfrog/delta-arrow-reader/issues/150) and [date arithmetic](https://github.com/mag1cfrog/delta-arrow-reader/issues/152). The full family index supplies exact IDs for further routing without changing those acceptance scopes.

## Multi-output first-error counterexample

Independent review of PR 291 added [12 control queries](arithmetic-error-order.jsonl), each in both ANSI modes. Their complete SQL, fresh Spark 4.2.0/JRE 21 and before/after native captures, plans, commands and review provenance are copied byte-for-byte into `reviewer-first-errors/`. `reviewer-source-hashes.json` records the original file identities. These 24 observations per engine remain separate from the 31,890-entry index.

```sql
SELECT CAST(x AS DECIMAL(2,0)) AS a, CAST(y AS DECIMAL(2,0)) AS b
FROM VALUES
  (CAST(0 AS DECIMAL(4,0)), CAST(100 AS DECIMAL(4,0))),
  (CAST(200 AS DECIMAL(4,0)), CAST(0 AS DECIMAL(4,0))) t(x,y)
```

With ANSI enabled, Spark fails on first-row `100` in its second output column. Both native versions fail on second-row `200` in the first column. For `SELECT 7 FROM (<the same query>) s`, the reviewed candidate preserves the required error but still selects 200; the older baseline incorrectly returns two 7s. Spark records optimization failure, condition `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, SQLSTATE 22003 and precision/scale 2/0.

The shared local evaluator checks one expression across the batch before the next expression. Spark's local conversion evaluates rows in source order, with output expressions inside each row. This two-column boundary was outside the accepted single-column repair. It remains unresolved in the first-error coverage owner; later runtime work needs its own bounded native owner and local regression. Global execution serialization is not required by this evidence.

Across the reviewer's controls, after has 14 matching successful values/types and ten paired errors. Eight select the same first value, while `two_columns_true` and `two_columns_unused_true` retain the difference above. The latter two are not counted as equivalent errors even though their range-error categories match. The Boolean controls are compared as Boolean values, with Decimal comparison for numeric columns.

## Remaining acceptance and reproduction

Completed in this slice: index the 67 accepted groups, reuse their recorded schemas, preserve duplicate historical IDs without ambiguous comparison keys, compare separate dimensions and retain the independently reproduced multi-output counterexample. The 92-observation local-CAST focus and 48 prior first-error controls remain in their accepted evidence and are not added to this index.

Still open: schema/nullability contracts, diagnostic identities and parameters, first-error behavior for multiple outputs, interpretation/routing of the listed differences, and the remaining historical reference questions. The original 389 differences, earlier 17-group record (1,552/1,662), six equal-IEEE display controls and integer ROUND extreme-scale/code-generation scouts keep their pinned prior archives. This index does not replace them or claim all possible Spark arithmetic inputs are covered.

`arithmetic-coverage-results.json` pins this archive, its input archives, comparator sources and selected runtime. After extracting the archive's `files` map into a fresh directory, use PyArrow 25.0.1 and run:

```sh
python /path/to/extracted/check-archive.py "$PWD/experiments/spark-sql"
```

The check recomputes every group/dimension, verifies the scope examples and original duplicate-ID rejection, checks all copied reviewer evidence hashes and retains the two F1 differences. No engine, dependency build or timing loop is run by this verification. Runtime costs remain the accepted bounded PR 291 measurements with their existing [performance owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/159); final performance and default-build adoption remain open.
