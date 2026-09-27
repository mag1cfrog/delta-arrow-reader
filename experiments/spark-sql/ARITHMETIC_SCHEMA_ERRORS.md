# Arithmetic schema and error audit

This evidence slice advances [arithmetic validation](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) on integration `9c748be5cca48024e882645ada654ca9d46b7af4`. It records schemas and compares diagnostic dimensions for the existing 37 replay groups. It changes the probe and comparison code; all 505 selected library artifacts, representing 414 named libraries, are unchanged. C04 acceptance remains open.

The native recapture preserves all 6,784 previous observations, including their plans, rows, types, nullable flags and 911 complete error records. The original value/type/coarse-stage comparison remains **6,603/6,784**. That count does not include the additional schema or error checks below.

## What is captured

The Rust probe writes complete logical and available physical Arrow schemas as IPC bytes, including field metadata, before collecting rows. Execution failures retain whichever schemas were reached. The capture keeps internal Arrow names and `NamedPlan` output names separately. Comparison uses the same public names as the existing host adapter, without changing the plan or raw capture.

The comparator reuses `extracted.spark_field`, `oracle.schema_dimensions` and the existing arithmetic and Decimal cause classifiers. It compares names, types including Decimal precision/scale, nested nullability and field metadata independently. Raw IPC also retains Arrow representation and schema-level metadata; Spark field-schema comparison does not establish equivalence of Arrow-specific representation or schema-level metadata.

Two frozen Spark groups, `existing:filter` and `existing:contexts`, lacked schemas. Fresh Spark 4.2.0 captures add them for 148 observations. Their status, rows, types and condition are identical to the frozen records. Both versions remain in the archive. The remaining groups use their original references.

Both ANSI modes use UTC and `allowPrecisionLoss=true`. Spark uses `local[2]` and two shuffle partitions; native uses two partitions and each case's batch size, defaulting to one. SQL and case IDs retain explicit CAST/VALUES/range inputs. The report records the full reference environment and probe build commands.

## Separate comparison results

These are observation counts, including schemas available before a query fails. Dimensions overlap and must not be added together. `Unobserved` means that one side did not produce a schema at that stage; it receives no matching credit.

| Schema dimension | Match | Difference | Unobserved |
| --- | ---: | ---: | ---: |
| Logical field names | 6,499 | 4 | 281 |
| Logical types | 6,487 | 16 | 281 |
| Logical nullability | 5,061 | 1,442 | 281 |
| Logical field metadata | 6,499 | 4 | 281 |
| Physical field names | 5,931 | 4 | 849 |
| Physical types | 5,931 | 4 | 849 |
| Physical nullability | 4,104 | 1,831 | 849 |
| Physical field metadata | 5,931 | 4 | 849 |

The four name/metadata observations are the same FLOAT/DOUBLE `NULLIFZERO` controls. Twelve logical-type observations concern CASE coercion and have matching physical types; four date-arithmetic observations differ at both stages. These retain their existing [conditional](https://github.com/mag1cfrog/delta-arrow-reader/issues/150) and [date](https://github.com/mag1cfrog/delta-arrow-reader/issues/152) review scopes. Nullability differences need cause and contract review: this report does not accept either broader logical fields or tighter fields after optimization as a blanket exception.

Of 905 paired errors, 603 have the same known cause classification, 82 have ambiguous Decimal CAST diagnostics and 220 remain unclassified. Ambiguous and unclassified errors receive no cause-match credit. The 24 harness-stage differences are reported separately. Matching cause categories does not verify SQLSTATE, message parameters, the first invalid input or a native typed-error API.

`*-complete-check.json` records each comparison; `schema-differences.json` and `error-families.json` group exact IDs for review. The counts do not represent independent defects or full Spark compatibility.

## Decimal diagnostic correction

Independent review R1 found that the original comparator treated every `Cannot cast` message as invalid syntax. The selected Rust string-to-Decimal adapter uses the same message for malformed input, source range rejection and target precision overflow. For example, `CAST('bad' AS DECIMAL(10,2))` and `CAST('99999999.995' AS DECIMAL(10,2))` have distinct Spark conditions but the same native diagnostic format. Its payload alone cannot establish the cause.

The corrected comparison marks all 82 generic native Decimal CAST errors as ambiguous: 71 previously counted as matches and 11 as differences. Forty other errors have explicit Arrow range or Decimal division overflow messages; the existing Decimal classifier and one exact division message identify them. This changes 38 unclassified observations and two classified differences to matches. Eight ROUND overflow observations retain their distinct `WITHOUT_SUGGESTION` condition. The resulting absence of classified differences does not resolve the 302 ambiguous or unclassified observations.

The archive's `diagnostic-review/` directory retains every changed ID, SQL, prior classification, the exact selected Rust sources and the independent reviewer's three fresh CAST controls in both ANSI modes. It pins the previous commit `85c536c2b679ba1f76ec97ce9947d34cd1efd73a` and archive. All original raw captures, schema dimensions, stages and numeric comparisons remain unchanged. These six independent controls are separate from the 6,784-observation replay. Runtime diagnostic repairs and the remaining reference questions stay open under the existing validation scope.

## Retained coverage and remaining work

The archive preserves the original 389 differing observations with SQL and both original captures, plus their current native records. It separately retains the earlier 17-group summary, 1,552/1,662, and pins its original archive. Neither historical set is added to the current total. Six Float32 decimal-display differences are still reported by the old comparator and independently verified to have equal IEEE values.

This recapture covers the 37 retained replay groups, including their arithmetic/CAST, constant/column, NULL/empty and expression-context controls. It is not a complete inventory of every later focused ROUND/CAST corpus. The separate 5,986-observation scalar matrix remains with its existing evidence; its 98 modulo-condition and two wrapped-condition observations are retained here as diagnostic inputs, without adding them to the replay total.

The next work within the existing validation leaf is to classify the recorded schema and error families, resolve the four retained first-invalid-input questions and map the newer focused corpora to the same acceptance dimensions. This report introduces no new compatibility exception and closes no runtime, performance or integration issue. [Default-build adoption](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) remains separate.

## Verification and reproduction

One native test checks IPC preservation of field/schema metadata, duplicate output names, empty output and error-path schema capture. Ten new and retained Python checks pass, including precision/scale, metadata, missing schema, unrelated error causes, Decimal ambiguity and capture identity. The new diagnostic regression fails against the prior comparator. The full replay checks every old native payload after removing only the newly added fields.

The first private probe link omitted the selected analyzer registration by using the default probe setup. The replay gate rejected it. `rejected-missing-analyzer/` preserves that attempt. The accepted link retains the exact `SparkDecimalNullPropagation` registration from the pinned selected probe; its only additions are schema capture and its test. Shared source files and executable slots were not modified.

`arithmetic-schema-errors-results.json` pins `arithmetic-schema-errors-runs.json.gz`. To verify without running either engine, extract its `files` map into a fresh directory and run its checker with the experiment directory as the argument. Use the existing PyArrow 25.0.1 environment:

```sh
python /path/to/extracted-audit/check-archive.py "$PWD/experiments/spark-sql"
```

The checker validates hashes, recomputes all field/error comparisons and the old numeric comparison, checks all 148 fresh references, preserves the 389-observation and 17-group records, and verifies the six IEEE display cases. It also verifies all 122 diagnostic reclassifications, the independent CAST controls and the selected source identities. `build.py` and `replay.py` record the private linking and capture commands; `capture-missing.py` records the two fresh Spark references. The archive includes their source snapshots and library identities. Query-runtime timing is not repeated for this capture/comparison change; accepted runtime measurements and open performance findings remain unchanged.
