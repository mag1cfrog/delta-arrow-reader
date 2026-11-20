# Spark planning attribution (#1390, PR1)

Statement parsing dominates the 1,000-literal IN and 200-branch CASE shapes.
The 400-column query's largest individual measured non-parser rule is
`OptimizeProjections::rewrite`. No optimization was attempted.

The single accepted schedule used product baseline `9fa5b4cd98fecc7a2e95debc67128c394b25fb9a` and committed
[protocol](planning-1390-protocol.md) `d2dffd3793b20dc1dc5803c64b90f9a4418335be`. It completed 70 processes,
2,100 query-process records and 18,900 measured iterations, with two warmups
and nine samples per query/process. All 25 paired and five stress queries have
results in all four configurations. The 25 paired total medians were 1.71 to
3.31 times native-configured, with median ratios 2.038 (ANSI off) and 2.041
(ANSI on). Their remaining gaps were 0.178 to 0.682 ms per query.

## Largest measured costs

All values below are milliseconds. Spark values are observed stage medians.
The native control is the same observed rule body for the wide query, and the
whole canonical native-configured logical phase for the two parsing shapes.

| Shape and stage | Spark ANSI off | Spark ANSI on | Native control |
| --- | ---: | ---: | ---: |
| 1,000-literal IN, `parser.parse` | 9.222210 | 9.229173 | 0.750704, whole logical phase |
| 200-branch CASE, `parser.parse` | 6.701788 | 6.701969 | 1.252576, whole logical phase |
| 400 columns, `OptimizeProjections::rewrite`, sum of two passes | 4.848326 | 4.794165 | 0.512571, same rule body |

The full statement parser's construction costs 0.0206 to 0.0222 ms on these
stress shapes. IN lexing costs about 0.802 ms and CASE lexing about 0.731 ms.
Construction is much smaller than parsing here. The measured source is Sail's
`parse!` macro in `vendor/sail/crates/sail-sql-analyzer/src/parser.rs`, using
`create_parser` and rich Chumsky error types. The untouched #1379 leaf profiles
showed `InputRef::add_alt_err` and `RichPattern::eq` on the valid IN/CASE SQL.
That historical profile and the source identify relevant rich-error paths;
this run did not split `parser.parse` into error bookkeeping and other work,
or measure how much a change to those paths could remove.

For the 400-column query, `OptimizeProjections::rewrite` calls
`optimize_projections` in
`vendor/dependencies/datafusion-optimizer/src/optimize_projections/mod.rs`.
Both passes are included, with invariant/schema checks timed separately.
Other ANSI-off physical buckets include `TypeCoercion::analyze` at 2.292879 ms
(native-configured 0.750855), `align_result_metadata_for_execution` at
1.166517 ms, and final `rename_physical_plan` at 0.309465 ms. The
`DefaultPhysicalPlanner` bucket is 1.680391 ms and includes physical optimizer
passes and validation. These intervals do not establish which internal name
or schema comparison causes their cost.

The observed analyzer label `preserve_spark_expression_errors` is
`PrepareSparkExpressions::analyze` in
`vendor/sail/crates/sail-plan/src/expression_errors.rs`. Its wide-query body
is 0.887188 ms with ANSI off. The automatic raw metadata mapping omitted this
label because its source has an inline comment before the returned literal.
The [source map](planning-1390-source-map.json) records the exact symbol and
source hash without rewriting the raw measurements or metadata.

The canonical wide-query physical phase is 15.989831 ms with ANSI off and
15.913059 ms with ANSI on, versus 2.495054 ms native-configured. The remaining
canonical physical gaps are 13.494777 and 13.418005 ms. Canonical IN logical
gaps are 9.898487 and 9.895211 ms; CASE logical gaps are 7.944618 and
7.963573 ms. These are baseline differences, not claimed savings. Whole
phases and parent intervals overlap their component rows.

## Noise and observation limits

Five fresh-process A/B pairs use the same observed binary for each of the
four configurations. Stage-specific absolute differences were 0.015439 and
0.050835 ms for IN `parser.parse`, 0.001053 and 0.006412 ms for CASE
`parser.parse`, and 0.030536 and 0.008897 ms for the wide projection rule
(ANSI off/on). These costs exceed their observed A/B differences. Small
components near their own A/B difference remain unresolved.

Canonical native-configured A/B phase differences had medians 0.000501 ms
logical and 0.001253 ms physical; maxima were 0.015528 and 0.033163 ms.
Canonical and observed walls remain separate. Across all 30 shapes, the
observed-minus-canonical median physical-wall differences were +0.026574 ms
native-plain, +0.029280 ms native-configured, +0.016561 ms Spark ANSI off,
and +0.018810 ms Spark ANSI on. Individual wall differences can be negative
because these are different process samples. No guessed observation overhead
is subtracted from a component. Use each row's process range/IQR and its own
A/B evidence rather than treating one number as a universal error bar.

## Correctness boundary and receipts

Eight planning-only trace processes produced 1,356 records. All four
canonical/observed configuration pairs are byte-identical: resolved logical
plans, output names, detailed physical plans, per-node properties and exact
Arrow schemas including nested fields, storage representation, nullability
and metadata. This covers 120 paired/stress and 558 ordinary Spark requests
per build. All 78 benchmark/trace process stderr files are empty. Planning
started no Delta data scan and received no data bytes. The frozen #1379
execution preflight supplies historical result equivalence; no query was
executed here, and opaque physical-node internals have no general equality
check.

| Successful build | Sealed executable SHA-256 | Assembled source tree SHA-256 |
| --- | --- | --- |
| Canonical | `289430f296e0cebb66f68cf7f1b14176431a2d914ebbed1f1e6d8b20bd62b2fa` | `8457ca57126e6ac6a6d3308246c79740e84ea688e8126475831864a95f41147b` |
| Observed | `930821b5f76c3c96b3cefb8528cbca6b775a3ef4778fc9592c3910eddf6a2d10` | `f56c0e2fcda85396ecc4d4cc7ac06aa8866f1940b67268a062559f0a144b4eb8` |

The exact [canonical receipt](planning-1390-canonical-receipt.json.gz) and
[observed receipt](planning-1390-observed-receipt.json.gz) preserve compiler,
locked release command, flags, source assembly hashes and local log hashes.
Before timing, an initial lint failure and a wrong-mode Cargo artifact reuse
in an observed build were corrected. The receipts above identify the two
distinct binaries that passed the plan trace and the one accepted timing
schedule under the final protocol. Full successful and failed build logs,
executables and caches remain in local evidence, outside this bundle.

[Wall times](planning-1390-walls.csv), [stage medians](planning-1390-stages.csv),
[A/B noise](planning-1390-noise.csv), and [parts larger than their whole native
phase](planning-1390-large-parts.csv) retain every measured case. Per-rule
rows retain counts and pass indices; raw spans retain each application.
The [artifact manifest](planning-1390-manifest.json) links and hashes the raw
samples, full plans, original metadata, proofs and build receipts. Parent
intervals include their children; residuals include framework, teardown and
observation work. Run `python3 planning-1390-verify.py` in this bundle to recheck hashes and
raw coverage without building or executing SQL.

Local Linux formatting, root crate Clippy and the full Spark workspace passed.
All 234 test groups and 3,185 named outcomes matched the preceding checkpoint:
3,182 passed and three were ignored. The [validation receipt](planning-1390-validation.json)
records the commands and log hashes.
