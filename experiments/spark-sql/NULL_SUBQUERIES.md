# NULL-discarded scalar subqueries

[Issue 233](https://github.com/mag1cfrog/delta-arrow-reader/issues/233) owns the
preparation of scalar subqueries inside NULL-discarded expressions. The accepted
baseline is PR 253, integration `c7b79d06611f3db68ab1132e0a2e45197bbe7690`.

The [Sail patch](sail-null-subquery.patch) extends the existing Rust analyzer.
The [DataFusion patch](datafusion-null-subquery.patch) exposes one application
of its existing algebraic simplifier, without evaluating child constants.
Dependency versions and numeric kernels are unchanged. The patches remain
optional; [issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
owns default-build adoption.

## Behavior

Legacy division can wrap a NULL divisor in NULLIF or SparkNullIfZero. The
analyzer now recognizes these wrappers and NULL-propagating arithmetic before
DataFusion validates correlated execution. Required local subquery CAST errors
are still checked before the discarded arithmetic becomes a typed NULL.

ROUND and BROUND need a different rewrite. For example,
`ROUND(CAST((SELECT v FROM VALUES ('bad') t(v)) AS DECIMAL(10,3)), NULL)` returns
NULL, because the outer CAST is never evaluated. But a surviving two-row scalar
subquery must still fail. The analyzer keeps those subqueries as arguments to
a small native Rust expression that returns typed NULL. Existing DataFusion
scalar-subquery execution retains cardinality and runtime errors. This adds no
Python execution, table scanner or physical execution node.

One-row scalar expressions and empty local inputs retain their existing
preparation rules. CASE/COALESCE pruning uses DataFusion's existing algebraic
rules and only folds conditions for reachability. This avoids folding a dead
outer CAST or retaining an already discarded subquery. Planning evaluates only
the existing local VALUES paths; it does not execute range/table scans, joins
or aggregates.

Spark's reference behavior comes from
[RoundBase](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala)
and its
[subquery optimizer](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/subquery.scala).
The source snapshots, fresh phase traces and their hashes are archived.

## Results and boundaries

All 50 assigned observations pass, improving from 35/50. Including their
opposite-mode controls, agreement improves 76/100 -> 100/100. Fresh Spark
captures agree with every frozen assigned/control reference.

The [431-query matrix](null-subquery.jsonl) improves 505/862 -> 814/862,
with no lost agreement. It covers numeric types, NULL positions, ANSI modes,
local/range sources, literal/column CASTs, empty/sorted/LIMIT inputs, dead/live
CASEs, nested subqueries, correlated queries, ROUND/BROUND and scalar validation.
A [conditional supplement](null-subquery-conditional.jsonl) improves
21/36 -> 36/36, including dead CASE/COALESCE, bad outer CASTs and nested NULL
parents. Per-query batch sizes are 1, 2 or 64, with two native partitions.

The 48 remaining main-matrix observations are explicit boundaries:

- 24 reject floating DIV in both engines, with different error conditions.
  [Issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) owns diagnostics.
- 20 ROUND/BROUND cases retain live correlated subqueries. Spark reaches a
  multiple-row error; DataFusion rejects the correlated execution shape earlier.
  [Issue 140](https://github.com/mag1cfrog/delta-arrow-reader/issues/140) owns the
  live execution route. These are not counted as agreeing errors.
- Two empty bare scalar queries retain the nonnullable-field defect owned by
  [issue 234](https://github.com/mag1cfrog/delta-arrow-reader/issues/234).
- Two projected IN cases retain correct SQL NULL semantics instead of Spark's
  false result, following the accepted policy.

Logical/physical nullability differences on successful main-matrix cases stay
at 4/2. Spark phase traces record 132 optimization, 122 execution and 30
analysis failures. Native phase evidence, complete errors and plans remain
separate from value/type/error-cause agreement. The existing comparator is
unchanged; it never infers an error condition from the SQL text.

Historical replay improves 6,587/6,784
-> 6,599/6,784 across 37 groups, retaining
every prior agreement. Integer overflow remains 616/616. All prior numeric,
CAST and early-arithmetic passing observations are retained. The earlier
ROUND-arguments suite now passes 388/388. Historical errors retain
915 paired failures and record
3 changed texts, with complete payloads archived.

Rust tests pass: 314 function, 50 planner and 28 runner. The new SQL regression
checks NULL results/types, local CAST failures, column counts, live cardinality,
dead conditional parents and legacy correlated NULL operands. Real-Delta checks
retain all 116 outcomes and 18 adapters; frozen Spark results remain 47 matches,
58 differences and 11 pending adapters. These bounded suites are not exhaustive
Spark conformance.

Early prototypes incorrectly discarded ROUND cardinality checks or retained
dead conditional subqueries. Those failing captures and source snapshots remain
in the archive. The final candidate also removes unrelated formatting changes.

## Bounded performance check

Both benchmark binaries compile the same Rust source against private artifact
snapshots, with identical package versions/features. Each links 505 artifacts
and 414 named libraries. Exact changed source/library hashes are recorded.

The fixed process order is before/after/after/before/after/before/before/after.
CPU 2, 262,144 rows, batches of 8,192, one partition, four warmups and 21 samples
per phase/process. Sixteen queries run with nonnullable and 10%-NULL divisor
patterns. Twenty-six configurations have equal output; six fail in the baseline
because of a dead outer CAST or an untyped NULL BROUND scale. They have candidate
timings only, with no speed ratio. Every successful run checks every row against Spark's 1,000-row period
for exact type/value/NULL/digest agreement. No sample or completed run is dropped.

Times are medians of four process medians. Largest observed increases among
equal-output configurations:

- Planning, `bround_decimal_nullstrue`: 0.3422305 -> 0.4773615 ms, +39.4854% (+0.1351310 ms).
- Execution, `bround_decimal_nullsfalse`: 0.0583780 -> 0.0637685 ms, +9.2338% (+0.0053905 ms).

Physical plans can change because the repair retains required ROUND subquery
execution and removes discarded arithmetic subqueries. Decimal BROUND is the
largest planning increase: the baseline removed its local scalar subquery during
resolution, while the candidate preserves its required execution. Equal output
for these benign inputs does not make the baseline generally correct. The measurements do not
separate that required work from expression overhead. No same-binary calibration,
hardware counters, allocation/memory measurement or reserved host was used.
[Issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) retains the
full cost evidence and unresolved attribution. This is not a zero-regression claim.

| Configuration | Planning before / after ms | Change | Execution before / after ms | Change |
| --- | ---: | ---: | ---: | ---: |
| column_nullsfalse | 0.226481 / 0.229085 | +1.15% | 0.022828 / 0.022978 | +0.66% |
| div_nullsfalse | 0.276875 / 0.277941 | +0.39% | 0.387069 / 0.388521 | +0.38% |
| round_live_nullsfalse | 0.336901 / 0.337382 | +0.14% | 0.452365 / 0.452946 | +0.13% |
| native_addition_nullsfalse | 0.175281 / 0.174594 | -0.39% | 0.085844 / 0.085253 | -0.69% |
| native_divide_nullsfalse | 0.194085 / 0.194692 | +0.31% | 0.286331 / 0.288084 | +0.61% |
| null_divide_nullsfalse | 0.354073 / 0.353496 | -0.16% | 0.043802 / 0.043731 | -0.16% |
| legacy_null_left_nullsfalse | 0.400293 / 0.370904 | -7.34% | 0.043235 / 0.043610 | +0.87% |
| legacy_null_right_nullsfalse | 0.407131 / 0.371841 | -8.67% | 0.043636 / 0.043411 | -0.52% |
| round_decimal_nullsfalse | 0.523879 / 0.524695 | +0.16% | 0.090317 / 0.063443 | -29.76% |
| bround_decimal_nullsfalse | 0.367699 / 0.505670 | +37.52% | 0.058378 / 0.063769 | +9.23% |
| round_bigint_nullsfalse | 0.520076 / 0.516169 | -0.75% | 0.085659 / 0.049582 | -42.12% |
| bround_bigint_nullsfalse | error / 0.520036 | no ratio | error / 0.049286 | no ratio |
| round_double_nullsfalse | 0.520757 / 0.519314 | -0.28% | 0.069945 / 0.049536 | -29.18% |
| bround_double_nullsfalse | error / 0.497520 | no ratio | error / 0.049387 | no ratio |
| round_outer_bad_nullsfalse | error / 0.525331 | no ratio | error / 0.063338 | no ratio |
| round_two_subqueries_nullsfalse | 0.763082 / 0.745590 | -2.29% | 0.093419 / 0.051811 | -44.54% |
| column_nullstrue | 0.223881 / 0.225459 | +0.70% | 0.022793 / 0.023513 | +3.16% |
| div_nullstrue | 0.261776 / 0.259557 | -0.85% | 0.373459 / 0.373008 | -0.12% |
| round_live_nullstrue | 0.315461 / 0.317555 | +0.66% | 0.453823 / 0.456358 | +0.56% |
| native_addition_nullstrue | 0.176097 / 0.175482 | -0.35% | 0.086781 / 0.087358 | +0.66% |
| native_divide_nullstrue | 0.194837 / 0.195794 | +0.49% | 0.364442 / 0.363661 | -0.21% |
| null_divide_nullstrue | 0.336129 / 0.337788 | +0.49% | 0.043992 / 0.044448 | +1.04% |
| legacy_null_left_nullstrue | 0.380491 / 0.349519 | -8.14% | 0.044583 / 0.044483 | -0.23% |
| legacy_null_right_nullstrue | 0.382931 / 0.350722 | -8.41% | 0.044457 / 0.044909 | +1.01% |
| round_decimal_nullstrue | 0.502679 / 0.496583 | -1.21% | 0.100066 / 0.073006 | -27.04% |
| bround_decimal_nullstrue | 0.342230 / 0.477361 | +39.49% | 0.059120 / 0.064219 | +8.63% |
| round_bigint_nullstrue | 0.516139 / 0.512337 | -0.74% | 0.086625 / 0.049517 | -42.84% |
| bround_bigint_nullstrue | error / 0.519154 | no ratio | error / 0.049637 | no ratio |
| round_double_nullstrue | 0.524064 / 0.518358 | -1.09% | 0.070250 / 0.049637 | -29.34% |
| bround_double_nullstrue | error / 0.497960 | no ratio | error / 0.049392 | no ratio |
| round_outer_bad_nullstrue | error / 0.527665 | no ratio | error / 0.063859 | no ratio |
| round_two_subqueries_nullstrue | 0.763703 / 0.745715 | -2.36% | 0.093479 / 0.051811 | -44.57% |

The archive contains all eight raw timing files, every process median, exact
SQL/input construction/settings, Spark rows, plans and source/binary/link hashes.
Candidate probe SHA-256: `cc630d84e90f5307f8d8a11eae15d703756e0eff4378272b8186ff6e63197b1f`.
Candidate runner SHA-256: `853c46ddd7aa01411b46366fd69ba8dfec1976dc12263855e0ec136e6b2fcb46`.

## Reproduction

The result map and archive are `null-subquery-results.json` and
`null-subquery-runs.json.gz`. Reuse the comparator with the named corpus:

```python
from pathlib import Path
from early_arithmetic import compare
result = compare(Path("spark-phases.json"), Path("matrix-after.json"), Path("cases.jsonl"))
```

The archive is a gzip-compressed JSON object with `files` and per-file `sha256`
maps. `check-archive.py` verifies source/patch identity in both directions,
frozen references and comparisons, retained suites, raw timing medians and
test/restoration records without rebuilding or running Spark. Set its `root`
to this checkout's `experiments/spark-sql` directory before running it.

`prepare.py` reconstructs the accepted 95-entry source set from hashed prior
archives. `build.py` installs the candidate, records commands/hashes and restores
all shared sources and executable slots on exit. `check.py`, `classify.py`,
`replay.py`, `validate.py`, `retained-casts.py` and `capture-delta.py` capture the
scoped and retained results. `link-bench.py` and `measure.py` link private
artifacts and run the fixed protocol. The archived paths describe the research
environment; clean-checkout/default CI integration remains with issue 165.
