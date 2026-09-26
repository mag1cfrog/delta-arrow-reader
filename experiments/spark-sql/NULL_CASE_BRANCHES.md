# Discard NULL CASE branches

[Issue 258](https://github.com/mag1cfrog/delta-arrow-reader/issues/258) owns
constant-NULL CASE branches that still execute discarded scalar subqueries.
The accepted baseline is PR 257, integration
`ae88a790942bbd6a9e8a3089de356f9df1e79b9b`.
The [optional Rust patch](datafusion-null-case.patch) changes the existing
DataFusion CASE simplifier and adds its regression test. Default-build
adoption remains with [issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).

## Behavior

```sql
SELECT CASE WHEN CAST(NULL AS BOOLEAN)
       THEN (SELECT CAST(7 AS INT) DIV CAST(0 AS INT))
       ELSE CAST(7 AS BIGINT) END AS r;
```

Spark returns 7. The baseline executes the dead subquery and raises division
by zero. The searched-CASE algebraic rule already removes FALSE conditions;
the patch makes it recognize and skip literal NULL conditions too, using the
existing helpers. Live branch order, nullable-column conditions and final
result-type derivation remain intact. Without ELSE, removing every branch
produces a NULL of the existing result type.

The shared rule is used by ordinary optimization and Spark's existing
preparation/ROUND paths. Required early arithmetic errors in range/VALUES
subqueries remain checked before outer pruning. The patch changes no arithmetic
kernel, subquery executor, dependency or analyzer traversal.

## Validation

All eight assigned ANSI results improve 0/8 -> 8/8. With opposite-mode
controls, agreement improves 8/16 -> 16/16. Fresh Spark results agree with
all 16 frozen references from the preceding arithmetic investigation.

The [209-query matrix](null-case.jsonl), run in both ANSI modes, improves
306/418 -> 376/418. It covers typed/untyped/folded NULL, CASE/IF, branch
positions, nesting, absent ELSE, six result types, nullable columns, arithmetic
and CAST failures, scalar cardinality, local VALUES/range and LIMIT controls.
All 48 missing-ELSE/NULL-ELSE observations return the correct typed NULL with
both nullable fields true. No previously passing observation is lost.

The remaining 42 comparator differences stay visible:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 16 | Spark rejects an untyped NULL CASE condition during analysis; the native frontend accepts it | [150](https://github.com/mag1cfrog/delta-arrow-reader/issues/150) |
| 24 | Scalar CAST preparation raises errors that Spark suppresses for dead one-row branches, empty local VALUES or range LIMIT 0 | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 2 | Approved projected-IN NULL policy | Accepted policy |

The 24 CAST payloads are identical before/after. All 16 untyped-condition
observations were already mismatches; eight now return a value instead of an
unrelated arithmetic error, while Spark still rejects their condition type.
Those changes are not counted as new Spark agreements. The comparator retains
exact numeric values/types and meaningful error causes; only existing Arrow
UTF8 spelling normalization is used for string results.

Successful observations have 20 logical / 140 physical nullable-field
differences before and 30 / 194 after. No nullable field changes for any
observation that already succeeded. The increased counts occur among 78 newly
successful native observations, including the eight untyped-condition
mismatches above. Conditional schema acceptance remains open with the review
owner; successful values alone do not establish full schema compatibility.

All prior dedicated numeric/CAST/subquery passes remain. The preceding scalar
arithmetic matrix improves 525/586 -> 533/586. The full error-aware DIV group
remains 350/350; historical replay remains 6,603/6,784 across 37 groups with
no changed outcome. The 911 retained paired historical errors include two
changed choices among failing Decimal inputs, preserved in full. Integer
overflow remains 616/616 with all 131 error payloads identical.

Rust suites pass: 314 function, 51 planner and 28 runner. The new embedded
simplifier regression passes through a native Rust test harness linked to the
private candidate libraries and fails against the accepted baseline. This
runs the exact embedded test without requiring dependency dev-tests to be
workspace members. Its type, NULL, live-column and branch-order checks and
linkage are archived. Real-Delta retains all 116 outcomes and 18 adapters.
These are bounded results, not full Spark conformance.

## Performance

One fixed comparison covers 12 queries and two input patterns. Both binaries
use the same Rust benchmark source and pinned package versions/features,
with 505 private artifacts / 414 named libraries each. Eight transitive
libraries change after rebuilding the single modified optimizer source.
All rows are checked against Spark. There are 22 equivalent-output pairs and
two candidate-only configurations where the baseline fails; no timing ratio
compares an error with a successful result.

Each process uses 262,144 rows, batch size 8,192, one partition, four warmups
and 21 samples per phase, pinned to CPU 2. Inputs are nonnullable or have 10%
NULLs. Eight processes use the fixed order before, after, after, before,
after, before, before, after. Ordinary projection and dynamic CASE controls
are included alongside dead scalar branches and native SQL controls.

For `case_null_nullsfalse`, planning changes 0.5104030 -> 0.4026530 ms
(-21.111%) and execution 0.0300910 -> 0.0230225 ms (-23.490%). For the
mixed dynamic/NULL CASE, execution changes 1.5165225 -> 0.9514275 ms
(-37.263%). The largest measured planning increase is an unchanged native
projection: 0.1388280 -> 0.1405865 ms (+1.267%, +0.0017585 ms). All paired
execution medians are lower in this run. Removing dead expressions changes
14 successful physical plans; eight successful controls retain their plans.

The run has no same-binary calibration or instruction/allocation/memory
counters. These observations do not establish final performance acceptance.
[Issue 157](https://github.com/mag1cfrog/delta-arrow-reader/issues/157) owns
this shared planning-rule cost record; execution timings provide context,
while operator-specific acceptance stays with its existing owners. Raw SQL,
plans, every sample/process median and all build/link hashes are archived.

## Reproduce

`null-case-results.json` identifies the hashed `null-case-runs.json.gz`
archive. Extract its `files` map to a scratch directory and run
`python check-archive.py` from this checkout. It verifies each member hash,
source identity, patch application/reversal, frozen comparisons, embedded
test identity and every timing median. Spark and a rebuild are not needed
for this offline verification.

`prepare.py` reconstructs and verifies the 99-entry accepted source manifest.
`build.py` installs the single candidate source, performs locked/offline
release builds and tests, then restores shared source/binary slots.
`run-native-test.py` extracts the embedded regression and links it to the
private before/after artifacts. The capture/comparison scripts, frozen Spark
phases, complete errors and Delta checks are retained alongside
`link-bench.py`, `measure.py` and raw timings. Python supplies test references
and orchestration; production execution and the regression remain Rust.
Rebuilding needs the recorded toolchains/dependencies and adapted cache paths.
