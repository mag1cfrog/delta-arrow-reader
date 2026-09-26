# DISTINCT output preparation under EXISTS

[Issue 264](https://github.com/mag1cfrog/delta-arrow-reader/issues/264) fixes
the unused DISTINCT output reported by the predicate-preparation slice.
The baseline is PR 263 integration
`2996d15f8fe5e0d346ebcd21a74e9f90c55babb1`. This remains an optional Rust
experiment; [issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
owns adoption in the default build.

## Behavior and implementation

With ANSI enabled, Spark returns true for this query. The accepted native
runtime instead raises CAST_INVALID_INPUT during expression simplification:

```sql
SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) FROM range(3)) AS r;
```

The [Rust patch](sail-exists-distinct.patch) removes the first DISTINCT along
the early uncorrelated EXISTS preparation path. It can cross a full identity
projection, a subquery alias or LIMIT 1 without an explicit OFFSET. Native
projection pruning then drops the unused output. Local VALUES arithmetic/CAST
checks and remaining scalar-subquery checks still run before that pruning.

This boundary matters for errors as well as row counts. Renamed, computed,
constant and partial projections, sorting, UNION branches, other limits and
explicit offsets retain their preparation boundary. A second nested DISTINCT
also remains. A top-level UNION DISTINCT can lose its outer DISTINCT, while
DISTINCT within a UNION ALL branch remains. Required filter/local/scalar errors
and duplicate-sensitive OFFSET results are checked explicitly.

Spark's [LimitPushDown rule](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)
provides source context: its early limit-one rewrite removes group-only
aggregation before later expression folding. The captured Spark 4.2.0 behavior
defines this experiment's reference. A broader recursive DISTINCT removal
swallowed required errors and was rejected; both rejected prototypes and their
captures remain in the archive.

Only `sail-plan/src/decimal_null.rs` changes in the selected 99-entry source
set. Both builds retain the same packages/features, 505 artifact records and
414 named libraries; only the `sail_plan` library hash changes. No numerical
kernel, scanner or execution node changes.

## Validation and remaining work

The [363-query corpus](exists-distinct.jsonl) includes the previous 208 queries
and both ANSI modes. It covers INT/DOUBLE/DECIMAL casts, division, local/range
inputs, empty inputs, negation/conditional/filter placement, identity and
nonidentity projections, nesting, LIMIT/OFFSET, duplicate/NULL rows, grouping,
set operations and correlated controls.

Strict value/type/error-cause agreement improves **616/726 -> 671/726**, with
no lost agreement. The assigned ANSI observation improves 0/1 -> 1/1; including
its opposite-mode control, 1/2 -> 2/2. All 416 prior Spark reference observations
retain their recorded status, values, types and error condition in the fresh
capture. There are 55 newly successful native observations and no changed
paired error payloads.

The 55 remaining strict differences stay visible:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 35 | Approved IN/NOT IN three-valued NULL policy | Existing policy |
| 13 | Spark REMAINDER_BY_ZERO versus native Divide by zero diagnostics | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 7 | Correlated EXISTS, group-only/HAVING preparation and nested DISTINCT with an inner LIMIT 1 | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |

All seven runtime findings have identical complete before/after native
payloads. They remain unresolved, not accepted exceptions. `classification.json`
preserves every SQL statement and full capture. Approved NULL-policy results
have independent expected-value checks and still count as strict Spark
differences. Logical/physical nullable-field differences change from 142/23
to 151/24 across successful comparisons; already-successful observations have
no nullable-field changes. Value agreement does not establish full schema
or diagnostic agreement.

The prior predicate matrix improves 370/416 -> 371/416. Other retained
arithmetic/CAST/scalar comparisons lose no agreement; the 12 numeric comparisons
are unchanged. Historical replay stays 6,603/6,784 across 37 groups with no
changed represented value/type/status outcomes or changes to its 911 errors.
Strict DIV remains 350/350. Integer overflow remains 616/616 with all 131
complete errors unchanged.

Rust suites pass 314 function, 54 planner and 28 runner tests. The single added
planner regression covers 22 queries with ANSI on/off/on in one session; the
same test fails on baseline libraries and passes on candidate libraries.
Real-Delta retains all 116 outcomes and 18 adapter checks. Its Spark comparison
remains 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Sixteen queries use nonnullable and 10%-NULL fixtures. There are 28 equivalent
successful configurations and four candidate-only configurations whose baseline
raises. Every successful output row matches the same Spark reference. Eighteen
paired physical plans are unchanged and ten change.

The balanced eight-process schedule uses 262,144 rows, batch 8,192, one
partition, four warmups and 21 samples per phase/process on CPU 2. Reported
numbers are medians of four process medians. The accepted harness resets
DataFusion plan states outside each execution timer.

Direct DISTINCT numeric/CAST, identity projection and LIMIT 1 planning medians
fall about 21%-27%; nested DISTINCT falls about 10%. All 28 paired planning
point estimates decrease in this run. These observations do not prove the
absence of planning overhead across other workloads.

The largest execution increase is an unchanged ordinary nullable numeric
plan: 0.1766580 -> 0.1828595 ms (+3.510%, +0.0062015 ms). Its four baseline
process medians are 0.1764280, 0.1765980, 0.1775100 and 0.1767180 ms; candidate
medians are 0.1764370, 0.1909250, 0.1892820 and 0.1755160 ms. The ranges overlap;
these measurements do not attribute that variation to this analyzer change.
Local VALUES CAST inside EXISTS changes +0.269%/+0.933% in execution. All raw
samples, process medians, SQL, settings, plans and build/link hashes are retained.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) owns
the cost disposition. This comparison has no same-binary calibration or
instruction/allocation/memory counters. Earlier costs and final performance
acceptance remain open; the observed reductions do not close them.

## Reproduce

`exists-distinct-results.json` pins `exists-distinct-runs.json.gz`. Extract
the archive's `files` map into a scratch directory and run its `check-archive.py`
from the repository root. It checks source/patch identities, frozen comparisons,
retained regressions, Delta outcomes and every raw timing median without
rebuilding Rust or starting Spark.

The archive retains reconstruction/build scripts, the exact native regression,
reference captures and benchmark commands. Rebuilding needs the recorded
toolchains/dependencies and adapted cache paths. Shared sources/executable
slots and Delta fixture files are restored after validation.
