# DISTINCT preparation under literal LIMIT 1

[Issue 266](https://github.com/mag1cfrog/delta-arrow-reader/issues/266) fixes
the inner LIMIT 1 failure retained by the first-DISTINCT slice. The baseline
is PR 265 integration `c9804516f1ecb9ea8ad581b8baf9257f0cdeb8fc`.
This remains an optional Rust experiment;
[issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns
default-build adoption.

## Behavior and implementation

With ANSI enabled, Spark returns true while the accepted runtime raises
CAST_INVALID_INPUT for this query:

```sql
SELECT EXISTS(
  SELECT DISTINCT v FROM (
    SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1
  ) t
) AS r;
```

The [Rust patch](sail-distinct-limit.patch) reuses the first-DISTINCT traversal
for explicit literal LIMIT 1. CAST preparation and arithmetic prechecks now
prepare these limits before native projection pruning. This also repairs
ordinary projections, COUNT, scalar subqueries and IN over the same input.
Local VALUES errors and expressions whose values remain necessary still run.

Implicit EXISTS preparation and explicit limits share a traversal. Adjacent
limits do not repeatedly strip nested DISTINCT layers; a separate limit below
the removed DISTINCT can prepare its own input. Arithmetic checks distinguish
already-prepared predicate plans from original plans. A prototype with two
independent preparation passes swallowed a required error and was rejected.

DataFusion first widens an integer LIMIT literal with a CAST to Int64. The
rule recognizes that automatic wrapper while preserving the different
preparation order of `LIMIT 2-1`, an explicit CAST and explicit OFFSET.
Identity projections and aliases retain their existing traversal behavior.
Sorting and nonidentity projections remain boundaries.

Spark's [LimitPushDown rule](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)
provides source context; captured Spark 4.2.0 results define the reference.
The archive also retains the inspected Spark optimizer and DataFusion limit
coercion/pushdown sources. Only `sail-plan/src/decimal_null.rs` changes in the
selected 99-entry source set. Package/features, 505 artifact records and 414
named libraries are retained; only the `sail_plan` library hash changes.
Execution nodes, scanners and numerical kernels stay the same.

## Validation and remaining work

The [500-query corpus](distinct-limit.jsonl) includes the prior 363 queries
and both ANSI modes. It covers CAST/division, empty and live range/VALUES,
LIMIT 0/1/2, explicit offsets, duplicate/NULL rows, selected and discarded
outputs, identity/nonidentity projections, scalar/IN/EXISTS/COUNT contexts,
nested and adjacent limits, dead parents, and batches 1/2/64 at the assigned
boundary.

Strict value/type/error-cause agreement improves **891/1,000 -> 938/1,000**,
with no lost agreement. The assigned ANSI observation improves 0/1 -> 1/1;
including the opposite-mode control, 1/2 -> 2/2. All 726 prior Spark reference
observations retain their status, values, types and error condition in the
fresh capture. There are 47 newly successful native observations and no changed
paired main-matrix error payloads.

The remaining 62 strict differences retain explicit dispositions:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 35 | Approved IN/NOT IN three-valued NULL policy | Existing policy |
| 13 | Spark REMAINDER_BY_ZERO versus native Divide by zero diagnostics | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 14 | Six prior correlated/grouping observations and eight newly exposed nonidentity-projection preparation observations | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |

The eight new observations insert a rename or constant projection between
an inner LIMIT 1 and DISTINCT, with INT/DOUBLE/DECIMAL CAST or division.
Spark returns true; both native builds retain the same error. These require
further preparation-order work and are not claimed fixed or waived here.
All 14 runtime observations have identical complete before/after payloads.
`classification.json` retains every SQL statement and full capture. Approved
NULL-policy results have independent expected-value checks and still count
as strict Spark differences.

Logical/physical nullable-field differences change 164/37 -> 168/41 as new
successes become comparable. Already-successful observations have no nullable
changes. Value agreement does not establish full schema or diagnostic agreement.

The prior DISTINCT matrix improves 671/726 -> 672/726; the predicate matrix
stays 371/416. Other retained arithmetic/CAST/scalar comparisons lose no
agreement, and the 12 numeric comparisons are unchanged. Historical replay
stays 6,603/6,784 across 37 groups with no changed represented value/type/status
outcomes. It retains 911 errors; one message changes from positive to negative
Decimal(38,38) overflow as a different failing input is reported first. Both
complete payloads remain in the diagnostic evidence. Strict DIV stays 350/350;
integer overflow stays 616/616 with all 131 complete errors unchanged.

Rust suites pass 314 function, 55 planner and 28 runner tests. The single
added planner regression covers 26 queries with ANSI on/off/on in one session.
The same test fails on baseline libraries and passes on candidate libraries.
Real-Delta retains all 116 outcomes and 18 adapters; its Spark comparison stays
47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Sixteen queries use nonnullable and 10%-NULL fixtures. There are 26 equivalent
successful configurations and six candidate-only configurations whose baseline
raises. Every successful output row matches the same Spark reference. Eighteen
paired physical plans are unchanged and eight change.

The balanced eight-process schedule uses 262,144 rows, batch 8,192, one
partition, four warmups and 21 samples per phase/process on CPU 2. Reported
numbers are medians of four process medians. DataFusion plan-state resets run
outside repeated execution timing.

Target nested LIMIT/DISTINCT, COUNT, local CAST and division planning medians
decrease about 16%-26%. The largest planning increase is the ordinary nullable
CAST fixture: 0.2770740 -> 0.2830105 ms (+2.143%, +0.0059365 ms), with overlapping
process ranges.

Local VALUES CAST inside IN has a measured execution increase despite an
unchanged printed physical plan: 0.2559605 -> 0.2815630 ms (+10.003%,
+0.0256025 ms) on nonnullable input and 0.2588305 -> 0.2809865 ms (+8.560%,
+0.0221560 ms) on the nullable fixture. All four candidate process medians
exceed all four baseline medians for both fixtures. The nonnullable baseline
medians are 0.2561050, 0.2544530, 0.2568270 and 0.2558160 ms; candidate medians
are 0.2819140, 0.2812120, 0.2801500 and 0.2819740 ms. The nullable baseline
medians are 0.2583700, 0.2596520, 0.2592910 and 0.2568570 ms; candidate medians
are 0.2804610, 0.2815120, 0.2803900 and 0.2831960 ms.

Ordinary CAST execution also increases 2.659%/3.557% for the nonnullable/nullable
fixtures, while the native SQL CAST controls decrease 0.088%/1.016%. The full
tables and raw captures retain these controls, every sample, process median,
SQL statement, plan and build/link identity.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) owns
these measured costs and their attribution. No same-binary calibration or
instruction/allocation/memory counters were collected. The unchanged printed
plans do not explain the execution increases. Earlier costs and final
performance acceptance remain open; this compatibility slice does not accept
or resolve them.

## Reproduce

`distinct-limit-results.json` pins `distinct-limit-runs.json.gz`. Extract the
archive's `files` map into a scratch directory and run its `check-archive.py`
from the repository root. It verifies source/patch identities, frozen
comparisons, retained regressions, Delta outcomes and every raw timing median
without rebuilding Rust or starting Spark.

The archive retains reconstruction/build scripts, the exact native regression,
reference captures, rejected prototypes and benchmark commands. Rebuilding
needs the recorded toolchains/dependencies and adapted cache paths. Shared
sources/executable slots and Delta fixtures are restored after validation.
