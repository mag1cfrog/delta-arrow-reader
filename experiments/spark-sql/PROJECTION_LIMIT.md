# DISTINCT preparation through a projection under LIMIT 1

[Issue 268](https://github.com/mag1cfrog/delta-arrow-reader/issues/268)
addresses the eight rename/constant-projection observations retained by
the preceding LIMIT slice. Its baseline is PR 267 integration
`43cb6ba28c9c7399c19280eb047cca3ce428cbcc`. This is an optional Rust runtime
experiment; [issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
still owns default-build adoption.

## Behavior and implementation

With ANSI enabled, Spark returns true while the accepted runtime raises
CAST_INVALID_INPUT for this query:

```sql
SELECT EXISTS(SELECT DISTINCT w FROM (
  SELECT v AS w FROM (
    SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)
  ) t LIMIT 1
) u) AS r;
```

The [Rust patch](sail-projection-limit.patch) extends the existing shared
DISTINCT preparation to account for one nonidentity projection below a literal
LIMIT 1. It reuses the accepted identity-projection check and native projection
pruning. It also counts deterministic parent projections that Spark moves below
the limit before checking that pattern. Aliases and limits carry the count;
other operators and volatile projections stop that propagation. Each removed
DISTINCT prepares its descendants once.

This order matters. Replacing the outer EXISTS/DISTINCT with COUNT also succeeds
in Spark. Direct EXISTS over the renamed input still raises:

```sql
SELECT EXISTS(
  SELECT v AS w FROM (
    SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)
  ) t LIMIT 1
) AS r;
```

The implicit EXISTS output projection uses the available projection allowance.
Two nested nonidentity projections also retain their error; identity projections
can remain transparent. Local VALUES evaluation and live selected/filter/scalar
expressions keep their required errors.

Spark's [LimitPushDown](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)
and [PushProjectionThroughLimitAndOffset](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PushProjectionThroughLimitAndOffset.scala)
provide the source context. Captured Spark 4.2.0 observations define the reference;
the archive retains the inspected sources and projection/depth investigations.
Only `sail-plan/src/decimal_null.rs` changes in the selected 99-entry source set.
Package/features, 505 artifact records and 414 named libraries are retained;
only the `sail_plan` library hash changes. Execution nodes, scanners, numerical
kernels and dependencies stay the same.

## Validation and remaining work

The [865-query corpus](projection-limit.jsonl) preserves the previous 500 queries
and adds projection depth, renames, constants, computed/partial outputs,
ordinary/COUNT/scalar/IN/EXISTS contexts, live and empty range/VALUES, selected
and filtered outputs, explicit offsets, nonliteral limits, sorting/UNION,
nested/adjacent limits and volatile projections. Both ANSI modes run, with
batch 1/64 controls at the assigned boundary.

Strict value/type/error-cause agreement improves **1,574/1,730 -> 1,676/1,730**,
with no lost agreement. The eight assigned ANSI observations improve 0/8 -> 8/8;
including opposite modes, 8/16 -> 16/16. All 1,000 prior Spark observations
retain status, values, types and error condition in a fresh capture. There are
102 newly successful native observations and no changed paired main-matrix
error payloads. All added observations agree with Spark after the change.

The remaining 54 strict differences have existing owners:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 35 | Approved IN/NOT IN three-valued NULL policy, also checked against independent expected values | Existing policy |
| 13 | REMAINDER_BY_ZERO versus native Divide by zero diagnostics | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 6 | One correlated EXISTS, four group-only expressions and one GROUP BY/HAVING preparation observation | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |

All six remaining runtime observations have identical complete before/after
native payloads. No new exception is approved. Logical/physical nullable-field
differences change 172/76 -> 176/76 as new successes become comparable;
already-successful observations have no nullable changes. Full schema and
diagnostic acceptance remain open.

The preceding LIMIT matrix improves 938/1,000 -> 946/1,000. All 16 retained
CAST/subquery comparisons lose no agreement, and 12 numeric comparisons are
unchanged. Historical replay stays 6,603/6,784 across 37 groups with no changed
represented value/type/status outcomes. Of 911 retained errors, one reports
positive instead of negative Decimal(38,38) overflow as a different failing
input is reported first. Both full payloads remain in `shared-controls.json`.
Strict DIV stays 350/350; integer overflow stays 616/616 with all 131 complete
errors unchanged.

Rust suites pass 314 function, 56 planner and 28 runner tests. The one new
planner regression covers 34 queries with ANSI on/off/on in one session;
the same test fails against baseline libraries and passes against candidate
libraries. Real-Delta retains 116 outcomes and 18 adapters, with its Spark
comparison unchanged at 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

Sixteen queries use nonnullable and 10%-NULL fixtures. There are 28 equivalent
successful configurations and four candidate-only configurations whose baseline
raises; those four have no speedup ratio. Every successful row matches the same
Spark reference. Sixteen paired printed physical plans are equal and 12 change.

The balanced eight-process schedule uses 262,144 rows, batch 8,192, one partition,
four warmups and 21 samples per phase/process on CPU 2. Numbers below are
medians of four process medians. DataFusion plan-state resets run outside
execution timing. The accepted Rust benchmark harness is reused.

Target rename/constant/computed/partial/COUNT/local-input planning medians
decrease about 15%-25%. COUNT execution decreases 8.699%/9.191% for
nonnullable/nullable fixtures.

Local IN execution increases 0.2824945 -> 0.2883860 ms (+2.086%, +0.0058915 ms)
and 0.2856760 -> 0.2902640 ms (+1.606%, +0.0045880 ms). All four candidate
process medians exceed all four baseline medians for both fixtures, despite
equal printed physical plans. These add to the preceding slice's unresolved
IN cost; they do not show that it has been removed.

Other controls also increase. Ordinary nonnullable numeric execution changes
0.1144925 -> 0.1251875 ms (+9.341%, +0.0106950 ms). Native DataFusion numeric
execution changes 0.0904685 -> 0.0982120 ms (+8.559%, +0.0077435 ms) and
0.0866055 -> 0.0988185 ms (+14.102%, +0.0122130 ms); these bypass the Spark
analyzer. All three process ranges overlap. The largest planning increase is
native nullable numeric, 0.1760265 -> 0.1789775 ms (+1.676%, +0.0029510 ms).
Ordinary CAST execution changes +0.109%/+0.021%.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) owns
the measured costs and their attribution. The archive retains every raw sample,
process median, SQL statement, setting, plan and build/link identity. No
same-binary calibration or instruction/allocation/memory counters were collected.
These controls do not establish that the preparation rule causes a global
execution regression, and the observed increases are not accepted or dismissed.
Earlier costs and final performance acceptance remain open.

## Reproduce

`projection-limit-results.json` pins `projection-limit-runs.json.gz`. Extract
its `files` map into a scratch directory and run the archived `check-archive.py`
from the repository root. It checks source/patch identities, frozen comparisons,
native regressions, Delta outcomes and every raw timing median without rebuilding
Rust or starting Spark.

`prepare.py` reconstructs all selected sources directly from the preceding
complete, hashed archive. Build/reference/native-test and measurement commands
are retained. Rebuilding requires the recorded toolchains/dependencies and
adapted cache paths. Shared sources, executable slots and Delta fixtures are
restored after validation.
