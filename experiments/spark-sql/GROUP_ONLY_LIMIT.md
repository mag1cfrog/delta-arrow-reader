# Group-only LIMIT preparation

[Issue 272](https://github.com/mag1cfrog/delta-arrow-reader/issues/272) owns
ordinary group-only LIMIT/EXISTS preparation in the selected optional Rust
runtime. Its baseline is PR 271 integration
`fcb7bc107cd0cd6061c3c728c72ba68ba074b65a`.
[Issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) owns
default-build adoption.

## Behavior and implementation

With ANSI enabled, Spark returns true for this query. The accepted runtime
raises Divide by zero while evaluating a grouping key whose value is unused:

```sql
SELECT EXISTS(
  SELECT id DIV 0 FROM range(3) GROUP BY id DIV 0
) AS r;
```

The [Rust patch](sail-group-only-limit.patch) prepares group-only plans after
the existing projection pruning. If the consumer needs at most one row and
does not read the grouping outputs, an aggregate with no remaining aggregate
functions becomes a native projection over LIMIT 1. Another projection-pruning
pass removes unused key expressions. This preserves zero rows for empty input
and avoids evaluating keys whose groups are no longer needed.

The three existing pruning callers share this preparation, including the
arithmetic-only path used by a Float64 provider without a CAST. The analyzer's
existing borrowed scan records whether ordinary groups are present. Queries
without groups skip the added traversal.

Live output values, real aggregate and HAVING work, OFFSET cardinality, grouping
sets and early local VALUES errors remain protected. Row-presence requirements
cross harmless projections, aliases and eligible limits. They stop at UNION
and actual filtering or ordering boundaries. Nested discarded groups are
prepared recursively. Ordering below a discarded group is removed only while
limits, windows and other boundaries remain intact.

Spark's
[LimitPushDown](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)
and
[groupOnly predicate](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala)
provide the source context. The archive retains the inspected sources, while
fresh Spark 4.2.0 captures define the reference. Execution remains Rust using
native DataFusion nodes. Only `decimal-null.rs` changes in the selected
99-entry source set. Both builds retain the same package/features, 505 artifact
records and 414 named libraries; only `sail_plan` changes.

## Validation and remaining work

The [1,624-query corpus](group-only-limit.jsonl) preserves all 1,227 preceding
queries and adds 397. It covers both ANSI modes, empty/live range and VALUES,
computed outputs, EXISTS/COUNT/scalar/IN contexts, LIMIT 0/1/2 and expressions,
OFFSET, nested groups, HAVING, aggregate arguments, ordering, UNION, volatile
keys and grouping sets. Batch 1/64 controls accompany the default batch 2
checks. A separate two-query fixture uses a Spark DataFrame and a native
Float64 RecordBatch to verify the arithmetic-only provider path.

Strict agreement improves **3,015/3,248 -> 3,155/3,248**, with no lost agreement
and 140 newly successful observations. The assigned ANSI observation is fixed;
including the opposite mode, 1/2 -> 2/2. The preceding corpus improves
2,397/2,454 -> 2,398/2,454. All 2,454 retained Spark observations keep their
status, rows, types and error condition in a fresh capture.

During investigation, an initial candidate lost seven agreements against the
original new controls. It was rejected. Restricting preparation to unused
outputs preserved required errors. Some value-reading controls also selected
an unspecified group with LIMIT 1. Three repetitions of the identical baseline
binary demonstrated changing row choices. The final corpus adds explicit
ordering to 49 new controls and captures their Spark references again. No
previous query changes. Original queries, both rejected candidates and their
captures remain archived; the comparator is not relaxed.

The 93 remaining strict differences retain explicit owners. Every remaining
native payload, including its available plans and error text, is identical
before and after this repair:

| Observations | Finding | Owner |
| ---: | --- | --- |
| 43 | Approved IN/NOT IN three-valued NULL policy, independently checked against expected values | Existing policy |
| 13 | REMAINDER_BY_ZERO versus native Divide by zero diagnostics | [149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) |
| 25 | Correlated EXISTS, IN decorrelation field lookup and UNION early-error ordering | [141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) |
| 12 | Grouping sets/ROLLUP, HAVING error pruning and volatile grouped-output analysis | [142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) |

Four new IN observations use the already approved NULL policy. Other remaining
findings are unresolved, with complete SQL, rows, errors and plans in
`classification.json`. Empty grouping-set output still needs the recorded
independent grand-total semantic review. No new exception is approved.

Logical nullable differences remain 192. Physical nullable differences change
from 100 to 104 as additional queries succeed. Already-successful results have
no nullable changes; paired main errors have no payload changes. Complete
schema and diagnostic acceptance remains open.

All 16 retained CAST/subquery comparisons lose no agreement. Twelve numeric
comparisons are unchanged. Historical replay stays 6,603/6,784 across 37 groups,
with no changed represented value/type/status outcomes. Of 911 historical
errors, one Decimal(38,38) case reports positive instead of negative overflow
from competing failing inputs in its two-partition plan. Both full payloads
remain archived; deterministic failure ordering is not claimed. Strict DIV
stays 350/350 and integer overflow 616/616, with all 131 integer errors unchanged.

Rust suites pass 314 function, 58 planner and 28 runner tests. One added
43-query regression runs ANSI on/off/on in one session; the exact embedded
test fails against baseline private libraries and passes against candidate
libraries. Real-Delta retains 116 outcomes and 18 adapters. Its Spark comparison
remains 47 matches, 58 differences and 11 pending adapters.

## Bounded performance comparison

The unchanged Rust harness measures 16 queries with nonnullable and 10%-NULL
fixtures: 28 equivalent successful configurations and four candidate-only
configurations whose baseline raises. Every successful output row matches
Spark. Printed physical plans are equal for 20 pairs and change for eight.
Candidate-only timings have no speedup ratio.

The balanced eight-process schedule uses 262,144 rows, batch 8,192, one
partition, four warmups and 21 samples per phase/process on CPU 2. Results are
medians of four process medians. DataFusion plan-state resets stay outside
execution timing.

For nonnullable/nullable fixtures, pure group-only planning decreases
14.331%/15.745%. Planning for grouped COUNT with LIMIT decreases
12.550%/13.479%, and execution decreases 24.304%/22.827%. The nested LIMIT
control also improves. Previous foldable-group planning decreases
15.901%/15.626%. These are comparisons against the immediately preceding
runtime, not evidence that earlier costs have all been removed.

Measured increases remain recorded. Ordinary numeric execution changes
0.1215405 -> 0.1260490 ms (+3.709%, +0.0045085 ms), with overlapping process
ranges. Native CAST planning changes 0.1566310 -> 0.1597315 ms
(+1.979%, +0.0031005 ms), with all four candidate medians above all four
baseline medians. Native SQL bypasses the Spark analyzer, so this comparison
does not attribute that increase to the new preparation. Equal printed plans
and overlapping ranges do not dismiss a measurement. Previous grouped COUNT
execution remains about 0.0657 ms; the preceding slice's roughly 13.5%-13.9%
increase remains open.

[Issue 161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) retains
all costs, including smaller changes in HAVING/local/selected controls. Every
SQL statement, absolute/relative phase median, process median, raw sample,
plan, output check and source/binary/library identity is retained. No
same-binary calibration or instruction/allocation/memory counters were
collected. Cost attribution and final performance acceptance remain open.

## Reproduce

`group-only-limit-results.json` pins `group-only-limit-runs.json.gz`. Extract
its `files` map into a scratch directory and run the archived
`check-archive.py` from the repository root. It verifies source and patch
identities, reference composition, retained comparisons, native regression
records, Delta outcomes and every raw timing median without rebuilding Rust
or starting Spark.

`prepare.py` reconstructs the accepted source set from the preceding hashed
archive. Build, native-test, Spark-reference and benchmark commands remain in
the evidence. Rebuilding requires the recorded toolchains/dependencies and
adapted cache paths. Shared sources, executable slots and Delta fixtures are
restored after validation.
