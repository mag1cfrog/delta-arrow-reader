# Scalar CAST reachability

[Issue 231](https://github.com/mag1cfrog/delta-arrow-reader/issues/231) repairs
when Spark SQL evaluates failing scalar casts. A dead CASE branch or a
NULL-propagating parent can discard a cast; a reachable cast must still
fail. For example, a NULL DOUBLE divided by CAST('bad' AS DOUBLE) returns
NULL, while a live invalid CAST raises CAST_INVALID_INPUT in ANSI mode.
Local VALUES, unused projections, LIMIT 0 and proven-empty inputs also
have ordering rules that affect whether an error is observable.

This candidate layers on integration fb0765449fb00acfadefb9959971e982cccf4a57,
including the accepted [floating-string CAST fix](FLOATING_STRING_CAST.md).
It is an optional patch under review. The default vendored runner remains
unchanged; [issue 165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
owns adoption into a clean default build. All planning and execution changes
are Rust. Python captures Spark references and checks archived evidence.

## Implementation boundary

The [DataFusion patch](datafusion-dead-cast.patch) changes the shared
expression simplifier. It prunes proven NULL-propagating scalar parents
before folding their failing children and defers literal CAST or UDF
simplification failures inside conditional expressions. Scalar subqueries
retain their separate checks. Fully constant AND/OR expressions are evaluated
as a parent first, preserving Spark's left-to-right distinction: false AND
bad_cast succeeds, while bad_cast AND false still fails. AND/OR are excluded
from blanket conditional error deferral.

The [Sail patch](sail-dead-cast.patch) reuses the existing analyzer and native
CASE selection. It guards column-CAST numerators when a simple deterministic
divisor is NULL, covering checked division, integer DIV and remainder. The
existing column-cast recognizer is shared instead of duplicated. Constant
CAST failures and arbitrary or volatile divisors are outside this narrow
runtime guard. A simple divisor can be evaluated again on live rows; the
measured cost remains open below.

The analyzer checks constant string-CAST errors over local inputs before
pruning projections and LIMIT 0. It uses input schemas and distinguishes a
one-row EmptyRelation from an actually empty input. Existing numeric-column
prechecks remain in place. Existing NULL-subquery validation also recognizes
checked floating division, preserving previously passing live-CASE errors.
This does not finish general early-arithmetic or discarded-subquery policy,
owned by [issue 232](https://github.com/mag1cfrog/delta-arrow-reader/issues/232)
and [issue 233](https://github.com/mag1cfrog/delta-arrow-reader/issues/233).

Only three selected source files change: Sail math.rs and decimal_null.rs,
and DataFusion expr_simplifier.rs. Package versions and features are unchanged;
all 505 benchmark link-artifact package/name/feature signatures match.
Eight of the 414 named library hashes change through recompilation. The
archive pins all 95 selected source entries, both builds and private link
manifests. No new arithmetic kernel or dependency is introduced.

## Correctness evidence

All 33 primary observations assigned to this leaf match Spark. One already
passed after issue 238; this patch fixes the remaining 32 and retains all 33
opposite-ANSI controls. The assigned/control set improves from 34/66 to 66/66.
The expanded set improves from 182/248 to 212/248. A separate AND/OR,
constant-versus-column and nullable-divisor guard set improves from 28/32 to
32/32. Combined, the [173-query corpus](dead-cast.jsonl) improves from
244/346 to 310/346 across both ANSI modes, without losing an agreement.

The 36 retained differences are all present in the accepted baseline:

- Six ANSI Decimal early-error observations are owned by
  [issue 246](https://github.com/mag1cfrog/delta-arrow-reader/issues/246).
  Spark raises for constant invalid STRING-to-DECIMAL casts in selected
  empty/unused plans; the candidate still returns an empty or unused result.
- Twenty-four STRING-peer remainder observations are owned by
  [issue 247](https://github.com/mag1cfrog/delta-arrow-reader/issues/247).
  The planner rejects mixed STRING/BIGINT percent expressions before applying
  Spark's conversions. The explicit floating CAST remainder guard is separate.
- Six invalid-type/missing-column observations reject in both engines but
  differ in error identity, owned by
  [issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).

Both new leaves are assigned to mag1cfrog and attached natively below issue
172. Issue 246 depends natively on this leaf. The remaining set is a bounded
corpus result, not a count of every remaining Spark incompatibility.

Prior explicit numeric CAST, strict integer and floating grammar suites retain
562/562, 496/496 and 488/488. The earlier selected CAST/control set improves
from 90/126 to 122/126. Integer-overflow controls retain 616/616 and all 131
complete error payloads. All 12 dedicated suites retain their earlier
agreements: string division improves 472/480 to 480/480, BIGINT DIV 232/234
to 234/234, and modulo 427/442 to 429/442. The other nine stay at their
accepted counts; all case-level comparisons are archived.

Across 37 historical groups, agreement improves from 6,508/6,784 to
6,542/6,784 without losing a prior agreement. There are 34 changed
value/type/status outcomes: 26 errors become successful results, and eight
successful results become required Spark errors. Historical comparison uses
its established Decimal-text and coarse-stage rules; it does not establish
complete schema or exact diagnostic compatibility.

Rust tests pass: 314 function, 48 planner and 28 runner tests. The new
planner test switches ANSI on/off/on within one session across 15 SQL shapes.
The comparator self-check passes. Real-Delta validation retains all 116
previous outcomes and passes 18 adapter checks. The frozen Spark comparison
remains 47 matches, 58 differences and 11 pending adapters.

### Schema and error details

| Corpus | Logical before / after | Physical before / after | Phase-label differences before / after |
| --- | ---: | ---: | ---: |
| combined | 4 / 12 | 25 / 44 | 96 / 30 |
| assigned | 0 / 4 | 7 / 16 | 32 / 0 |
| expanded | 2 / 6 | 8 / 16 | 60 / 30 |
| guards | 2 / 2 | 10 / 12 | 4 / 0 |
| legacy | 54 / 54 | 210 / 210 | 0 / 0 |
| previous_owned | 0 / 0 | 4 / 7 | 36 / 4 |

These dimensions are reported separately from value/type/error-cause
agreement. Harness phase labels do not identify Spark's internal optimizer
stage. In the combined corpus, 17 retained error payloads change,
including 11 error-text changes. Historical replay retains 882 error
outcomes; 89 full payloads and 20 error texts change. Two multi-error Decimal
queries report a different first invalid row. The exact IDs, complete before
and after payloads, and logical/physical plans remain in the archive for
issue 149. Nothing is silently normalized into full diagnostic agreement.

## Bounded performance check

Both benchmark executables use the same source and private accepted/candidate
library sets, without falling back to the shared target directory. The fixed
process order is before, after, after, before, after, before, before, after.
Each process uses CPU 2, one partition, 1,048,576 rows, batches of 8,192,
eight warmups and 41 samples per phase. The table reports the median of four
process medians. The host was not reserved; no samples or processes were
discarded and no same-binary calibration was run. All raw samples, commands,
plans, binary/library hashes and per-process medians are retained.

Fourteen queries run with nonnullable and 10%-NULL divisors. The ordinary
numerator columns are valid and nonnullable; guarded_invalid puts invalid
strings only on rows whose divisor is NULL. Every successful process checks
all rows against a frozen Spark reference, including exact Float64 bits or
Int64 values, NULLs and types. Full-output digests agree for all 21 comparable
pairs. Seven configurations failed before and have candidate-only timings;
they have no speedup ratios.

The new nullable-divisor guard has a material execution cost: integer CAST
DIV increases from 18.157823 to 25.089360 ms (+38.17%), floating CAST division
from 18.745880 to 24.925973 ms (+32.97%), and floating CAST remainder from
23.247129 to 29.072609 ms (+25.06%). For each of these three, all four
candidate process medians exceed all four baseline medians. The corresponding
nonnullable executions change +1.76%, +0.28% and -0.13%.

This points to the newly selected CASE/projection path, not an equally large
cost in ordinary numeric execution. It is not an allocation-level attribution.
Strict/TRY CAST controls show smaller positive shifts around 2%, with overlap
between process ranges; these remain recorded rather than dismissed as noise.
Physical plans expose the added selection/projection work. Allocations,
hardware counters and memory were not measured. This check compares against
the already patched issue 238 runtime and does not resolve its earlier costs.

| Query / divisor NULLs | Planning before / after ms | Change | Execution before / after ms | Change |
| --- | ---: | ---: | ---: | ---: |
| integer_cast_div_nullsfalse | 0.381609 / 0.347155 | -9.03% | 18.051340 / 18.368544 | +1.76% |
| float_cast_div_nullsfalse | 0.369026 / 0.334060 | -9.47% | 18.268529 / 18.320104 | +0.28% |
| float_cast_mod_nullsfalse | 0.395680 / 0.359808 | -9.07% | 23.182815 / 23.152118 | -0.13% |
| legacy_cast_div_nullsfalse | 0.391112 / 0.384870 | -1.60% | 7.969530 / 8.175153 | +2.58% |
| strict_cast_nullsfalse | 0.333444 / 0.301450 | -9.60% | 16.443599 / 16.769103 | +1.98% |
| float_cast_nullsfalse | 0.330459 / 0.272091 | -17.66% | 16.969631 / 16.932251 | -0.22% |
| try_cast_nullsfalse | 0.333190 / 0.332708 | -0.14% | 16.069425 / 16.401200 | +2.06% |
| integer_div_nullsfalse | 0.314805 / 0.284764 | -9.54% | 1.573087 / 1.547303 | -1.64% |
| numeric_nullsfalse | 0.337542 / 0.327603 | -2.94% | 0.612693 / 0.612873 | +0.03% |
| native_numeric_nullsfalse | 0.183711 / 0.183561 | -0.08% | 0.252479 / 0.251843 | -0.25% |
| dead_case_nullsfalse | error / 0.384790 | candidate only | error / 0.060012 | candidate only |
| column_case_nullsfalse | error / 0.503631 | candidate only | error / 0.701533 | candidate only |
| null_literal_nullsfalse | error / 0.333033 | candidate only | error / 0.141673 | candidate only |
| guarded_invalid_nullsfalse | 0.408208 / 0.364652 | -10.67% | 18.078796 / 18.388447 | +1.71% |
| integer_cast_div_nullstrue | 0.395610 / 0.373073 | -5.70% | 18.157823 / 25.089360 | +38.17% |
| float_cast_div_nullstrue | 0.354579 / 0.433565 | +22.28% | 18.745880 / 24.925973 | +32.97% |
| float_cast_mod_nullstrue | 0.373880 / 0.458978 | +22.76% | 23.247129 / 29.072609 | +25.06% |
| legacy_cast_div_nullstrue | 0.370844 / 0.361075 | -2.63% | 8.462432 / 8.120351 | -4.04% |
| strict_cast_nullstrue | 0.314875 / 0.279610 | -11.20% | 16.572444 / 16.937802 | +2.20% |
| float_cast_nullstrue | 0.282870 / 0.249347 | -11.85% | 17.050025 / 16.996336 | -0.31% |
| try_cast_nullstrue | 0.308854 / 0.307256 | -0.52% | 16.119371 / 16.436431 | +1.97% |
| integer_div_nullstrue | 0.295990 / 0.265077 | -10.44% | 1.516687 / 1.506067 | -0.70% |
| numeric_nullstrue | 0.316012 / 0.304115 | -3.76% | 0.628111 / 0.611390 | -2.66% |
| native_numeric_nullstrue | 0.184668 / 0.183416 | -0.68% | 0.255970 / 0.253385 | -1.01% |
| dead_case_nullstrue | error / 0.362028 | candidate only | error / 0.060056 | candidate only |
| column_case_nullstrue | error / 0.481274 | candidate only | error / 0.704317 | candidate only |
| null_literal_nullstrue | error / 0.315025 | candidate only | error / 0.144168 | candidate only |
| guarded_invalid_nullstrue | error / 0.374310 | candidate only | error / 24.850994 | candidate only |

[Issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) owns
these costs together with prior CAST measurements. Its evidence includes
all four process medians for both phases and all 28 configurations. Final
performance acceptance remains open; compatibility acceptance does not imply
that these regressions are resolved.

## Reproduction and rejected attempts

[Results](dead-cast-results.json) pin the repository artifacts and
[compressed evidence](dead-cast-runs.json.gz). The archive contains raw Spark
and Rust captures, the final source snapshots, exact patches, source/build
manifests, tests, all eight timing processes and failed prototypes. Existing
accepted archives supply the frozen historical references and predecessor
captures. Every archive and every member is SHA-256 checked.

From the repository root, extract and run the offline verifier:

```sh
python - <<'PY'
import gzip, json
from pathlib import Path
root = Path('experiments/spark-sql')
a = json.loads(gzip.decompress((root/'dead-cast-runs.json.gz').read_bytes()))
Path('/tmp/check-dead-cast-archive.py').write_text(a['files']['check-archive.py'])
PY
python /tmp/check-dead-cast-archive.py
python -m unittest discover -s experiments/spark-sql -p test_division_cast_classification.py
```

The verifier reruns comparisons and recomputes timing summaries without
Spark, Rust compilation or network access. The corpus generator reuses the
existing comparator and frozen ownership classification. It can regenerate
the 173 SQL queries with dead_cast.py generate.

For a live rebuild, the archive's prepare.py, source-paths.json, override.toml
and build.py describe the cumulative source selection and exact Cargo
command. Install the final candidate-expr-simplifier, candidate-math.rs and
candidate-decimal-null.rs snapshots, then use build.py; do not regenerate
older prototypes with candidate.py or refine.py. These scripts retain the
original scratch-host/cache paths and are not a clean-checkout installer.
Issue 165 owns that integration. Sources, shared executable slots and Delta
inputs were restored and their hashes verified after the run.

Attempt 1 did not compile because of a borrow/move conflict in the new
NULL-folding helper. Attempt 2 fixed only 27 of the 32 remaining assigned
observations: it used output schemas for input expressions, treated
DataFusion's one-row EmptyRelation as empty, and hid left-side AND/OR errors.
Attempt 3 passed the assigned/guard sets but lost 166 historical agreements:
its local evaluation was too broad for legacy numeric casts and COALESCE,
and it lost two live-CASE subquery errors. All are rejected. The final patch
restricts the extra local check to constant string casts, lowers conditional
functions with the existing simplifier and retains checked-float subquery
validation. The rejected sources, captures and logs remain inspectable.
