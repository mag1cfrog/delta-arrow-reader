---
title: Frozen Spark SQL experiment
description: The preserved Spark and Sail extraction experiment, its tested context, unresolved correctness and maintenance costs, and inspection paths.
---

# Frozen Spark SQL experiment

Status: frozen by maintainer decision on September 27, 2026 (UTC). Autonomous
implementation, review and performance work have stopped. This is an unfinished
engineering experiment, not a supported Spark-compatible frontend or an active
roadmap commitment. The decision is to stop investment, not to declare compatibility
complete. Python bindings proceed under the independent
[Reader plan](project-direction.md).

## Snapshot and evidence

The last integrated experiment is commit
[`bde063e527547f20e687548cd8cda098c1b0fb88`](https://github.com/mag1cfrog/delta-arrow-reader/commit/bde063e527547f20e687548cd8cda098c1b0fb88),
the normal merge of evidence PR #292 into `feat/spark-sql-extraction`.
The archival tag `archive/spark-sql-2026-09-27` identifies that exact pre-closeout
tree. The experimental branch is retained; subsequent freeze documentation does
not change the accepted runtime or make the unfinished candidate part of it.

Use the immutable
[experiment directory](https://github.com/mag1cfrog/delta-arrow-reader/tree/bde063e527547f20e687548cd8cda098c1b0fb88/experiments/spark-sql),
[README](https://github.com/mag1cfrog/delta-arrow-reader/blob/bde063e527547f20e687548cd8cda098c1b0fb88/experiments/spark-sql/README.md)
and [provenance record](https://github.com/mag1cfrog/delta-arrow-reader/blob/bde063e527547f20e687548cd8cda098c1b0fb88/experiments/spark-sql/UPSTREAM.md).
Their descriptions of future work are historical. Test inputs, failing cases,
captures, checks, reports and rejected alternatives remain available.

## What was attempted

The experiment extracted an in-process, read-only SQL frontend from Sail. Its
parser and analyzer resolve Spark-shaped SQL through retained Sail planning and
function code into DataFusion plans. Tables come from the existing native Delta
provider; the Reader still owns Delta/Parquet scans and incremental Arrow output.
Small execution adapters support range, partition IDs, required/partition-local
sorting and monotonic IDs.

Python execution, Spark Connect services, Sail catalogs/storage/writes and
streaming/checkpoint services were removed from the extracted runtime. Python and
Java remain external reference/capture tools. The experimental workspace is separate
from the root library, and the package excludes `experiments/**`. The main Reader
does not acquire the extracted frontend as a dependency.

## Tested versions and configurations

| Component | Recorded context |
| --- | --- |
| Spark reference | Apache Spark / PySpark 4.2.0; original oracle CPython 3.13.12, Temurin JRE 21.0.12.1+1, Py4J 0.10.9.9 |
| Sail source | Sail 0.7.1, commit `9544c9253e981a82c5f9e493c43ce98a4d9d41b7`, Apache-2.0; source notices and LICENSE retained |
| Native extraction | DataFusion 54.1.0, Arrow/Parquet 58.4.0; tested extraction compiler Rust 1.97.1; Sail declares Rust 1.96.0 |
| Core snapshot dependencies | Delta Kernel 0.25.0; core MSRV remains separate from the extraction compiler |
| Capture/schema tools | PyArrow 25.0.1 in the recorded private environments |
| Original oracle | UTC, ANSI enabled and case-insensitive names, with per-query overrides; local Spark has two workers and two shuffle partitions |
| Later C04 corpora | Both ANSI modes, recorded SQL/settings and batch/partition controls; overlapping corpora, not a count of independent supported features |

There are two different native baselines. A normal build uses the committed
vendored checkpoint. Later C04 repairs are optional patch/source/artifact selections
documented by each report and archive. A clean checkout does not automatically
reproduce the selected patched runtime. That adoption/build gap is still
[#165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165), not a completed
packaging step. Preserve exact manifests instead of substituting whatever happens
to be in a mutable Cargo/source cache.

## Demonstrated behavior and remaining gaps

The extracted checkpoint demonstrated queries over real Delta fixtures, projection
and pruning through `DeltaScanExec`, retained snapshots, deletion vectors, early
stream drop and mid-stream failure. Partition IDs, partition-local sorting,
ordered FIRST and monotonic IDs progressed from missing execution adapters to
working bounded checks. The historical 116-case checkpoint recorded 87 successful
queries, 18 planning errors and 11 execution errors. Its Spark comparison was
47 strict matches, 58 differences and 11 pending reference cases. Those are
historical checkpoint results, not a current product support table.

Subsequent optional slices repaired bounded integer/Decimal/ROUND/CAST and
preparation behaviors. The final accepted
[C04 coverage index](https://github.com/mag1cfrog/delta-arrow-reader/blob/bde063e527547f20e687548cd8cda098c1b0fb88/experiments/spark-sql/ARITHMETIC_COVERAGE.md)
preserves 67 groups, 15,945 SQL entries and 31,890 observations including overlap.
The legacy comparison records 30,513 matches; it does not establish complete
schema or error equivalence. Separate dimensions record:

- 2,857 logical and 6,512 physical nullability differences; 60 logical and
  19 physical type/precision/scale differences. Dimensions overlap and some are
  unobserved, so they are not additive defect counts.
- Among 5,767 paired failures, 4,421 known-cause category matches, 576 ambiguous
  generic Decimal CAST diagnostics, 539 unclassified causes and 231 different
  classifications. SQLSTATE/parameter equivalence is not established.
- 457 success/error outcome differences in the indexed captures. Existing
  accepted NULL-policy cases and unresolved findings keep their original evidence
  and owners; no new waiver is created by freezing.
- The independently reproduced two-output local CAST first-error difference:
  Spark reports first-row `100` from the second output, while the accepted native
  build reports second-row `200` from the first output. Category agreement does
  not make these errors equivalent.

[#149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149),
[#172](https://github.com/mag1cfrog/delta-arrow-reader/issues/172) and the broader
compatibility initiative were not accepted. Parsing/name resolution, relational
and aggregation coverage, NULL/conditional behavior, strings, temporal behavior,
nested values, execution extensions, Delta integration and lifecycle coverage
also retain their recorded gaps. Examples include wildcard EXCEPT, per-query case
sensitivity, correlated subqueries, ARRAY_CONTAINS NULL semantics, DATE_DIFF width,
DST handling and ELEMENT_AT bounds. No unchecked case was changed to a pass and
no correctness test was weakened.

## Performance and maintenance findings

Results belong to their exact builds and workloads. The
[last accepted local Decimal CAST report](https://github.com/mag1cfrog/delta-arrow-reader/blob/bde063e527547f20e687548cd8cda098c1b0fb88/experiments/spark-sql/LOCAL_DECIMAL_CAST.md)
measures nine queries under two input NULL configurations. Local queries use two
or four VALUES rows; nonlocal controls use 262,144 rows in batches of 8,192. It
uses one partition, CPU 2, four warmups, 21 samples per phase, and eight balanced
before/after processes; each result is the median of four process medians.

Local strict CAST planning increased 0.013566-0.020333 ms (2.14%-2.65%). For one
local rounding configuration, planning changed from 0.795713 to 0.812764 ms.
The nonlocal no-NULL Decimal execution control changed from 2.353001 to
2.348202 ms. These small control differences are observations, not calibrated
attribution: that slice has no same-binary calibration and gives no final
performance acceptance. Complete tables, raw samples and identities stay with
[#159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159).

Earlier checked-integer cost, argument ownership, NULL filtering, buffer and
rejected SIMD/block prototypes remain in their named reports. Faster incorrect
baselines are not interchangeable with correct execution. No new performance
loop was run to justify this stop decision.

The recorded extraction inventory reduced 12 Sail crates and 126,013 gross Rust
lines to eight crates and 91,529 lines at the monotonic-ID checkpoint. Its graph
still held 524 all-target packages and 452 Linux normal/build packages, including
the Reader and runner. These historical measurements include comments/tests and
do not count the later optional patch stack as a fresh full inventory.

Most remaining code implements SQL semantics: coercion, ANSI/NULL handling,
decimal arithmetic, datetime behavior, functions, field types/names and optimizer
preparation. Keeping it would require maintaining those semantics and their patch
ordering across upstream versions, with independent reference/schema/error and
performance validation. Required-order plans also disable automatic round-robin
repartitioning in a cloned planning state to preserve order, with a possible
parallelism cost. Source reduction alone did not eliminate this responsibility.

## Unfinished candidate and shutdown record

The executor stopped on [#293](https://github.com/mag1cfrog/delta-arrow-reader/issues/293)
on `fix/local-projection-error-order` at `bde063e`. Its repository worktree was
clean. A private Rust candidate, paired sources, 52-query captures, regression
logs and replay results remain under
`/home/hanbo/.cache/delta-reader-spark-planning/local-projection-order-2a82385/`.
The candidate source SHA-256 is
`654bb7e8e7316ab6277e87720d9e264b6084171bcece919abca3e70588bcac1b`.
It has no approved commit/PR, no independent review and no completed cost check.
Executor-reported focused results are preliminary evidence, not an accepted fix.

The reviewer had completed the bounded PR #292 evidence review for `0c7e63cf`.
Its reports and private reproduction files remain under
`/home/hanbo/.local/share/delta-arrow-reader-review/reviews/`.
Both tasks recorded a stop and ended their turns. Already-started executor replay
and baseline linking had finished; no repository write was killed.

The Windows native automation `delta-arrow-reader-linux` is paused. The existing
Linux `send.py` refuses delivery while the coordination directory's `FROZEN`
marker exists. Historical workflow and tick files were backed up and marked
inactive. No replacement timer, loop or supervisor was created.

The coordination directory's `freeze-2026-09-27/` contains a complete Git bundle,
issue/dependency snapshots, working-tree patches and a verified archive of the
private candidate and review evidence. Its manifest records archive hashes.
The unrelated DV and dependency-upgrade worktrees retain their uncommitted files;
they were backed up, not reset or incorporated into this decision.

## Inspect or reproduce later

Use a separate checkout of `archive/spark-sql-2026-09-27`; do not replace an existing
working tree. Start with the immutable README and the report for the desired
baseline. Its `Run the references`, `Check the fixed oracle` and `Run the extracted
frontend` sections contain the original commands and environment requirements.

For an offline C04 evidence check, extract the `files` map from
`arithmetic-coverage-runs.json.gz` into a fresh directory and use PyArrow 25.0.1:

```sh
python /path/to/extracted/check-archive.py /path/to/frozen-checkout/experiments/spark-sql
```

That checks saved evidence; it does not rebuild or rerun an engine. The local-CAST
archive likewise contains paired private source/build manifests and a checker.
Inspect those manifests before rebuilding the selected optional runtime; the
default vendor build and newer dependencies are different experiments. Keep
source hashes, reference settings, unknowns and failures when reproducing a result.

The freeze does not authorize resumed compatibility work. Any restart, support
claim, new runtime adoption or larger upstream effort needs a new maintainer
decision. Existing isolated upstream findings remain available for separate
evaluation.

## Issue disposition at closeout

The following 62 issues were updated and read back after the change. All 24
roadmap/adoption closures use GitHub `not_planned`; none represents completed
compatibility. The 34 unresolved Spark issues remain open with `deferred`.
Already completed leaves and core/upstream investigations were left unchanged.

| Issue | Retained scope | State after closeout |
| --- | --- | --- |
| [#101](https://github.com/mag1cfrog/delta-arrow-reader/issues/101) | feat(python): ship PyO3 bindings and publish delta-arrow-reader to PyPI | Open: core Reader Python plan |
| [#102](https://github.com/mag1cfrog/delta-arrow-reader/issues/102) | feat(python): add an installable package with Delta table loading | Open: core Reader Python plan |
| [#109](https://github.com/mag1cfrog/delta-arrow-reader/issues/109) | ci(python): build platform wheels and publish through release-plz | Open: core Reader Python plan |
| [#112](https://github.com/mag1cfrog/delta-arrow-reader/issues/112) | design(python): investigate optional DataFusion provider FFI integration | Open: deferred |
| [#113](https://github.com/mag1cfrog/delta-arrow-reader/issues/113) | feat(sql): track Spark SQL frontend compatibility and adoption | Closed: not planned |
| [#116](https://github.com/mag1cfrog/delta-arrow-reader/issues/116) | fix(sql): preserve NULL semantics in ARRAY_CONTAINS | Open: deferred |
| [#117](https://github.com/mag1cfrog/delta-arrow-reader/issues/117) | perf(sql): reduce shared Float64 addition dispatch overhead | Open: deferred |
| [#137](https://github.com/mag1cfrog/delta-arrow-reader/issues/137) | test(sql): verify identifier and statement-boundary semantics | Open: deferred |
| [#138](https://github.com/mag1cfrog/delta-arrow-reader/issues/138) | fix(sql): support Spark wildcard EXCEPT projections | Open: deferred |
| [#139](https://github.com/mag1cfrog/delta-arrow-reader/issues/139) | fix(sql): honor per-query Spark case-sensitive name resolution | Open: deferred |
| [#140](https://github.com/mag1cfrog/delta-arrow-reader/issues/140) | fix(sql): execute retained nested correlated scalar subqueries | Open: deferred |
| [#141](https://github.com/mag1cfrog/delta-arrow-reader/issues/141) | test(sql): complete retained relational-query compatibility coverage | Open: deferred |
| [#142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142) | test(sql): resolve aggregation and window coverage gaps | Open: deferred |
| [#149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149) | test(sql): verify retained arithmetic schemas and error conditions | Open: deferred |
| [#150](https://github.com/mag1cfrog/delta-arrow-reader/issues/150) | test(sql): resolve remaining NULL and conditional behavior findings | Open: deferred |
| [#151](https://github.com/mag1cfrog/delta-arrow-reader/issues/151) | test(sql): verify string binary and concatenation semantics | Open: deferred |
| [#152](https://github.com/mag1cfrog/delta-arrow-reader/issues/152) | fix(sql): preserve DATE_DIFF result width and date arithmetic | Open: deferred |
| [#153](https://github.com/mag1cfrog/delta-arrow-reader/issues/153) | test(sql): verify timestamp formatting parsing and type contracts | Open: deferred |
| [#154](https://github.com/mag1cfrog/delta-arrow-reader/issues/154) | fix(sql): preserve Spark DST gap and overlap conversions | Open: deferred |
| [#155](https://github.com/mag1cfrog/delta-arrow-reader/issues/155) | fix(sql): preserve non-ANSI ELEMENT_AT bounds behavior | Open: deferred |
| [#156](https://github.com/mag1cfrog/delta-arrow-reader/issues/156) | test(sql): verify nested values maps and explode contracts | Open: deferred |
| [#157](https://github.com/mag1cfrog/delta-arrow-reader/issues/157) | perf(sql): evaluate retained analyzer and planning costs | Open: deferred |
| [#158](https://github.com/mag1cfrog/delta-arrow-reader/issues/158) | perf(sql): evaluate division rounding and allocator-sensitive controls | Open: deferred |
| [#159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159) | perf(sql): evaluate cast buffer ownership and conversion fallbacks | Open: deferred |
| [#160](https://github.com/mag1cfrog/delta-arrow-reader/issues/160) | perf(sql): evaluate floating comparison and IN costs | Open: deferred |
| [#161](https://github.com/mag1cfrog/delta-arrow-reader/issues/161) | perf(sql): evaluate floating joins projected IN and COUNT costs | Open: deferred |
| [#162](https://github.com/mag1cfrog/delta-arrow-reader/issues/162) | perf(sql): evaluate grouping normalization IDs and bit permutation | Open: deferred |
| [#163](https://github.com/mag1cfrog/delta-arrow-reader/issues/163) | perf(sql): evaluate DOUBLE arithmetic and final Decimal conversion | Open: deferred |
| [#164](https://github.com/mag1cfrog/delta-arrow-reader/issues/164) | perf(sql): validate the selected runtime on real Delta streams | Open: deferred |
| [#165](https://github.com/mag1cfrog/delta-arrow-reader/issues/165) | build(sql): reproduce the selected Spark runtime and integration checks | Open: deferred |
| [#166](https://github.com/mag1cfrog/delta-arrow-reader/issues/166) | docs(sql): record the tested Spark support and adoption decision | Closed: not planned |
| [#167](https://github.com/mag1cfrog/delta-arrow-reader/issues/167) | track(sql): Spark SQL compatibility | Closed: not planned |
| [#168](https://github.com/mag1cfrog/delta-arrow-reader/issues/168) | track(sql): Spark SQL performance validation | Closed: not planned |
| [#169](https://github.com/mag1cfrog/delta-arrow-reader/issues/169) | track(sql): C01 Parsing/names | Closed: not planned |
| [#170](https://github.com/mag1cfrog/delta-arrow-reader/issues/170) | track(sql): C02 Relational queries | Closed: not planned |
| [#171](https://github.com/mag1cfrog/delta-arrow-reader/issues/171) | track(sql): C03 Aggregation/windows | Closed: not planned |
| [#172](https://github.com/mag1cfrog/delta-arrow-reader/issues/172) | track(sql): C04 Arithmetic/casts | Closed: not planned |
| [#173](https://github.com/mag1cfrog/delta-arrow-reader/issues/173) | track(sql): C05 NULL/conditional | Closed: not planned |
| [#174](https://github.com/mag1cfrog/delta-arrow-reader/issues/174) | track(sql): C06 Strings/binary | Closed: not planned |
| [#175](https://github.com/mag1cfrog/delta-arrow-reader/issues/175) | track(sql): C07 Dates/timestamps | Closed: not planned |
| [#176](https://github.com/mag1cfrog/delta-arrow-reader/issues/176) | track(sql): C08 Nested values | Closed: not planned |
| [#177](https://github.com/mag1cfrog/delta-arrow-reader/issues/177) | track(sql): C09 Execution extensions | Closed: not planned |
| [#178](https://github.com/mag1cfrog/delta-arrow-reader/issues/178) | track(sql): C10 Delta integration | Closed: not planned |
| [#179](https://github.com/mag1cfrog/delta-arrow-reader/issues/179) | track(sql): C11 Lifecycle/failures | Closed: not planned |
| [#180](https://github.com/mag1cfrog/delta-arrow-reader/issues/180) | track(sql): C12 Excluded operations | Closed: not planned |
| [#181](https://github.com/mag1cfrog/delta-arrow-reader/issues/181) | track(sql): P01 Analyzer and planning | Closed: not planned |
| [#182](https://github.com/mag1cfrog/delta-arrow-reader/issues/182) | track(sql): P02 Division remainder and rounding | Closed: not planned |
| [#183](https://github.com/mag1cfrog/delta-arrow-reader/issues/183) | track(sql): P03 Casts and buffer ownership | Closed: not planned |
| [#184](https://github.com/mag1cfrog/delta-arrow-reader/issues/184) | track(sql): P04 Comparisons and IN | Closed: not planned |
| [#185](https://github.com/mag1cfrog/delta-arrow-reader/issues/185) | track(sql): P05 Joins and subqueries | Closed: not planned |
| [#186](https://github.com/mag1cfrog/delta-arrow-reader/issues/186) | track(sql): P06 Grouping ordering and windows | Closed: not planned |
| [#187](https://github.com/mag1cfrog/delta-arrow-reader/issues/187) | track(sql): P07 Arithmetic and final Decimal conversion | Closed: not planned |
| [#188](https://github.com/mag1cfrog/delta-arrow-reader/issues/188) | track(sql): P08 Real Delta query and stream profile | Closed: not planned |
| [#189](https://github.com/mag1cfrog/delta-arrow-reader/issues/189) | test(sql): verify retained execution-extension contracts | Open: deferred |
| [#190](https://github.com/mag1cfrog/delta-arrow-reader/issues/190) | test(sql): verify retained Delta-provider integration | Open: deferred |
| [#191](https://github.com/mag1cfrog/delta-arrow-reader/issues/191) | test(sql): verify retained planning and stream lifecycle | Open: deferred |
| [#192](https://github.com/mag1cfrog/delta-arrow-reader/issues/192) | test(sql): verify explicit unsupported-operation rejection | Open: deferred |
| [#202](https://github.com/mag1cfrog/delta-arrow-reader/issues/202) | perf(sql): decide checked integer kernel optimization | Open: deferred |
| [#203](https://github.com/mag1cfrog/delta-arrow-reader/issues/203) | perf(sql): reduce checked arithmetic argument ownership | Open: deferred |
| [#204](https://github.com/mag1cfrog/delta-arrow-reader/issues/204) | perf(sql): finish checked integer NULL filtering and query validation | Open: deferred |
| [#206](https://github.com/mag1cfrog/delta-arrow-reader/issues/206) | perf(sql): attribute BROUND query costs after Decimal support | Open: deferred |
| [#293](https://github.com/mag1cfrog/delta-arrow-reader/issues/293) | fix(sql): preserve row order across local CAST outputs | Open: deferred |

Issue #149 was already near the issue-body length limit. Its original body is
unchanged; the [decision comment](https://github.com/mag1cfrog/delta-arrow-reader/issues/149#issuecomment-5853462495)
and `deferred` label record the freeze.

Native roadmap relationships were also changed and verified: #113 no longer
blocks #102; #112 no longer blocks #109; #113 and #112 are no longer children of
#101; #106 now directly blocks #109 alongside the existing #107 prerequisite.
The original issue bodies, hierarchy and blocker records are backed up in the
closeout audit directory.

GitHub Projects board fields could not be inspected with the available token,
which lacks `read:project`. No credential scopes were changed. Issue state, labels,
sub-issues and native dependencies above were verified independently of boards.
