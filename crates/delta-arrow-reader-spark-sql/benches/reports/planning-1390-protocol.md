# Planning attribution protocol (#1390, PR1)

This reference fixes the measurement before it runs. It is for maintainers
choosing the two bounded optimization changes in #1390. PR1 changes benchmark
artifacts only. It measures planning over registered, warm providers and does
not execute SQL or change parser, resolver, rule, or planner behavior.

## Source and fixture boundary

The product baseline is staging commit
`9fa5b4cd98fecc7a2e95debc67128c394b25fb9a`. The protocol commit must descend
from that commit and contain the reviewed measurement files. Assembly, builds,
plan checks, and measurement require the same clean, committed protocol.

[`../planning_attribution/frozen.json`](../planning_attribution/frozen.json)
records the unchanged #1379 benchmark, queries, eligibility, schedule, scripts,
reports, lockfile, fixture helpers, and relevant production source. The runner
checks these hashes before assembly, builds, plan checks, and measurement.
All #1379 files remain byte-identical. Their historical product baseline was
`7e41302dd74e857373c0e993231b2a106ab27041`; their timings are historical evidence,
not the current baseline.

The new driver uses the exact existing `planning.rs` fixture builders. Assembly
copies that file's bytes to a new sibling module and appends one child-module
declaration. The child can call its original private input/session builders,
configuration helpers, and scan checks. The original executable entry point and
its execution preflight are unreachable from the new entry point. The generated
module retains the original include paths. No fixture builder is maintained a
second time.

The frozen final #1379 preflight contains 120 successful equivalence records:
25 paired and five stress statements on native-plain, native-configured, Spark
ANSI off, and Spark ANSI on. They matched values and executed SQL types without
extra Delta reads. Its eligibility contains all 279 ordinary identities in
both Spark ANSI modes, with no exclusions. The new runner checks the actual
`equivalence`, `matches`, and `extra_delta_io` fields and the full identity sets.
It never calls the old preflight.

Time all 30 paired/stress identities in all four configurations, including
native-plain stress. Keep the 558 ordinary Spark requests in the untimed plan
check. A previously eligible failure stops the run with its full error and ID;
it does not become a new exclusion. No ordinary query receives new timing rows.

Reuse the exact #1379 Delta fixture bytes. The runner requires the two file
hashes recorded in its metadata and checks them again after each schedule.
Registration uses the original `WarmupMode::QueryPlanning` provider. Fixture
creation, Delta log loading, snapshot warmup and session registration occur
outside every timer. Every returned physical plan must show zero started scan
partitions, zero started file tasks and zero received Parquet data bytes.

## Canonical and observed source

`assemble.py` extracts committed files with `git archive` into a new external
directory. It never copies ignored checkout content or modifies Cargo's source
cache. Both assemblies add the same benchmark-only entry and driver. Canonical
production source remains untouched. The observed assembly applies 61
exact-once, anchored insertions/replacements to existing source. Anchor drift
fails before building. A small standard-library timing buffer is copied into
the four existing crates that need it; no dependency or fork is added.

Each assembly records generated file hashes, anchor hashes, the source commit,
and a hash of the complete assembled source tree. The build checks them before
and after Cargo. Build receipts retain the exact compiler-reported executable
path, a sealed executable copy and hash, the compiler version, command,
environment, assembly receipt and logs. Both builds may share the existing
external owned Cargo target directory and run serially. The sealed canonical
binary cannot be overwritten by the observed build.

Build with `cargo +1.96.0 build --locked --release --bench planning_attribution
--message-format=json-render-diagnostics`, `CARGO_BUILD_JOBS=2`, and
`CARGO_PROFILE_RELEASE_DEBUG=1`. These retain the #1379 compiler/profile flags;
the new target name, source location, and `--locked` are explicit differences.
Compiler wrapper and Rust flag overrides are rejected. No other benchmark or
build runs concurrently. Use logical CPU 2, whose SMT sibling is CPU 10. Record
the host and governor without changing system settings.

The original current-thread Tokio runtime, one target partition, batch size
256, one provider partition and input batches of 32 are retained. Each measured
query has two warmups followed by nine samples. Options precede its timers.
Logical starts immediately before `plan_query` or `SessionContext::sql`;
physical starts immediately before the corresponding public
`create_physical_plan`. Scan checks, event draining, plan snapshots, output and
the returned physical plan's destruction are outside those timers.

## Plan and exact schema checks

Before timing, each build makes four planning-only trace processes. Both call
the real public Spark or native APIs. The canonical trace captures resolved
logical display with schemas, logical qualifiers and functional dependencies,
Spark output names, detailed physical display, and each physical node's name,
children, output partitioning/order, required input distribution/order, and
order-preservation flags.

Every logical and physical node retains its complete Arrow schema through a
schema-only IPC stream. The observed process decodes and compares those schemas
with `Schema` equality. This preserves nested Fields, nullability, metadata,
schema metadata and Arrow storage representation. IPC metadata ordering is not
a false mismatch. There is no SQL-type conversion or nullability/metadata
removal. All other snapshot fields compare exactly. The schema comparator's
round-trip and deliberate nullability, field-metadata and string-storage
mismatches are checked outside timers in every process.

The frozen workload has no unseeded time/random call; its ordinary RAND use has
seed 7. A changed display or schema fails the trace rather than being normalized
away. The eight processes preserve 1,356 trace records: 678 per build. This
establishes exact public plan display/property/schema parity. Opaque physical
node internals have no general structural equality API. Historical #1379 result
checks remain the result evidence; this run does not establish new result
equivalence by executing queries.

## Attribution boundaries

All stage values are inclusive wall intervals. Parent intervals and their
children are separate rows and must not be added together. Per-sample residuals
subtract direct child intervals from their parent. Residuals retain framework
work, teardown, one-statement extraction, query assembly, and observation
overhead rather than assigning that work to a rule.

| Part | Actual source boundary |
| --- | --- |
| Logical preparation | `plan_query`: variable substitution, AST-to-read-only conversion and timezone normalization separately |
| Statement parsing | `parse_one_statement` wall; the real `parse!` macro's lexer construction, lexing, token-input adapter, statement-parser construction and `parser.parse` |
| Resolution | `plan_statement` resolver preparation and `resolve_named_plan` separately |
| Physical preparation | `SparkSqlQuery::create_physical_plan`: state cloning/start-time/extensions and DataFrame construction |
| Analyzer | `execute_and_check` wall, input/final invariant checks, generated function-rewrite setup and each actual `AnalyzerRule::analyze` call |
| Logical optimizer | `optimize` wall, initial/final checks, initial/pass signatures, each rule/pass's preparation, actual rewrite/tree walk and post-rule schema validation separately |
| Spark planner | Full `SparkQueryPlanner::create_physical_plan`, subdivided as below |
| Renaming | Final `rename_physical_plan` |

The analyzer includes the generated `ApplyFunctionRewrites` pass and all default
and configured Spark rules. Optimizer events retain pass index, ordered
top-level rule applications and transformed status. Report summed per-rule
rewrite time across passes, application counts and pass indices; the raw events
retain each application. A top-level application includes the optimizer's
`rewrite_with_subqueries` traversal when it owns recursion. It is not a count of
per-node `rewrite` callbacks. Failed rule handling and validation are preserved.

The locked DataFusion 54.1.0 observers run after validation. Differences between
observer callbacks would include checks and framework work, so they are not
used as rule-body timings. In particular, `assert_valid_optimization` is a
separate validation bucket, not charged to its preceding rule body.

SparkQueryPlanner has six subdivisions: extension/order scan; Spark
expression/window/prefix lowering; `drop_declaration_inputs`;
`align_result_metadata_for_execution`; `DefaultPhysicalPlanner`; and seeded
RAND binding. Its whole wall remains a separate row. `DefaultPhysicalPlanner`
includes its physical optimizer passes and invariant work; PR1 does not claim
individual physical optimizer costs. Further internal subdivisions require a
new reviewed protocol if this initial partition leaves a large bucket unresolved.

Both native configurations receive the same analyzer/optimizer instrumentation.
Native-configured also passes through the same SparkQueryPlanner with Spark
semantics absent, giving controls for those six subdivisions. Native-plain's
remaining physical wall includes the default physical planner and its wrapper;
it is labelled residual. No new DataFusion fork is introduced to divide it.

## Fixed process schedule and noise

[`../planning_attribution/schedule.json`](../planning_attribution/schedule.json)
fixes 70 measurement processes. It retains #1379's five canonical
native-configured A/B pairs and five canonical main processes per configuration.
Each canonical main process is followed by an observed A/B pair in alternating
order. Observed A supplies five main processes per configuration; observed B
supplies its same-binary stage-noise control. Every observed configuration has
five A/B pairs, including native-plain. Every process has a 300-second timeout.
The eight trace processes bring the benchmark process cap to 78.

The schedule runs once, in order, into new destinations. A failed process stops
it and leaves its logs. There is no automatic retry, adaptive repetition,
query reordering, or second run to choose a number. The B controls remain in the
fixed schedule even when a stage appears stable.

For each stage and case, take each process's median of nine samples, then the
median of the five process medians. Retain their minimum, maximum and inclusive
interquartile range. Compute total from each iteration's logical-plus-physical
values, never by adding aggregated medians. Rule/pass event order must match
across observed samples and processes.

Canonical phase totals are the production baseline. Observed wall inflation is
reported separately for every case and phase. Same-binary observed A/B absolute
differences and ratios describe each stage's noise; canonical native-configured
A/B retains the original whole-phase noise control. Do not subtract guessed
clock or allocation overhead from stage values. Values near their stage's A/B
difference remain unresolved. Native-plain's canonical main dispersion is
available, but it has no additional canonical A/B series.

## Running and retained evidence

Run from `crates/delta-arrow-reader-spark-sql`, after the reviewed measurement
files and this protocol are committed. Each destination below must be new.
`DAR_DELTA_ROOT` names the existing #1379 fixture directory.

```sh
python3 benches/planning_attribution/run.py --check
TASK_EVIDENCE=/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1390
python3 benches/planning_attribution/run.py assemble canonical "$TASK_EVIDENCE/source-canonical"
python3 benches/planning_attribution/run.py assemble observed "$TASK_EVIDENCE/source-observed"
python3 benches/planning_attribution/run.py build "$TASK_EVIDENCE/source-canonical" /home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 "$TASK_EVIDENCE/build-canonical"
python3 benches/planning_attribution/run.py build "$TASK_EVIDENCE/source-observed" /home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 "$TASK_EVIDENCE/build-observed"
python3 benches/planning_attribution/run.py trace "$TASK_EVIDENCE/build-canonical/receipt.json" "$TASK_EVIDENCE/build-observed/receipt.json" "$DAR_DELTA_ROOT" "$TASK_EVIDENCE/plans"
python3 benches/planning_attribution/run.py measure "$TASK_EVIDENCE/plans" "$DAR_DELTA_ROOT" "$TASK_EVIDENCE/run"
python3 benches/planning_attribution/run.py summarize "$TASK_EVIDENCE/run" "$TASK_EVIDENCE/report"
```

The static check validates frozen bytes, historical proof, identities, exact-once
anchors, no execution calls in the new driver, fixed schedule and residual
arithmetic. It does not build or run a query. Assembly receipts, sealed binaries,
build logs and each process's stdout/stderr remain under the external evidence
directory. Deterministic compressed raw samples and complete plan traces,
metadata, receipts, per-stage CSV, stage-noise CSV, canonical/observed wall CSV,
and the large-component CSV accompany the final compact report in new
`planning-1390-*` files. No generated timings are written into the #1379 files.

The report names source code for each component larger than the whole canonical
native-configured logical or physical phase for that query. It includes residual
and observer limits, both native controls, all four configurations, and every
paired/stress identity. PR1 makes no optimization or speedup claim. Invalid SQL
behavior, CLI/Python activation and row-value parsing optimizations are outside
this measurement change.
