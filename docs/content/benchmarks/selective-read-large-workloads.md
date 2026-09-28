---
title: Large-workload selective-read benchmark plan
description: Scale calibration, workload coverage, and publication requirements beyond the small mechanism experiments.
---

# Large-workload selective-read benchmark plan

This amendment defines the next experiments under
[#312](https://github.com/mag1cfrog/delta-arrow-reader/issues/312).
It is a design contract, not a measured result or an implemented CLI.
The scale factors for publication remain unset until the pilot is complete.

The existing report profile uses original lineitem at SF10 but wide lineitem at
SF1. The clustered SF1 wide fixture contains 6,001,215 rows and about 3.8 GB of
Parquet data. Repacking those rows into 4,096 files changes metadata and file
geometry, not the data volume. The completed experiments validate correctness
and explain mechanisms; they do not establish performance on large workloads.

Keep that evidence and add larger experiments using the same five pinned
readers: DAR, delta-rs, DuckDB, Polars, and Daft. Both ordinary and DV snapshots
remain in the main comparison. No speedup is required for completion.

## Contract and historical records

The [revision 2 protocol](selective-read-protocol.md) remains byte-for-byte
unchanged. Its SHA-256 is
`affe5fde898c1e39c03792fadbfd79d17b6af8a16dbdaa48988869a4567d5efe`.
Its pins, semantics, correctness rules, resources, scheduling, and storage
observation rules apply except where this amendment explicitly changes them.

Adoption requires explicit comparison revision 3 support in the harness. New
records bind this amendment's hash as `protocol_sha256`, the revision 2 hash as
`base_protocol_sha256`, and a frozen `workload_manifest_sha256`. References,
correctness certificates, schedules, and reports must agree on all three.
Generator hashes remain provenance. Never relabel an old run as revision 3 or
overwrite an old fixture identity, case definition, or artifact.

Before formal measurement, the workload manifest must contain every concrete
case/session ID, source scale, layout, file geometry, snapshot, expanded SQL,
projection, literal set, fixture hash, reader/configuration identity, environment,
resource ceiling, timeout, and required sample count. A missing value prevents
publication. Pilot and formal campaign IDs are separate.

## What each group of experiments establishes

| Group | Required evidence | Role in the report |
| --- | --- | --- |
| Existing 46 cases and three reuse profiles | Exact revision 2 inputs and all five reader statuses | Mechanism and compatibility evidence; retain millisecond results with their scope |
| Larger data volume | More source rows at the normal writer geometry, with complete result consumption | Main evidence for substantial single-query work |
| Many-file compound reads | Large wide tables, extensive independently predicted file skipping, paired DV states | Main evidence for planning, skipping, and work inside surviving files |
| Opening and reuse | Open-and-query plus native reused-table sessions on the same inputs | Show how costs change when table metadata can be reused |

More SQL predicates do not automatically imply more useful pruning. Report
predicate structure, candidate files, actual rows, projection width, and physical
layout separately. A minute-long scan does not prove that overhead is negligible.

## Scale ladder and preparation

Use wide lineitem at SF1, SF10, SF30, SF100, and SF300, in that order. SF1 is the
existing baseline; the other rungs are candidates, not promises that the current
host can hold them. Generate the original source at the same scale, then apply
the existing 64-column payload rule. Preserve source values, deterministic
ordering, statistics, and pinned Parquet settings. Do not duplicate SF1 rows to
claim a larger TPC-H scale.

The normal layout retains 131,072 rows per group and eight groups per file.
Increasing rows therefore increases file count while keeping ordinary file
sizes. Record actual file sizes, row groups, page geometry, total physical bytes,
source rows, and projected logical output bytes. A TPC-H scale factor is not a
claim about the size of the wide derivative on disk.

Reuse the existing generator, external sort, repacker, DV writer, and exact
oracle. Add only the explicit scale/profile inputs and identity checks they
need. Regenerating the same small fixture must reproduce hashes; existing
smoke/development/report outputs must not change.

Preparation must have explicit finite memory, disk, and elapsed-time ceilings
before each rung starts. Account for saved source, both layouts, Delta/DV
objects, sort spill, MinIO copies, SQLite references, and one reader's complete
validation export and comparison index. Include build storage separately.
Generate and validate one fixture/case at a time, reusing immutable source data;
do not require every rung and every reader export to coexist. Retain manifests,
commands, hashes, validation records, and pilot results when retiring reproducible
bulk data. Existing unrelated data is not part of this cleanup.

The previous 192 GiB report preparation ceiling does not authorize a larger
allocation. Record and review any larger ceiling before using it. Reader limits
remain eight logical CPUs and 8 GiB, with MinIO at two CPUs and 4 GiB. A larger
disk host can keep those measured limits. A changed CPU/memory/storage environment
gets a new environment identity and a new pilot; never combine its samples with
the old one. If capacity is insufficient, record an unmet requirement and plan
the necessary environment. Do not silently reduce the workload.

## Fixed query families

Use the revision 2 projection definitions. The date equalities remain
`l_shipdate = DATE '1995-03-15'` and `l_shipmode = 'AIR'`. At each scale, derive
IN20 from the first 20 distinct ascending matching source partkeys, before any
timing. Both layouts and all readers at that scale use those same literals.

The new `date30` predicate is
`l_shipdate >= DATE '1995-03-01' AND l_shipdate < DATE '1995-03-31'`.
Keep the existing seven-day predicate unchanged. No measured query replaces
projected output with COUNT, hashing, or another aggregate.

| Larger-data cases, each on clustered and shuffled wide tables | Projection | Purpose |
| --- | --- | --- |
| Unfiltered | `wide69` | Full scan and output-throughput control |
| `date30` | `wide69` | Substantial scan with bounded selectivity; calibration anchor |
| Existing seven-day range | `wide69` | More selective range |
| Shipdate equality | `wide69` | First predicate stage |
| Shipdate and shipmode equalities | `wide69` | Second predicate stage |
| Two equalities plus IN20 | `wide69` | Compound selective read |
| Same compound predicate | `keys` | Matched narrow projection |

These are 14 no-DV cases at the selected data scale. Add two `date30` DV cases,
one per layout, with identical base Parquet bytes and the existing public
logical deletion rule. Verify at least one qualifying deletion and a surviving
qualifying row; report the changed live output volume. They test substantial
work with DVs as well as the highly selective DV cases below.

Add two formal scale controls at the immediately preceding ladder rung: shuffled
`date30`/`wide69` and clustered compound/`wide69`. These make the data-volume
change visible in the formal evidence. The same literal-selection rule applies
at both scales; report the resolved literals and actual selectivities rather
than claiming identical qualifying keys across different TPC-H scales.

Reuse sessions at the selected data scale cover the shuffled `date30` anchor
and clustered compound read, both without DVs. Keep initialization and ten
separately timed queries per independent session, using the existing native
reuse facilities. Preserve every query index rather than treating ten executions
as ten independent samples.

## Many-file pair and file-size control

Keep the original SF1 4,096-file pair in #330 as a small-file mechanism control.
For the new large pair, select the first SF10/SF30/SF100/SF300 rung satisfying
all of these rules, before reader timing:

- Exactly 4,096 clustered files with the existing ordinal repacking boundaries.
- Median physical Parquet file size at least 64 MiB; report the complete size
  distribution. This is a guard against using only tiny files, not a claim that
  64 MiB is an optimal production file size.
- At least 99% of files independently excludable by conservative Delta statistics
  for the compound IN20 predicate, with at least one qualifying live row.

Use 69 projected columns and pair ordinary and real-DV snapshots on identical
Parquet objects. Also run the same two queries/DV states at the same source scale
with the normal file boundaries. These four large cases hold logical data,
predicate, and projection constant when comparing file organizations.

Compute one shared logical deletion set for both organizations: the existing
public hash rule, the smallest nonmatching logical key in every file of either
organization, and the smallest matching key in the full source. Apply that union
to both layouts. Require a nonempty DV on every file, a deleted matching row,
and a surviving match. Save deleted logical keys and physical ordinals, physical
and live counts, and DV coverage for candidate, excluded, and matching files.
Keep physical numRecords, conservative statistics, and tightBounds valid.

The large pair's scale can differ from the data-volume scale because its
selection criterion is geometry. Record both explicitly; never compare runtimes
at different scales as a file-organization or DV overhead ratio. Add two native
reuse profiles for the large 4,096-file compound query, one per DV state.

## Pilot selection and minute-scale acceptance

Freeze these rules, the pilot budgets, and reader builds before piloting. Attempt
all five readers at each visited rung, with independent exact correctness gates.
An unsupported feature needs probe evidence. OOM, timeout, wrong results, and
missing implementation are failures, not unsupported capabilities.

For the shuffled no-DV `date30`/`wide69` anchor, run two pilot reuse sessions per
runnable reader, once in fixed reader order and once in reverse. Each session
uses the existing ten-query contract. Also record two open-and-query pilot runs
in those orders. Retain all query times, initialization, failures, and resource
observations. Pilot runs are exploratory, not formal repetitions.

Choose the first feasible rung above SF1 where at least three readers pass both
pilot sessions and the median across their per-reader query-2 medians reaches
60 seconds. Every passing reader contributes, including DAR; no reader or sample
is dropped for its speed. Do not select by DAR's speedup, the slowest reader, a
timeout duration, or the sum of repeated short queries. Save all visited rungs
and exclusion reasons. If the target is not reached within the ladder and
predeclared budgets, the minute-scale requirement remains unmet. A further rung
or changed workload needs a recorded amendment before it is tried.

Use the pilot to verify the entire proposed inventory's geometry, correctness,
resource feasibility, and expected campaign duration before freezing it. An OOM
on a materializing full-scan API stays visible; do not swap in another engine's
execution, omit the case, or tune one reader's memory to obtain a timing.

The 60-second criterion applies to the heavy anchor's reused execution. It does
not require every reader or selective query to last a minute. A query that
prunes a large table in two seconds is a valid result. Do not add sleeps, reduce
thread counts, weaken optimizations, or loop millisecond scans to meet the target.
The selected configuration is then measured again with the full frozen balanced
schedule; pilot samples never count toward formal medians. Confirmation uses
each reader's median query-2 time across independent sessions, then the median
across all readers whose full anchor schedule passed. At least three must pass
and that median must reach 60 seconds. If it misses, report the result and revise
through a new pilot and campaign instead of choosing another scale after seeing
formal speedups.

## Timing, work avoided, and publication

Reuse the existing query clocks. Open-and-query includes table opening, planning,
and complete result consumption; process startup, imports, extension loading,
oracle work, and serialization are outside that clock. Reuse reports include
initialization, each query position, initialization plus first/all queries, and
enclosing session time. Whole-process CPU/RSS and cleanup remain separate.

Record native planning/scan phases when they are available with a defined
boundary; otherwise use null with a reason. Open-and-query minus a reused query
is not an overhead measurement: caches and execution state also differ. Empty
queries and small cases are context, not a fixed cost to subtract. Metadata and
planning can themselves grow with table size and are part of the workload.

Pair timings with separately collected common I/O: log/Parquet/DV requests and
bytes, candidate/matching/touched files, selected-file sizes, qualifying/live
rows, projection width, and reliable decoded-row/group/page counters. Small
output does not imply little work; large table size does not imply much was read.
Show whether extra scale increases useful scan work, metadata work, or both.

Keep the existing fresh-client/reused-server-and-OS-cache policy. Larger-than-RAM
datasets do not make it a controlled cold-disk test. Label the storage environment
as local S3-compatible MinIO; do not generalize it to AWS S3 latency.

The inventory has the 46 existing cases plus 22 large/scale-control cases,
and three existing plus four new reuse profiles: 340 case/reader entries and
35 session/reader entries, including evidence-backed statuses. The frozen
manifest expands their exact IDs and bindings before measurement. Coverage is
checked against that manifest, not a hardcoded 46-case success condition.

Publication leads with the large-workload results and keeps the existing
mechanism results in a clearly labeled section. Ratios require matching validated
inputs and every scheduled successful sample. Preserve losses, ties, errors,
unsupported features, timing/I/O separation, and metric limitations. Existing
small campaigns may be cited with their original identities; they do not fill
revision 3 formal slots without a new validated run.

Large generation, pilots, formal measurement, and performance thresholds remain
manual. Add no CI job or step. Extend only relevant existing bounded smoke checks
when the implementation changes.

## Delivery order

Each native child of #312 is one reviewable PR, assigned to mag1cfrog:

| Order | Issue | Deliverable |
| --- | --- | --- |
| 1 | [#322](https://github.com/mag1cfrog/delta-arrow-reader/issues/322) | Complete the existing paired-DV mechanism slice |
| 2 | [#343](https://github.com/mag1cfrog/delta-arrow-reader/issues/343) | Bounded scale-ladder generation |
| 3 | [#344](https://github.com/mag1cfrog/delta-arrow-reader/issues/344) | Large query definitions, exact references, identities, and reuse |
| 4 | [#330](https://github.com/mag1cfrog/delta-arrow-reader/issues/330) | Small-file control and large same-scale file/DV pairs |
| 5 | [#345](https://github.com/mag1cfrog/delta-arrow-reader/issues/345) | Pilot evidence and concrete workload freeze |
| 6 | [#323](https://github.com/mag1cfrog/delta-arrow-reader/issues/323) | Clean-checkout reproduction and reporting tools |
| 7 | [#324](https://github.com/mag1cfrog/delta-arrow-reader/issues/324) | Formal measurement and reviewed publication |

Product fixes remain separate PRs with identified revalidation. The existing
generator, oracle, adapters, scheduler, and observer remain the implementation
base; these steps do not require a new benchmark framework.
