---
title: Compare predicates and projection width
description: Run all 30 public query cases across five pinned readers and retain correctness, selectivity, timing and I/O evidence.
---

# Compare predicates and projection width

These queries are mechanism and throughput controls. The
[Q2/Q4-derived workloads](selective-read-production-shapes.md) define the main
comparison of selective reads across thousands of files and wide stored schemas.
Completing this matrix does not complete that main workload.

The public query matrix compares compound predicates and projection width on
the same generated tables. It contains 30 cases, each accounting for
delta-arrow-reader, delta-rs, DuckDB, Polars and Daft. All tables in this slice
have no deletion vectors. DV and file-organization cases remain part of the
[full comparison protocol](selective-read-protocol.md).

These are TPC-H-derived scans. The Q6-derived case keeps the date, Decimal and
quantity predicates but projects rows without the revenue aggregation. The
wide derivative adds 64 deterministic integer payload columns. Neither is an
official TPC-H query suite or score.

## Cases and scales

Each row below runs on both clustered and shuffled layouts:

| Table | Query shapes | Output |
| --- | --- | --- |
| Original lineitem | `all-keys`, `all-full`, `empty` | Two keys, all 16 columns, or an empty 16-column result |
| Original lineitem | `date7-full`, `date7-keys`, `date7-limit` | Seven-day range with 16 columns, two keys, or at most 100 rows |
| Original lineitem | `q6-scan`, `eq2-in1`, `eq2-in20` | Q6-derived scan or date/shipmode equalities plus partkey membership |
| Wide lineitem | `eq1`, `eq2`, `eq2-in1`, `eq2-in20` | Date equality, then shipmode equality, then 1 or 20 membership values; 69 columns |
| Wide lineitem | `eq2-in20-keys`, `all-wide` | Same compound predicate with two keys, or an unfiltered 69-column scan |

The narrow and wide compound queries use the same physical wide table. Their
only difference is projection: the three predicate columns stay hidden in the
two-key output. The oracle checks the ordered output schema and every value.
An unordered LIMIT may return any qualifying rows up to its limit, with valid
values and multiplicities. Empty output must still carry the correct schema.

Use `smoke` (SF0.01) to check the machinery, `development` (SF1) for validation,
and `report` for SF10 original tables plus SF1 wide tables. Smoke has only five
qualifying distinct partkeys, so its `in20` cases use all five. SF1 and SF10
use 20. Smoke's one-file tables cannot demonstrate pruning across many files.

[`query-matrix.json`](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/query-matrix.json)
checks in every canonical SQL statement, the three scales' frozen membership
values, generator provenance and Polars/Daft expression hashes.
`benches/selective_read/native-expressions.jsonl` contains the full native
expression identities, one JSON record per distinct reader/expression hash. The native
translations are constructed by the pinned adapters from these SQL statements
as filter, projection, then optional limit. Each real validation run also
preserves its native expressions. A campaign rejects a successful observation whose executed
translation hash differs from the checked-in query.

## Prepare and run all 150 entries

Follow the [fixture](selective-read-fixtures.md),
[reader](selective-read-runners.md) and [storage](selective-read-storage.md)
guides first. Use the oracle environment and new output directories. Prepare
all references before starting a timed campaign:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/matrix.py prepare \
  --fixtures ../selective-read-smoke --output ../matrix-smoke
```

Preparation independently scans the source and fixture for every case. It saves
the exact reference result, predicate-step counts, conservative candidate files
and actual matching files. To reuse references produced by the current oracle,
pass `--references PATH`, containing `CASE_ID/reference.json` directories.
Changed SQL, literals, source counts, reference hashes or protocol identities
fail preparation. A failed case remains in `matrix.json` with its reason.

Start MinIO and upload these same fixtures using the storage guide, then run:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/campaign.py \
  --state ../selective-read-storage --fixtures ../selective-read-smoke \
  --upload ../selective-read-upload.json --matrix ../matrix-smoke/matrix.json \
  --binary ../selective-read-dar-build/selective-read-dar \
  --binary ../selective-read-delta-rs-build/selective-read-delta-rs \
  --binary ../selective-read-duckdb-build/selective-read-duckdb \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --output ../campaign-matrix-smoke
../selective-read-oracle-venv/bin/python -B benches/selective_read/matrix.py report \
  --campaign ../campaign-matrix-smoke --output ../matrix-smoke-report
```

`--matrix` selects all 30 isolated cases and their references. It cannot be
combined with case, reference or session overrides. Reuse sessions are separate
campaign jobs described in the [campaign guide](selective-read-campaign.md).
Every reader must pass an independent correctness gate before its case can
enter the frozen schedule. Five runnable readers produce 150 gates, 300 warmups,
1,500 timed samples, 150 plan runs, 300 diagnostic warmups and 600 paired
traced/untraced observations. All invocations run sequentially.

The same commands accept development or report fixtures. Finish compilation,
fixture generation and reference preparation before timing. Stop the owned
MinIO service when finished. All commands here are manual; they add no CI work.

## Read the report

`matrix-report.json` and `matrix-report.csv` retain 150 entries, including
preparation failures, unsupported readers, failed samples, ties and losses.
The exporter verifies the frozen campaign inputs and recomputes summary values
from raw observations before writing the report. It never substitutes a failed
sample or derives a speedup from an incomplete timing series.

Each entry includes predicate columns, projected column count, layout, actual
qualifying/output rows and selectivity. JSON retains the ordered predicate-step
counts and exact oracle candidate/matching file sets. These independently
computed sets describe the data. They are not the engine's planned file count.
Native plans are linked as artifacts; a normalized planned-file count is null
with an explanation because the current adapters do not export that counter.

Timing distributions and comparator/DAR ratios come from the untraced campaign.
Separate diagnostics record distinct touched Parquet objects, response-body
bytes accepted by MinIO, requests and observer overhead. The CSV keeps each
successful diagnostic's totals as arrays; JSON also retains every diagnostic's
run ID and failure status. A touched file can mean only a footer request.
Consult the [storage guide](selective-read-storage.md) before interpreting bytes
or counting a touched object as a full file read.

This 30-case report is one part of the protocol's 46-case publication inventory.
It does not reproduce private historical data or establish the old speedups.

## Check or regenerate the catalog

Run the bounded matrix check against a prepared smoke matrix:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/check_matrix.py \
  --fixtures ../selective-read-smoke --matrix ../matrix-smoke/matrix.json
```

It checks all SQL/literal scales, paired projections, translation binding,
changed metadata rejection and a synthetic 150-entry report containing failed
and unsupported cases. Real engine correctness still requires the campaign.

To regenerate the catalog deliberately, supply all three fixture profiles and
the pinned Polars and Daft executables:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/matrix.py freeze \
  --fixtures ../selective-read-smoke --fixtures ../selective-read-development \
  --fixtures ../selective-read-report \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --output ../matrix-catalog
```

Review the generated `query-matrix.json` and `native-expressions.jsonl` before
replacing the checked-in copies.
The command checks generator SQL against the independent oracle and constructs
expressions in each adapter's own pinned environment. Keep generated data,
references and campaign outputs outside Git.

## Large-workload candidates

The large workload adds the cases from the
[large-workload amendment](selective-read-large-workloads.md). Its workload
manifest is separate from the checked-in revision 2 catalog. New manifests use
revision 4 and the [reduced sampling contract](selective-read-sampling.md).

The owner selected SF10 for the main large-data family after the SF1/SF10
screen in [#345](https://github.com/mag1cfrog/delta-arrow-reader/issues/345).
Keep SF1 as the scale control. The shuffled SF10 anchor contains 59,986,052
rows and 35.95 GiB of Parquet; the five readers' two-sample median times ranged
from 22.46 to 106.94 seconds. This decision replaces the earlier automatic
scale escalation and 60-second acceptance rule. Report it as an owner-selected
scale after calibration, not as a pass of that historical rule. First and
reused execution still need separate measurements. The many-file geometry
requirements and complete publication inventory remain unresolved in #345;
this decision does not select or allocate a larger source scale for them.

| IDs | Inputs |
| --- | --- |
| `large.wide.{clustered,shuffled}.{all-wide,date30-wide,date7-wide,eq1,eq2,eq2-in20,eq2-in20-keys}` | Seven shapes on each layout at one explicit source scale |
| `large.wide.{clustered,shuffled}.date30-wide.dv` | Identical base Parquet bytes with real deletion vectors |
| `scale-control.wide.shuffled.date30-wide`, `scale-control.wide.clustered.eq2-in20` | The immediately preceding scale rung |
| `reuse.large.date30`, `reuse.large.compound` | Shuffled date30 and clustered compound, respectively; initialization followed by two newly planned queries |

All shapes except `eq2-in20-keys` project the same 69 columns. `date30` covers
March 1 through March 30, 1995; `date7` retains March 15 through March 21.
The equality literals are unchanged. IN20 is resolved separately at each scale
from the first 20 distinct ascending source partkeys matching both equalities.
The oracle verifies the literals and reports actual selectivity.

Generate one wide layout at a time with the large fixture profile. To prepare
its DV counterpart, reuse the existing writer with an explicit table selection:

```sh
target/selective-read/release/selective-read-fixtures \
  --profile large --scale-factor 10 --fixture wide.shuffled \
  --dv-from ../large-data-shuffled --dv-table wide.shuffled \
  --disk-limit-mib 196608 --elapsed-limit-seconds 1800 \
  --output ../large-data-shuffled-dv
```

The example uses the selected SF10 data scale. Its ceilings do not establish
that the host has enough capacity. Preflight rejects inadequate
space before copying. Prepare the clustered counterpart separately. Each new
DV directory contains the source, base snapshot, and paired DV snapshot; the
input directory stays immutable. Do the same preparation at the preceding rung
for the two controls, which do not need DVs.

To calibrate scale first, bind only the shuffled `date30` anchor with
`--family pilot`, one `--fixtures` directory and no `--control-fixtures`.
This accepts the SF1 baseline and the declared larger rungs. For example:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/large_workloads.py \
  --family pilot --fixtures ../large-wide-shuffled-sf1 \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --disk-limit-mib 8192 --elapsed-limit-seconds 1800 \
  --output ../pilot-workload-sf1
```

The resulting manifest has `scope: pilot` and `publication_ready: false`.
It preserves the same SQL, five readers, native expression identities and exact
oracle checks. It needs no DV copy, other layout or 4,096-file pair to measure
this one query. Choose oracle ceilings from the expected result and record them
before each rung; the values above describe the SF1 preparation example.

For another declared no-DV data query, add `--case`, for example
`--case large.wide.clustered.eq2-in20` or
`--case large.wide.clustered.eq2-in20-keys`, with its matching fixture directory.
Each pilot manifest binds one query and only its declared reuse profile, if any.
These pilots check individual workloads before the full inventory is ready;
they retain `publication_ready: false` and cannot replace the complete campaign.

The [sampling amendment in #345](https://github.com/mag1cfrog/delta-arrow-reader/issues/345)
starts screening with two independent single queries per reader, with correctness
and I/O checks outside timing. Use the declared standalone invocations from the
[storage guide](selective-read-storage.md) for screening. The revision 4
campaign below uses five independent samples per case or session, and two
queries per reuse session, bound by `sampling_sha256`. Screening observations
cannot fill formal measurement slots or freeze the full publication inventory.

For the complete large-data family, bind all 18 cases before preparing its
references or timing readers:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/large_workloads.py \
  --fixtures ../large-data-clustered-dv --fixtures ../large-data-shuffled-dv \
  --control-fixtures ../large-control-clustered \
  --control-fixtures ../large-control-shuffled \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --disk-limit-mib 196608 --elapsed-limit-seconds 1800 \
  --output ../large-workload
```

`workload.json` freezes fixture hashes, scales, geometry, expanded SQL,
projections, literals, native translations and oracle ceilings. It binds the
amendment hash, sampling hash, base protocol hash and harness sources. Each reference, reader
identity, correctness certificate, campaign, schedule slot and report carries
that workload file's hash. Mismatches fail validation. Rebuild all five adapters
after changing the harness or oracle.

The command produces a **candidate**, with `publication_ready: false`. The full
inventory including many-file cases, reader and environment bindings, and
first-versus-reused execution measurements belong to the pilot in
[#345](https://github.com/mag1cfrog/delta-arrow-reader/issues/345). This command
cannot declare a formal publication workload.

For a bounded implementation check, add `--smoke` and use SF0.01 fixtures for
both roles. Create the two small DV pairs with `--profile smoke --dv-from ...
--dv-table wide.clustered` and its shuffled equivalent. Use smaller explicit
oracle ceilings. The two smoke control IDs exercise binding only; identical
SF0.01 inputs cannot measure a scale effect.

Prepare one case at a time:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/oracle.py prepare \
  --workload ../large-workload/workload.json \
  --fixtures ../large-data-shuffled-dv --case large.wide.shuffled.date30-wide \
  --output ../large-date30-reference
```

Use the existing campaign command with the selected fixture's upload receipt,
all five binaries, and these additional arguments:

```sh
--workload ../large-workload/workload.json \
--case large.wide.shuffled.date30-wide \
--reference ../large-date30-reference --session reuse.large.date30
```

A campaign uses one staged fixture directory. `--case` selects its jobs; without
it, all workload cases bound to that directory are selected. Matching new reuse
sessions are included by default; `--no-sessions` disables them. Revision 2
session defaults remain unchanged. Supply references for every selected case.
Keep five explicit reader statuses, including native DV rejections and failures.

Export a checked report with:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/large_workloads.py report \
  --campaign ../large-date30-campaign --output ../large-date30-report
```

The exporter verifies identities and ordering, then recomputes summaries from
raw observations. It retains both query positions across independent reuse
sessions, initialization, enclosing session time and failure statuses. Separate
native planning/scan clocks are null with a reason where unavailable. No fixed
overhead is inferred by subtracting cached execution from open-and-query time.
Large generation and performance runs remain manual; no new CI job or step is
required.
