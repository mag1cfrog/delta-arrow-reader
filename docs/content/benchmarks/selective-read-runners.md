---
title: Run the public Rust reader comparison
description: Build the pinned DAR and delta-rs adapters, validate their output, and record individual streaming invocations.
---

# Run the public Rust reader comparison

The two standalone executables implement the same request and observation
contract for the [selective-read protocol](selective-read-protocol.md).
DAR uses this checkout's DataFusion integration. delta-rs uses the released
`deltalake = "=1.0.0"` provider. Each has its own manifest and lockfile under
`benches/selective_read/runners`; delta-rs is not a library dependency.

These commands exercise individual invocations. The campaign scheduler, fixed
CPU affinity, process memory limit, storage observations, and statistical report
are separate roadmap steps. A successful invocation is not a published speedup.
Missing external observations remain null in the record.

## Build both readers

Use Linux x86-64 and Rust 1.98.1. Run from the repository root:

```sh
python3 benches/selective_read/runners/build.py \
  --reader dar --output ../selective-read-dar-build
python3 benches/selective_read/runners/build.py \
  --reader delta-rs --output ../selective-read-delta-rs-build
```

Each destination must be new. The helper builds with the frozen release profile,
eight build jobs, `--locked`, an explicit Linux x86-64 target, and
`RUSTFLAGS=-C target-cpu=x86-64`. Keep the executable beside its `build.json`.
The runner verifies its actual executable and compiled lockfile hashes before
opening a table.

Build artifacts include the lockfile, complete Cargo metadata and enabled
features, Cargo dependency tree, source hashes, checkout commit and dirty status,
compiler/Cargo/linker versions, command, and executable hash. A source change
during compilation fails the build helper. Use a clean committed checkout for
measurement artifacts; development builds retain their dirty status.
Published delta-rs VCS metadata is retained alongside the archive checksums in
the lockfile, including any dirty flag recorded by its publisher.

## Validate a case before timing it

[Generate fixtures](selective-read-fixtures.md) and install the pinned
[oracle environment](selective-read-oracle.md). Prepare a reference using that
environment's Python:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/oracle.py prepare \
  --fixtures ../selective-read-smoke --case li.clustered.eq2-in20 \
  --output ../reference-li-clustered
```

Export the reader's complete output and validate it:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/runners/run.py \
  --binary ../selective-read-dar-build/selective-read-dar \
  --fixtures ../selective-read-smoke --case li.clustered.eq2-in20 \
  --purpose validation --reference ../reference-li-clustered \
  --output ../dar-li-clustered-validation
```

Use the same reference and case with
`../selective-read-delta-rs-build/selective-read-delta-rs` and a new output
directory. Use `li.shuffled.eq2-in20` with its own reference to check the paired
layout. Development/report fixtures contain the frozen 20-value IN list;
the smoke profile retains its documented shorter-list exception.

The helper reads the saved SQL from the fixture manifest. Both executables
register the selected snapshot as `bench` and execute that SQL without a
preselected file list. The oracle verifies complete values, logical types and
multiplicity. A failed check produces `validation_failed` and a nonzero exit.

After validation, a timing invocation consumes batches without writing,
hashing, or retaining their values:

```sh
python3 benches/selective_read/runners/run.py \
  --binary ../selective-read-dar-build/selective-read-dar \
  --fixtures ../selective-read-smoke --case li.clustered.eq2-in20 \
  --purpose timing --correctness ../dar-li-clustered-validation/correctness.json \
  --output ../dar-li-clustered-timing
```

The certificate must match the reader build, resolved configuration, table
location, snapshot, SQL, fixture manifest, protocol and oracle. Stale or missing
certificates fail before table I/O. These identity checks do not rescan source
objects before timing; the campaign must preserve the validated immutable inputs.
Timed output counts must also match the validated counts; a difference marks the
observation as failed validation while preserving its timings.

For remote objects, `--table-uri s3://bucket/table/` changes the actual reader
location. Validate that location separately; a certificate for a local URL
cannot authorize timing a remote URL. Storage credentials stay in the reader's
environment, outside request and build records.

## Reuse and diagnostics

Add `--execution reuse` to validation and timing commands to retain one
snapshot/provider registration for ten queries. Validation checks all ten
exports independently. DAR uses `WarmupMode::QueryPlanning`; delta-rs loads an
`EagerSnapshot`. Both include that eager work in `initialization_ns`.

The executable accepts any supplied case in reuse mode. The report protocol
selects `reuse.li`, `reuse.wide`, and `reuse.files4096`; the last profile's
fixture is delivered by its file-organization slice. The current manifest
helper selects the original/wide public cases.

Use `--purpose diagnostic` in a separate invocation to save physical plans and
provider evidence. Its headline timers are null. delta-rs evidence checks the
built provider's pushdown and Arrow view options. DAR records its Direct
backend options; generic DataFusion Parquet settings alone do not describe
that backend's behavior.

## Request and observation contract

The helper saves `request.json` and invokes the executable as:

```text
selective-read-dar REQUEST.json NEW_OUTPUT_DIRECTORY
selective-read-delta-rs REQUEST.json NEW_OUTPUT_DIRECTORY
```

The JSON request contains table URL, explicit snapshot, case ID, expanded SQL,
comparison revision/hash, fixture manifest hash, profile, execution mode,
purpose, resource budget, correctness-certificate path, and run identity.
Campaign ID, repetition and order are null for these standalone invocations.
The executables reject unknown fields, unknown modes, changed protocol settings,
embedded URL credentials, and SQL that performs DDL or DML.

Each output directory preserves the request, raw stdout/stderr, reader artifacts,
and `observation.json`. Validation also saves `correctness.json`. Within
`reader/`, `record.json` describes execution; each validation query has an Arrow
IPC stream and its oracle identity. The outer observation includes the subsequent
independent validation result. Existing destinations are never overwritten.

| Observation field | Meaning |
| --- | --- |
| `identity` | Reader/build/config, fixture, snapshot, case, SQL, protocol and optional native-expression hashes; shared with later Python adapters |
| `settings` | Complete resolved DataFusion/runtime options, provider options, requested resource budget, table URL and streaming execution mode |
| `status`, `failure_reason`, `phase` | Success, unsupported feature, operational failure, or failed validation, with the failure location |
| `capability` | Evidence for the requested query and snapshot; it does not claim untested feature support |
| `correctness` | Independent certificate status and artifact identity |
| `queries` | Per-query row/batch counts, completion and first nonempty batch time, and untimed artifact paths |
| `open_query_ns` | Snapshot open through complete stream consumption, including provider registration and planning |
| `initialization_ns` | Reuse snapshot load and eager provider initialization |
| `initialization_plus_query1_ns`, `initialization_plus_all_queries_ns` | Reuse totals that retain initialization cost |
| `session_elapsed_ns` | Enclosing open/reuse interval, including gaps between queries |
| `cleanup_ns` | Work after the final stream completes, including provider/runtime shutdown |
| `external_metrics`, `external_resource_limits` | Null with reasons until the storage observer and process scheduler supply them |

Times are integer nanoseconds. First-batch time is null with reason
`empty result` for an empty timed query. Validation and diagnostic invocations
leave headline timers null. Failures remain visible and cannot produce a speedup.
The executable waits for runtime cleanup before exit; the campaign launcher
must enforce the protocol's query and cleanup deadlines and wait for process exit.

## Check the adapters

Run the bounded contract check with actual public smoke fixtures:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/runners/check.py \
  --binary ../selective-read-dar-build/selective-read-dar \
  --binary ../selective-read-delta-rs-build/selective-read-delta-rs \
  --fixtures ../selective-read-smoke --output ../reader-contract-check
```

It checks both layouts, exact Decimal output, nullable wide output, empty and
LIMIT results, reuse exports, timing boundaries, diagnostics, wrong snapshots,
query/schema failures, and stale correctness identities. Run it after changing
an adapter and before collecting measurements. Ordinary library CI does not
build these standalone runners; the generator/oracle job retains its path filter.
No performance threshold is part of this check.
