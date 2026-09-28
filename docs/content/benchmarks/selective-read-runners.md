---
title: Run the public reader comparisons
description: Prepare the five pinned reader adapters, validate their output, and record individual streaming invocations.
---

# Run the public reader comparisons

The standalone executables implement the same request and observation
contract for the [selective-read protocol](selective-read-protocol.md).
DAR uses this checkout's DataFusion integration. delta-rs uses the released
`deltalake = "=1.0.0"` provider. Each has its own manifest and lockfile under
`benches/selective_read/runners`; delta-rs is not a library dependency.
DuckDB uses a separate Python environment and its official Delta extension.
Polars and Daft each use an isolated environment with their native Delta scans
and lazy expressions. The Python readers do not change the library's dependencies.

These commands exercise individual invocations. The campaign scheduler, fixed
CPU affinity, process memory limit, storage observations, and statistical report
are separate roadmap steps. A successful invocation is not a published speedup.
Missing external observations remain null in the record.

## Build the Rust readers

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

## Prepare DuckDB

Use CPython 3.14.6, Linux x86-64, and `uv`. From the repository root:

```sh
python3 benches/selective_read/runners/duckdb/prepare.py \
  --output ../selective-read-duckdb-build
```

The committed `duckdb/lock.json` fixes both Python packages, their wheel URLs
and SHA-256 hashes, and both external extensions. It pins DuckDB 1.5.5,
PyArrow 25.0.1, Delta source `45c40878601b54b4188b09e08732fe0d576ad222`,
and httpfs source `827222fb45a043a7a852d1f7aae46901492a3cda`. It also records
the DuckDB engine and Python binding source commits and the extension ABI.
The four built-in extensions are part of the pinned DuckDB wheel.

Preparation downloads and verifies the artifacts, creates a dedicated environment,
and writes `selective-read-duckdb` beside `build.json`. The build record includes
the adapter sources, interpreter build/ABI/hash, installed package file hashes,
wheel metadata, and every loaded extension's version. Each invocation checks
these identities and extension hashes before opening a table. Imports and local
extension loading occur before the query clock. Automatic extension installation
and loading are disabled during execution.

Keep the entire output directory at its original path; its launcher refers to
that environment. To prepare another build without downloading anything:

```sh
python3 benches/selective_read/runners/duckdb/prepare.py \
  --artifacts ../selective-read-duckdb-build/artifacts \
  --output ../selective-read-duckdb-build-copy
```

Every artifact is checked again. The extension repository URLs can change;
a different binary fails the hash check. Retain the downloaded artifacts for
reproduction instead of updating the lock during a campaign.

## Prepare Polars

Use the same CPython 3.14.6, Linux x86-64, and `uv` prerequisites:

```sh
python3 benches/selective_read/runners/polars/prepare.py \
  --output ../selective-read-polars-build
```

`polars/lock.json` pins Polars and `polars-runtime-32` 1.44.2,
Python `deltalake` 1.6.6, PyArrow 25.0.1, and all three transitive Python
dependencies. Every wheel has a fixed URL and SHA-256 hash. The build records
the same interpreter, installed-file, source, and wheel identities as DuckDB.
Use `--artifacts ../selective-read-polars-build/artifacts` with a new output
directory to prepare an offline copy. Keep each build at its original path.

The lock also records the Polars and Python deltalake source revisions. Preparation
retains and verifies deltalake's published source archive, which includes its
complete resolved Cargo lockfile. It records `buoyant_kernel` 0.28.1,
`buoyant_kernel_engine` 0.28.0, Arrow 59.3.0, and DataFusion 55.1.0.
Those versions describe the published source; wheel hashes identify the executed
binaries. Python `deltalake` 1.6.6 and the separate Rust delta-rs comparator have
different dependency sets.

## Prepare Daft

Use CPython 3.14.6, Linux x86-64, and `uv`:

```sh
python3 benches/selective_read/runners/daft/prepare.py \
  --output ../selective-read-daft-build
```

`daft/lock.json` pins Daft 0.7.25, Python `deltalake` 1.6.6, PyArrow 25.0.1,
and all five transitive Python dependencies. It records each wheel's URL and
SHA-256 hash, Daft source `50b3f1208f4d3d1ed4bf3bb1da44b31fc6d9a14f`, and
the same deltalake source archive and resolved Delta dependencies as Polars.
The shared preparation helper records and verifies interpreter, installed-file,
source, and wheel identities. Use
`--artifacts ../selective-read-daft-build/artifacts` with a new output directory
for an offline copy. Keep each build at its original path.

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
`../selective-read-delta-rs-build/selective-read-delta-rs` or
`../selective-read-duckdb-build/selective-read-duckdb` or
`../selective-read-polars-build/selective-read-polars` or
`../selective-read-daft-build/selective-read-daft` and a new output directory.
Use `li.shuffled.eq2-in20` with its own reference to check the paired layout.
Development/report fixtures contain the frozen 20-value IN list;
the smoke profile retains its documented shorter-list exception.

The helper reads the saved SQL from the fixture manifest. The Rust and DuckDB
executables register the selected snapshot as `bench` and execute that SQL.
Polars and Daft translate the scan shape to native filter, projection, and limit
expressions. No adapter supplies a preselected file list. The oracle verifies
complete values, logical types and multiplicity. A failed check produces
`validation_failed` and a nonzero exit.

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
location, snapshot, SQL, native expression where applicable, fixture manifest,
protocol and oracle. Stale or missing
certificates fail before table I/O. These identity checks do not rescan source
objects before timing; the campaign must preserve the validated immutable inputs.
Timed output counts must also match the validated counts; a difference marks the
observation as failed validation while preserving its timings.

For remote objects, `--table-uri s3://bucket/table/` changes the actual reader
location. Validate that location separately; a certificate for a local URL
cannot authorize timing a remote URL. Storage credentials stay in the reader's
environment, outside request and build records.

DuckDB reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional
`AWS_SESSION_TOKEN`, and `AWS_REGION` (falling back to `AWS_DEFAULT_REGION`,
then `us-east-1`). `AWS_ENDPOINT_URL` selects an HTTP(S) endpoint and path-style
S3 addressing, suitable for MinIO. The adapter creates an in-memory secret
before the clock; it does not save credential values. Shared remote-storage
validation and observation belong to the storage-observer slice.

Polars accepts the same AWS variables and endpoint, passes them through native
storage options, and disables automatic credential-provider discovery. Records
contain the region and endpoint but omit secret values. The local check verifies
configuration before table I/O; remote execution still needs the shared storage
observer's integration checks.

Daft accepts the same AWS variables and endpoint through its native `IOConfig`.
Unspecified credentials use the native provider chain. The adapter records the
region and endpoint without secret values. Its local check verifies configuration
before table I/O; remote execution needs the same later integration checks.

## Reuse and diagnostics

Add `--execution reuse` to validation and timing commands to retain one
snapshot/provider registration for ten queries. Validation checks all ten
exports independently. DAR uses `WarmupMode::QueryPlanning`; delta-rs loads an
`EagerSnapshot`. Both include that eager work in `initialization_ns`.

DuckDB attaches the explicit version as `bench` with `READ_ONLY` and
`PIN_SNAPSHOT true`. Since `ATTACH` is lazy, initialization also runs
`DESCRIBE bench` to load and retain the snapshot and schema. Add-action replay
and scan planning still belong to each query where the native API performs them.
Each of the ten queries creates a fresh relation on the same connection.
Open-and-query uses `PIN_SNAPSHOT false` and includes all lazy opening work in
its single interval.

Polars creates a lazy `scan_delta` source at the explicit version. Reuse
initialization calls `collect_schema()` to load and retain its native Delta
snapshot. Each query clones this source and constructs fresh filter, projection,
and limit expressions. It retains no prepared physical query plan or result.
Open-and-query includes lazy snapshot loading in its single interval.

Daft's `read_deltalake` eagerly loads the snapshot and schema. Its pinned
implementation opens the latest snapshot before loading the requested version;
both operations belong to initialization or the open-and-query interval.
Reuse retains this native Delta data source for ten newly planned queries.
Native add-action enumeration, optimization, and physical planning still occur
per query. The adapter does not retain a collected result or physical plan.

The executable accepts any supplied case in reuse mode. The report protocol
selects `reuse.li`, `reuse.wide`, and `reuse.files4096`; the last profile's
fixture is delivered by its file-organization slice. The current manifest
helper selects the original/wide public cases.

Use `--purpose diagnostic` in a separate invocation to save physical plans and
provider evidence. Its headline timers are null. delta-rs evidence checks the
built provider's pushdown and Arrow view options. DAR records its Direct
backend options; generic DataFusion Parquet settings alone do not describe
that backend's behavior.

### DuckDB execution settings

DuckDB executes canonical SQL through
`connection.sql(sql).to_arrow_reader(8192)`. A fresh relation uses the binding's
[streaming execution path](https://github.com/duckdb/duckdb-python/blob/b236c8194ed14c7a7c685e0534dde501cc855b3a/src/duckdb_py/pyrelation.cpp#L1037).
The runner consumes and drops each batch. It does not call `execute()` followed
by a conversion of a collected result. Diagnostic invocations save `EXPLAIN`
output; measured invocations do not enable profiling or export values.

| Setting | Value |
| --- | --- |
| DuckDB threads / native memory limit | 8 / 4 GiB |
| Delta executor `TOKIO_WORKER_THREADS` | 8, set before extension initialization |
| Delta attach | Explicit `VERSION`, `READ_ONLY`, filter pushdown `all`, partition-info pushdown enabled |
| Snapshot reuse | `PIN_SNAPSHOT true` for reuse, `false` for open-and-query |
| Arrow delivery | Native stream, at most 8,192 rows per batch |
| External object-data cache | `enable_external_file_cache=false` |
| Parquet metadata / HTTP metadata cache | Pinned defaults, both disabled |
| Scratch files | Build-local `spill` directory, managed by DuckDB |
| Extension installation / autoloading | Disabled; load only the verified local binaries |

Other native options retain their pinned defaults. The observation saves every
resolved `duckdb_settings()` value, provider options, and storage configuration
without secret values. DuckDB's memory setting limits its buffer manager;
the scheduler still must enforce the full process's 8 GiB limit and common CPU
affinity. Native snapshot reuse does not imply zero metadata I/O.

### Polars execution settings

The adapter accepts the public `SELECT columns FROM bench [WHERE ...] [LIMIT n]`
shape. It uses `pl.col` for the ordered projection and Polars' `sql_expr` parser
for the predicate, then applies `filter`, `select`, and `limit` in that order.
Date literals and explicit Decimal precision/scale remain native expressions.
The serialized expression tree, projection order, and limit have a separate
hash bound to the correctness certificate. Each query reconstructs these
expressions inside its measured interval.

[`scan_delta`](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_delta.html)
uses `use_pyarrow=False`, retains native predicate/projection optimizations,
and supplies both Delta add-action statistics and deletion vectors to Polars'
native reader. The harness does not enumerate or filter its files.

| Setting | Value |
| --- | --- |
| Polars compute / async workers | 8 / 8 |
| Polars maximum blocking threads | 64 |
| Delta executor `TOKIO_WORKER_THREADS` | 8 |
| Delta table options | `without_files=False`, `skip_stats=False`, `log_buffer_size=8` |
| Result delivery | `collect_batches(chunk_size=8192, maintain_order=False, lazy=False, engine="streaming")` |
| Native spill threshold | `POLARS_OOC_MEMORY_BUDGET_MB=4000`, or 4,000,000,000 bytes |
| Spill directory | Build-local `spill`, managed by Polars |
| File-cache TTL / result cache | 0 seconds / none |

The spill threshold is not a hard process limit. The scheduler must still
enforce 8 GiB and CPU affinity. Other native settings retain their pinned
defaults. The runner clears inherited `POLARS_*` overrides before importing
Polars and saves its selected environment, public configuration, optimizer flags,
and provider options. Native metadata can remain available within a session.

[`collect_batches`](https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_batches.html)
is the pinned version's streaming API, marked unstable upstream. The adapter
counts each delivered DataFrame and releases it before advancing. It never
collects the full result first. Arrow conversion/export happens only in untimed
validation; diagnostic runs save the streaming physical graph. The first-batch
timer measures the first nonempty DataFrame delivered by this native API.

### Daft execution settings

Daft accepts the same public scan shape as Polars. It uses `daft.col` for the
ordered projection and `daft.sql_expr` for the predicate, followed by `where`,
`select`, and `limit`. Date and Decimal expressions retain their native types.
The certificate binds the projection, predicate, and limit to hashes of the
native expression objects serialized with Python pickle protocol 5. The adapter
only writes this serialization for hashing; it never loads a pickle. Query
expressions are reconstructed inside the clock; serialization is outside it.

The local native runner reads through `read_deltalake` with
`ignore_deletion_vectors=False`. It receives native Delta statistics and keeps
its predicate/projection optimizations. The harness supplies no file list.

| Setting | Value |
| --- | --- |
| Native compute workers | 8 |
| Native I/O workers | `min(8, Rust available_parallelism)` |
| Compute / I/O maximum blocking threads | 1 / pinned Tokio default |
| Delta executor `TOKIO_WORKER_THREADS` | 8 |
| Scan-task parallelism / default morsel rows | 8 / 8,192 |
| Maintain output order | `False` |
| Native memory budget | `DAFT_MEMORY_LIMIT=4294967296` |
| Result delivery | `to_arrow_iter(results_buffer_size=8)` |
| Analytics / event log / dashboard | Disabled |
| Collected-result cache | None |

The pinned native runner ignores `results_buffer_size`; its own channels control
buffering. The record states this limitation. `to_arrow_iter` streams native
micro-partitions as Arrow batches without calling `collect`. The adapter consumes
and drops each batch, and measures the first nonempty batch delivered by this API.
Validation exports and diagnostic `explain` output run in untimed invocations.

The memory budget applies to Daft's native memory manager, not all allocations.
The campaign scheduler must enforce the common 8 GiB process cap and CPU affinity.
Other native options retain their pinned defaults. The adapter clears inherited
`DAFT_*` overrides before import and records resolved execution/planning options,
provider options, and selected environment values. Native metadata may remain
available within a session.

## Request and observation contract

The helper saves `request.json` and invokes the executable as:

```text
selective-read-dar REQUEST.json NEW_OUTPUT_DIRECTORY
selective-read-delta-rs REQUEST.json NEW_OUTPUT_DIRECTORY
selective-read-duckdb REQUEST.json NEW_OUTPUT_DIRECTORY
selective-read-polars REQUEST.json NEW_OUTPUT_DIRECTORY
selective-read-daft REQUEST.json NEW_OUTPUT_DIRECTORY
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
| `identity` | Reader/build/config, fixture, snapshot, case, SQL, protocol and optional native-expression hashes |
| `settings` | Native configuration, provider options, requested resource budget, table URL and streaming execution mode |
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
Rust and DuckDB explicitly shut down their runtimes/connections. Polars and Daft release
query and snapshot objects; its global native runtimes end with the process.
The campaign launcher must enforce the query and cleanup deadlines and wait for
process exit. Their `cleanup_ns` covers object release, not process teardown.

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

DuckDB's check includes the shared check above and capability probes:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/runners/duckdb/check.py \
  --binary ../selective-read-duckdb-build/selective-read-duckdb \
  --fixtures ../selective-read-smoke --output ../duckdb-contract-check
```

The probes reuse the repository's Spark-written DV fixture and saved expected
Arrow values. They distinguish snapshot 0 with DV features but no descriptors
from snapshot 1 with three actual deletions, and check predicates that match
only deleted rows or a mix of deleted and live rows. Both open and reuse modes
must return exact values and types. A no-DV copy provides the feature baseline.

An unsupported feature produces `unsupported`; a missing data object or invalid
snapshot produces `operational_failure` if execution reaches it. A reader that
first rejects DV support cannot probe a missing DV object's handling. Wrong values
or types fail independent validation. A stream with a late execution error must deliver rows before the
error, demonstrating that the adapter did not first collect the whole result.
Invalid requests and SQL with side effects are also rejected. Probe records
include the reader build and fixture hashes. They are bounded capability
evidence, not correctness certificates for campaign fixtures. Rerun probes on
the exact later DV/file-layout cases before scheduling their measurements.

Polars uses the same public-case and Delta capability checks:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/runners/polars/check.py \
  --binary ../selective-read-polars-build/selective-read-polars \
  --fixtures ../selective-read-smoke --output ../polars-contract-check
```

It also checks NULL/duplicate-IN semantics, projection order, expression hashes,
and native snapshot reuse after temporarily hiding a copied table's log.
A late invalid cast in a multi-file Delta fixture must fail after earlier
batches have arrived. Deliberately altered output must fail the independent oracle.
The fixed Polars version passes the bounded feature-only and real-DV cases,
including deleted-only and mixed live/deleted predicates, in open and reuse modes.
This does not establish support for every future campaign fixture.

Daft uses the shared public cases and records its native limitations explicitly:

```sh
PYTHONDONTWRITEBYTECODE=1 ../selective-read-oracle-venv/bin/python \
  benches/selective_read/runners/daft/check.py \
  --binary ../selective-read-daft-build/selective-read-daft \
  --fixtures ../selective-read-smoke --output ../daft-contract-check
```

The fixed Daft version rejects both feature-only and actual-DV snapshots when
`delta.enableDeletionVectors=true`. These observations are `unsupported`.
A separate metadata-only commit sets the property to `false` while retaining
the active DV descriptors. Daft then returns deleted rows. The bounded validator
records `validation_failed` against the Spark-written expected values, for
full, deleted-only, and mixed predicates in both modes. This known failure is
part of the capability evidence; it cannot authorize timing.

The check also covers exact no-DV time travel, NULL/duplicate-IN semantics,
projection order, expression-certificate binding, native reuse with the log
hidden, missing data/dependencies, and frozen storage configuration. A late date
parse error must arrive after streamed rows. No-DV public cases must pass the
independent oracle. Reprobe the exact campaign fixtures before reporting support.

These commands run locally. Adding the Python readers does not add a CI job or
production-library dependency.
