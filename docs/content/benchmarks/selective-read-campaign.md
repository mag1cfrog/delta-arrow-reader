---
title: Run a selective-read campaign
description: Validate all five readers, freeze their schedule, and retain timings, failures and separate I/O diagnostics.
---

# Run a selective-read campaign

The Rust campaign controller runs the prepared readers sequentially against one dedicated
MinIO server. It checks each case and execution mode against the independent
oracle, saves the runnable subset and full schedule, then runs warmups, timing
samples and separate diagnostics. All five readers remain in the inventory.

New large workloads use [revision 4 sampling](selective-read-sampling.md):
one validation, one warmup, five independent timed samples, one plan capture
and one I/O diagnostic per runnable reader and job. Each reuse invocation
contains two queries. The `sampling_sha256` identity binds these counts through
the adapters, watchdog, certificates and report. This schedule does not run
paired tracing-overhead experiments; report that estimate as unavailable.
The revision 2 examples and historical schedules below remain reproducible.

For Q2/Q4, `xtask selective-read freeze-workload --stage formal` accepts one or
more complete no-DV/DV pairs of full fixtures. New definitions use revision 6;
revision 5 creation uses the archived harness. Each pair shares a query shape
and layout. Prepare pairs in batches to stay within the disk budget.
Probe fixtures and missing pair members are rejected.
Formal jobs use five independent samples and two queries per reuse invocation.
The workload and report retain all eight case entries, with other inputs marked
`not_prepared`. A batch report covers its selected jobs and keeps
`publication_ready: false`; publication still requires all eight cases and their
reuse profiles under the frozen conditions.

For all 30 public predicate/projection cases, use the
[query matrix guide](selective-read-matrix.md). Its `--matrix` argument supplies
the full case list and references without adding the default reuse sessions.

## Prepare the inputs

Use the Linux host, pinned builds, verified upload and oracle environment from
the [storage guide](selective-read-storage.md). Start MinIO once before uploading
the fixtures. Keep it running throughout the campaign. Compilation, fixture
generation and reference preparation must finish before timing starts.

Build the [native controller](../../../benches/selective_read/README.md#reproduce-a-run)
before starting the campaign. Its executable and Rust sources are recorded with
the campaign inputs; each watchdog result also identifies the controller binary.
Run `xtask selective-read run-campaign` with `--python PATH` for the pinned
oracle environment. Run it from the repository
root, or pass `--harness PATH` to the `benches/selective_read` directory.

Rust loads the frozen matrix or workload and binds each reader request to its
fixture and query. Python runs in one persistent validation process outside
the measured reader intervals. Its checksum and metadata caches survive across
gates. The controller retains validation errors in the campaign artifacts and stops launching readers
if an invocation cannot prove cleanup.

For the default compound-query smoke cases, prepare these references from the
repository root. Each output directory must be new:

```sh
for case in li.clustered.eq2-in20 li.shuffled.eq2-in20 wide.clustered.eq2-in20; do
  cargo +1.98.1 run --locked -p xtask -- selective-read prepare-reference \
    --python ../selective-read-oracle-venv/bin/python \
    --fixtures ../selective-read-smoke --case "$case" \
    --output "../reference-$case"
done
```

References contain complete expected values and independently evaluated file
sets. They must match the fixture manifest, snapshot, SQL and frozen protocol.
The upload receipt must belong to the current server and those same fixtures.
An upload made with `xtask selective-read upload-storage --table` covers only its declared native
tables. Select the corresponding query with `--case`; its reuse session is
included by default when using a production workload. Jobs whose tables are
absent from the upload remain `preparation_failed` and launch no readers.
Use separate verified uploads for the other snapshots without editing the
complete-pair workload definition.

For a new campaign, add `--upload-table NATIVE_TABLE_ID` and point `--upload`
to a new receipt path. Repeat `--upload-table` to stage multiple native tables.
The controller uploads them with server SHA-256 verification before creating
reader requests. Upload and campaign checks share the existing local checksum
cache in the validation process, so an unchanged file is hashed once. The cache checks
device/inode, size, mtime and ctime; changed files receive a fresh hash.
An existing receipt is rejected in this mode. Omit `--upload-table` to consume
an already verified receipt through the existing path.

Preparation stays outside the reader clocks. Every native invocation still
starts a fresh process/client/JVM. The campaign freezes the upload verification
method and receipt hash before timing; the overview retains the declared staging
method per campaign. Keep the receipt and complete run history with the results.

## Run the smoke campaign

```sh
cargo +1.98.1 run --locked -p xtask -- selective-read run-campaign \
  --python ../selective-read-oracle-venv/bin/python \
  --state ../selective-read-storage --fixtures ../selective-read-smoke \
  --upload ../selective-read-upload.json --output ../campaign-smoke-001 \
  --binary ../selective-read-dar-build/selective-read-dar \
  --binary ../selective-read-delta-rs-build/selective-read-delta-rs \
  --binary ../selective-read-duckdb-build/selective-read-duckdb \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --reference ../reference-li.clustered.eq2-in20 \
  --reference ../reference-li.shuffled.eq2-in20 \
  --reference ../reference-wide.clustered.eq2-in20 \
  --session reuse.li --session reuse.wide
```

The output directory name is the campaign ID. Use a new directory for every run;
there is no retry or resume into an existing schedule. Repeating `--case` selects
other prepared cases. The default isolated cases are the compound query on the
clustered and shuffled original tables.

The three session aliases are `reuse.li`, `reuse.wide` and `reuse.files4096`.
Omitting `--session` requests all three. Use `--no-sessions` for only isolated
cases; it cannot be combined with `--session` or `--matrix`. The 4,096-file
session needs the [file-organization fixture](selective-read-files.md).
Missing fixtures produce `preparation_failed` inventory entries and an
incomplete campaign. The example selects the two sessions in the smoke fixture.

A failed correctness gate excludes that reader only from that case and mode.
Native feature rejection is `unsupported`; a missing build, crash or malformed
record is a failure. The scheduler has no DV-specific exclusion rule: later DV
fixtures use the same gate and schedule once the common fixture/request/oracle
helpers support them.

## Read the saved results

| Artifact | Contents |
| --- | --- |
| `campaign.json` | Selected jobs, protocol and source hashes, reader build records, server identity, cache policy and timer resolution |
| `inventory.json` | Five entries per selected case/session, including gate evidence and exclusions |
| `schedule.json`, `frozen.json` | Expanded run order saved before warmups/timing, plus hashes of the frozen inputs |
| `observations.jsonl` | Every attempted or skipped scheduled slot, its request, validation prerequisite and complete observation |
| `summary.json` | Status counts, independent sample distributions, eligible speedups and separate diagnostic observations |
| Each run directory | Request, raw reader record, stdout/stderr, lifecycle progress, process resources and storage observation |
| Each `-io` sibling directory | Observer request, capture proof and sanitized S3 requests when tracing is enabled |

Cases run in lexical order, followed by sessions in lexical order. In the
historical revision 2/3 schedule, each runnable
reader warms up once in fixed order and once in reverse. The existing
`balanced_orders` helper supplies complete cycles: five, four, three, two and one
runnable readers receive 10, 16, 12, 12 and 10 independent samples respectively.
Failed slots remain in their original positions. No outlier removal or sample
replacement occurs.

Each historical reuse sample opens one process, initializes its native snapshot/session and
plans and executes the query ten times. The summary reports initialization,
each query index across independent sessions, initialization plus the first/all
queries, and enclosing session time. Ten queries within a process never become
ten independent samples. Missing first-batch times retain the adapter's reason.

Distributions use linear interpolation at `(n-1)*p` for Q1, median and Q3;
IQR is Q3 minus Q1. A comparator/DAR speedup requires both correctness gates and
every scheduled warmup and timing sample to succeed. Ties and losses remain
visible. Durations below the recorded clock resolution yield no ratio. Raw
durations and byte counts are integers; interpolated summaries can be fractional.

By default, plan export starts after the entire timing schedule. The revision 6
[gate-diagnostics amendment](selective-read-gate-diagnostics.md) instead records
I/O and four readers' plans during exact validation, retaining separate DuckDB
EXPLAIN invocations. It keeps every independent timing slot.

Historical I/O diagnostics have
one traced warmup round in fixed order and one in reverse, followed by two
measured diagnostic rounds in those orders. Each traced observation has a
separate untraced run of the same query, ordered off/on in the first round and
on/off in the second. Their median difference and ratio describe observer cost.
These two pairs are descriptive observations, separate from headline timings.

## Process and cleanup boundaries

Each invocation uses a fresh process under the shared eight-CPU, 8 GiB reader
budget. MinIO retains its two-CPU, 4 GiB budget. The supervisor and trace observer
run on the separate observer core. A campaign holds the exclusive server lock
through its last diagnostic. Server and OS caches remain in place.

A small inherited pipe carries lifecycle messages before or after query clocks,
with no per-batch messages. Initialization and each query have a 1,800-second
deadline; startup without table I/O has the same bound. Between-query bookkeeping
and final cleanup have 60-second bounds. Final cleanup includes process exit,
an empty reader cgroup and zero pending server requests. An unproven cleanup
stops further launches and records the remaining slots as `not_run`.

The watchdog retains completed-query counters and clocks if a process crashes or
times out. It cannot reconstruct rows consumed inside an interrupted query;
native partial-query records are retained when the reader managed to write them.
Oracle checking, source hashing, result serialization and reporting occur outside
measured query intervals. Oracle comparison also occurs after process/I/O cleanup.

Linux `wait4` supplies whole-process user/system CPU and peak RSS, including
startup, imports, identity checks and shutdown. These are separate from native
query timers. CPU includes waited descendants; peak RSS is the largest individual
process high-water mark, not the sum of a process tree. The pinned adapters run
their queries in one process with native threads. RSS has KiB precision and CPU
accounting has microsecond precision, stored as bytes and nanoseconds.

## Check the scheduler without a campaign

```sh
cargo test --locked -p xtask --bin xtask selective_read::
cargo test --locked -p xtask --test selective_read
```

These checks cover all reader subsets, historical counterbalancing,
five-sample ordering, two-query sessions, sampling identity rejection,
ties/losses, timer resolution, failed/missing slots and real
subprocess crashes, malformed output and phase timeouts. They also check merged
gate diagnostics and reject missing or changed native plans. They run in the
tooling CI job and require no reader builds, MinIO or Python packages.

A successful smoke campaign verifies the execution machinery. Revision 2
publication still requires the protocol's full 46-case and three-session inventory
at report scale.
Keep generated results outside Git and stop the owned MinIO service when finished.
