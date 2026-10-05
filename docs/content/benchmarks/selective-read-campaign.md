---
title: Run a selective-read campaign
description: Validate all five readers, freeze their schedule, and retain timings, failures and separate I/O diagnostics.
---

# Run a selective-read campaign

The campaign script runs the prepared readers sequentially against one dedicated
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

For Q2/Q4 revision 5, `production_workloads.py --stage formal` accepts one or
more complete no-DV/DV pairs of full fixtures. Each pair shares a query shape
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

For the default compound-query smoke cases, prepare these references from the
repository root. Each output directory must be new:

```sh
for case in li.clustered.eq2-in20 li.shuffled.eq2-in20 wide.clustered.eq2-in20; do
  ../selective-read-oracle-venv/bin/python -B benches/selective_read/oracle.py prepare \
    --fixtures ../selective-read-smoke --case "$case" \
    --output "../reference-$case"
done
```

References contain complete expected values and independently evaluated file
sets. They must match the fixture manifest, snapshot, SQL and frozen protocol.
The upload receipt must belong to the current server and those same fixtures.
An upload made with `storage.py upload --table` covers only its declared native
tables. Select the corresponding query with `--case`; its reuse session is
included by default when using a production workload. Jobs whose tables are
absent from the upload remain `preparation_failed` and launch no readers.
Use separate verified uploads for the other snapshots without editing the
complete-pair workload definition.

## Run the smoke campaign

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/campaign.py \
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

Plan export starts after the entire timing schedule. Historical I/O diagnostics have
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
python3 -B benches/selective_read/check_campaign.py
```

This bounded check covers all reader subsets, historical counterbalancing,
five-sample ordering, two-query sessions, sampling identity rejection,
ties/losses, timer resolution, failed/missing slots and real
subprocess crashes, malformed output and phase timeouts. It requires no reader
builds, MinIO or additional Python packages. Campaigns and this check are manual;
this slice adds no CI job or step.

A successful smoke campaign verifies the execution machinery. Revision 2
publication still requires the protocol's full 46-case and three-session inventory
at report scale.
Keep generated results outside Git and stop the owned MinIO service when finished.
