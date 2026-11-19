# String function attribution protocol (#1388, PR 1)

This reference tells maintainers how to reproduce the attribution step of
[#1388](https://github.com/mag1cfrog/delta-arrow-reader/issues/1388).
The product baseline is staging commit
`8260c58ba48e0a65ca7cb7668dd2d8c64b1434f1`. This PR changes benchmark selection,
validation and profiling only. It does not change product SQL or row kernels.

Independently review and commit this protocol, the new manifests/controller,
harness selector/profile hook and successful untimed receipt before timing.
Rebuild the exact committed source and verify the recorded source, executable
and fixture hashes. Run the fixed schedule only after those checks. A clean
working tree alone is insufficient. Do not run pilot SQL timings or repeat an
unchanged timing schedule or profile.

## Workload and immutable evidence

The separate [query manifest](../strings/queries.json) has four shapes, each on
`native`, `spark-off` and `spark-on`:

- `string_functions` is the exact combined SUBSTRING/CONCAT/UPPER query from #1380.
- `substring_only` retains both lineitem keys and `SUBSTRING(l_comment,1,12)`.
- `concat_only` retains both keys and `CONCAT(l_shipmode,':',l_shipinstruct)`.
- `upper_only` retains both keys and `UPPER(l_comment)`.

The original common native ViewTable, dimension, session configuration and UTC
setting are unchanged. `native` calls `SessionContext::sql`; the two Spark paths
call `plan_query` with the respective ANSI setting. Source-to-text work remains
in the common view where required by its existing definition. The explicit
`--strings-1388` selector chooses the new files in the existing release harness.
The new [controller](../strings/run.py) imports the original controller's process,
resource, hash, metadata and exact source-I/O primitives, and the original
summarizer's median/spread aggregation. It does not modify either old script.

Every #1380, #1385 and other historical manifest, schedule, receipt, controller,
protocol, report and compressed artifact in the 86-file historical inventory
must remain byte-identical. Their frozen receipts continue to require their
own historical source/harness. No old 18-shape or cast timing schedule runs here.
Native root/core, CLI/Python, Sail and dependency-fork product sources stay
unchanged. Only the four declared string shapes are eligible for this schedule.

Reuse the retained genuine SF1 fixture documented by
[the #1380 receipt](execution-1380-fixture.json): 6,001,215 lineitems, six unchanged
Parquet objects, 80 stored columns and the original virtual file_bucket column.
Do not regenerate it after checksums pass. Verify the source manifest, original
Delta log, derived version-0 Delta log, every Parquet SHA-256 and file size.

```text
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/base-sf1
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot
```

Use new evidence destinations below
`/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr1`.
Keep IPC exports, build logs and local gate receipts outside Git. The fixture,
exports and scratch allowance remains 64 GiB, separate from the owned build
cache. Retain partial evidence on any failure.

## Build and untimed comparison

Serialize Cargo and query work on local Linux. From the Spark workspace, use:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Release optimization remains level 3 with debug level 1. Do not supply
RUSTFLAGS, LTO, target-CPU, codegen or #1381 CLI release overrides. Resolve BINARY
from the compiler-artifact JSON and retain the exact command, working directory,
verbose toolchain, source/lock hashes, flags and executable SHA-256. Keep a
separate read-only executable copy for the final binding.

From the repository root, run the controller checks and full untimed preflight:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings/run.py --check
python3 crates/delta-arrow-reader-spark-sql/benches/strings/run.py preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr1/preflight-v3
```

The controller rejects optimized Python with an explicit early diagnostic, before
self-checks, workload loading or workflow path creation. Bounded subprocess
checks cover both -O and PYTHONOPTIMIZE.

Preflight exports all twelve complete results and compares all eight Spark
results with their corresponding native reference. Its sort exists only for
unordered result export; timed plans have no export sort. Decode every value,
NULL and row multiplicity across unequal batch boundaries. Compare executed SQL
primitive types, allowing only #1380's existing string-storage, metadata and
nullability normalization. Retain both actual Arrow storage types and SQL schemas.
The existing Rust negative self-check rejects changed values, NULLs, SQL types,
missing/duplicate rows and accepts equivalent string storage/batch boundaries.

Require 6,001,215 rows, two output partitions, final source counters, identical
observed Parquet path sets and exact delivered/range byte totals for each pair.
Each recorded range sum must equal received bytes. Retain full IPC checksums,
source ranges, pre-export-sort physical plans and both ANSI plan diffs per shape.

A confirmed value/type/count or complete source-I/O difference excludes the whole
shape, including native and both ANSI modes, from timing. Report its evidence
and reason; do not change product code or workload to obtain eligibility.
Export failures, comparator crashes/OOM/missing IPC, malformed output, incomplete
counters or invalid partition telemetry stop preflight and preserve partial
evidence. Only the legacy comparator's three exact decoded mismatch errors with
exit status 1 establish a comparison-process shape exclusion. There is no
inference of equality from IPC bytes, checksums or row counts alone.

The full compressed receipt is `strings-1388-preflight.json.gz`; it retains all
source ranges and counters. The separate
[bound receipt](../strings/preflight-receipt.json) stores only expected file
identities/counts and received/range byte totals for each shape/configuration,
alongside eligibility/exclusions,
rows and exact source I/O, executable hash, snapshot identity and hashes of the
manifest, schedule, controller, harness, original drivers, protocol, Cargo files,
fixture receipt and full compressed preflight. Commit the independently reviewed
freeze, rebuild its exact source and verify the bound hashes before timing.

## Fixed timing schedule and decisions

The [new schedule](../strings/schedule.json) keeps CPU affinity `2,3,4,5`, two
runtime workers and target partitions, batch size 8,192, one full warmup and three
retained iterations per fresh process. Do not flush the filesystem cache.

- Three alternating native A/B pairs per shape: 24 noise processes.
- Three processes per shape/configuration, with the declared rotating path order:
  36 main processes.
- Maximum total: 60 fresh processes, 60 separate warmups and 180 retained
  iterations. Excluded shapes lose all fifteen scheduled processes without
  reordering the remaining ones.
- Each process has 180 seconds and the whole schedule has one hour. Preflight
  has 300 seconds per export, 180 seconds per comparison and 30 minutes overall.
- Timing/preflight pools are 2/4 GiB, address limits are 12/16 GiB, and the spill
  ceiling is 8 GiB. No concurrency work is part of this attribution.

All existing timing and counter functions remain byte-identical. Runtime/session
creation, table loading with WarmupMode::QueryPlanning, common view and dimension
registration are outside timers. Query entry precedes logical planning; query
completion ends when every result batch has drained. Planning, first-batch and
execution completion remain separate metrics. Do not add separately aggregated
medians to invent a completion interval. Plans, counters and JSON serialization
remain outside those intervals. Track the existing actual per-iteration pool
peak and per-process setup/peak RSS.

For every metric, take each process's median of three retained iterations, then
the median of its three process medians, retaining min/max/IQR. The per-shape
completion noise floor is the absolute difference between the two native A/B
arm medians, each aggregated by the same rule. Native A/B noise describes this
campaign's repeatability, not variation between workers or dates.

For each ANSI mode, `completion_gap_exceeds_noise` is true strictly when
`spark_query_complete_ns - native_query_complete_ns > native_ab_noise_ns`.
Equality is below the line. There is no 1.25x or 50 ms gate in this suite, and
memory diagnostics do not authorize an optimization. Evaluate the unrounded
nanoseconds. UPPER is a control using the same implementation on both paths;
planning or shared work may account for an observed difference.

After the independent review, commit, exact-source rebuild and bound-hash checks,
invoke once:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings/run.py measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr1/before-once
python3 crates/delta-arrow-reader-spark-sql/benches/strings/run.py summarize /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr1/before-once crates/delta-arrow-reader-spark-sql/benches/reports
```

Every retained sample must match preflight rows, partitions, final counters and
exact source files/bytes. A failure stops without an unchanged retry. Preserve
compressed raw samples, metadata, full metric spread, summary flags, plans and
validation alongside the final report. The report gives all four shapes in all
three configurations in both ms and ns/row, with noise, deltas and each mode's
`completion_gap_exceeds_noise` value. A shape excluded before timing appears with its reason and no timing.
Isolated projection costs do not add up to the combined cost.

## Bounded query-window profiles

Take exactly one profile for each eligible shape whose
`completion_gap_exceeds_noise` value is true in either ANSI mode, at most four
profiles. Select the ANSI mode with the larger
unrounded completion delta; ties choose ANSI off. Do not profile native or run
both modes of one shape. The summary fixes this list before collection.

The strings-only profile helper reuses the existing session builder and query
measurement function. It completes one warmup with sampling disabled, sends
`enable` through perf's control FIFO, requires perf's NUL-terminated `ack\n\0`, executes exactly three
queries, then sends `disable` and requires the same acknowledgment again. It does not change normal
timing paths. Sampling covers the three calls to measure_query, including
recorder/pool resets before its timers and source evidence, operator metrics and
JSON Value construction after drain. Registration, warmup and final stdout JSON
are excluded. Attribute those outside-timer instrumentation frames separately;
their sampled cost cannot explain a timed product delta. Diagnostic sample
intervals are not additional baseline timing observations.

Use user-space `cpu-clock:u` at 199 Hz, `--user-callchains`, DWARF stack size 8,192,
`--delay=-1` and the control/ack FIFO pair. Each profile is capped at 180 seconds
and 512 MiB raw data, with 900 seconds for the entire profile/decoding stage.
The controller owns the process group, kills it on exit/error/timeout and reaps
orphaned descendants. Missing or invalid acknowledgments fail closed. Preserve
raw data, command, stderr, source-line/inline decoded stacks and symbol/source
report; no automatic retry or global perf policy change is permitted.

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/strings/run.py profile BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr1/before-once /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1388/pr1/profiles-once
```

A non-product C calibration on this worker confirmed DWARF callers and source
lines with these sampling settings. For each selected shape, connect actual
query-window stacks to the physical-plan difference and named function/source
locations. Distinguish row kernels, adapters, planning and shared work. A leaf
symbol alone does not prove a caller or explain a native/Spark delta; state
sampling/unwinding limits and unresolved causes instead of claiming causality.
No parser, product or fork optimization is part of this PR.

## Local gates and review

Run serially from the Spark workspace with the owned cache and two Cargo jobs:

```sh
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 clippy --locked -p delta-arrow-reader-spark-sql --all-targets --no-deps -- -D warnings
cargo +1.96.0 test --locked --workspace
```

Preserve all 3,167 prior named outcomes in 230 groups: 3,164 passed, zero failed
and three ignored. No existing test is edited. Keep the full named inventory
and actual command exit statuses in local evidence. Verify all 86 historical
assets and every out-of-scope source object before freezing exact hashes.
Review callers, exclusions, noise boundaries, profile cleanup and documentation.
Require an independent review of the concrete freeze before commit and timing.
PR 1 leaves all optimization decisions to the attribution report and the issue's
subsequent reviewed slices.
