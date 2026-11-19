# STRING cast attribution protocol (#1385)

This reference tells maintainers how to measure the DATE and DECIMAL costs from
[#1385](https://github.com/mag1cfrog/delta-arrow-reader/issues/1385). PR 1 changes
only benchmark code and evidence. Product source is pinned to staging commit
`176a5761abe19eeb6a2058c9008d97ce1647b7ef`.

Commit this protocol, both new manifests, the shared harness changes, and the
completed untimed preflight receipt before timing. Rebuild that exact committed
source. The coordinator reviews the freeze, owns commits and PRs, and explicitly
releases the timing step. A clean working tree alone does not authorize timing.

## Workload and retained fixture

The separate [query manifest](../casts/queries.json) contains exactly three
shapes, each in `native`, `spark-off`, and `spark-on` configurations:

- `cast_strings`: the exact combined DATE/DECIMAL query from #1380.
- `cast_date`: both lineitem keys and `CAST(date_text AS DATE)`.
- `cast_decimal`: both lineitem keys and `CAST(decimal_text AS DECIMAL(15,2))`.

Keep #1380's [queries](../execution/queries.json),
[schedule](../execution/schedule.json),
[preflight receipt](../execution/preflight-receipt.json), and all its report
receipts byte-identical. The explicit `--casts-1385` selector chooses only the
new suite in the existing release execution harness and Python drivers. Its
inventory checks reject the old 18-shape schedule and concurrency; its baseline
schedule selects no profiling. The existing profile entry remains available
only for the conditional final diagnostic described below.
No #1380 schedule is rerun. Historical timing reproduction requires checkout of
#1380's frozen source; its original receipt pins the old harness hash and is
expected to reject this changed harness.

Reuse the retained SF1 `wide.clustered` snapshot and its unchanged
[fixture receipt](execution-1380-fixture.json). It has 6,001,215 genuine rows,
six Parquet files, 80 stored columns plus the virtual INT `file_bucket`, and
3,808,281,862 data bytes. Before preflight, verify the source-manifest, base-log,
derived-log and all base/snapshot Parquet SHA-256 values against that receipt.
Before timing, the driver verifies the derived log and each snapshot Parquet
checksum again. Do not regenerate a valid retained fixture.

The common native `ViewTable`, deterministic supplier registration, session
construction, UTC, reader warmup, and Spark ANSI settings are unchanged from
#1380. In particular, `date_text` and `decimal_text` are still native casts of
`l_shipdate` and `l_extendedprice` in the common view. These source-to-text casts
remain in query execution. The isolated shapes can project fewer input columns
and produce fewer output columns than the combined shape. Compare each Spark
shape with its own native reference. Their overheads are not additive and must
not be used to divide the old combined 440 ms gap arithmetically.

The retained fixture and raw #1380 evidence are under:

```text
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380
```

Use new evidence destinations under:

```text
/home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19
```

The fixture, result exports and scratch remain outside Git, within a 64 GiB
allowance. Build artifacts use only the owned cache below. Serialize all Cargo
work and all query processes; use two Cargo jobs and no other timing workers.

## Build and untimed comparison

From `crates/delta-arrow-reader-spark-sql`, build with the original #1380 Linux
release settings, including debug information. Do not add target CPU, LTO,
stripping, or other release overrides:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/casts-1385-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Resolve `BINARY` from this command's `compiler-artifact` JSON for the execution
benchmark. Record its absolute path and SHA-256, exact command, toolchain verbose
version, source commit and source-file hashes in the build receipt. Use this
release binary for the following commands from the repository root:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py --casts-1385 --check
python3 crates/delta-arrow-reader-spark-sql/benches/execution/summarize.py --casts-1385 --check
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py --casts-1385 preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19/preflight-v1
```

Preflight has nine full result exports and six native/Spark comparisons. The
existing export path adds a global sort only for these untimed comparisons.
Decode and compare every value, NULL, multiplicity and executed SQL primitive
type across unequal batch boundaries. Only string storage, metadata and
nullability normalization from #1380 are permitted. The existing Rust negative
self-check must reject changed values, NULLs, SQL types, missing rows and duplicate
rows while accepting equivalent batch boundaries and string storage.

Require identical observed Parquet path sets and delivered/range byte counts for
each native/Spark pair. Retain complete schemas, 6,001,215-row counts, IPC hashes,
raw read ranges, physical plans before the export sort, and both plan diffs for
each shape. Source counters must be final after drain and recorded range bytes
must equal concrete received-byte counters. The new driver's negative self-check
rejects changes to file identity, count and either byte count, and rejects a
matching pair whose received/range byte counts disagree internally. Stop for coordinator
handling of a failed comparison or I/O change; do not patch product code here.

Compress the full successful receipt as `casts-1385-preflight.json.gz` beside this
protocol. The separate bound receipt at `../casts/preflight-receipt.json` records
manifest, schedule, harness, driver, summarizer, protocol, binary, fixture and log
hashes, exact result counts, expected rows and expected source I/O for every
shape/configuration. Keep the IPC exports outside Git. Freeze these files and
rebuild before the coordinator releases timing.

## Once-only timing schedule

The separate [schedule](../casts/schedule.json) retains CPU affinity `2,3,4,5`,
two runtime workers, two target partitions, batch size 8,192, one warmup and three
retained iterations per fresh query/configuration process. There is no cache
flush. Retain setup RSS and process peak RSS with the existing pool tracker.

- Noise first: three alternating native A/B process pairs per shape, 18 processes.
- Main: three processes per shape/configuration, 27 processes. The schedule
  records their rotated configuration order explicitly.
- Total: 45 fresh processes, 45 separate warmups, and 135 retained iterations.
- Each query process has 180 seconds; the complete schedule has one hour.
  Preflight has 300 seconds per export, 180 seconds per comparison, and 30
  minutes overall. Failures retain partial evidence and stop the schedule.
- Single-query pool limit is 2 GiB, preflight pool limit is 4 GiB, and spill
  ceiling is 8 GiB. Address-space limits are 12 GiB and 16 GiB respectively.
- No concurrency checks, diagnostic profiles, pilot timings, or unchanged
  repetitions are part of PR 1.

Session/runtime creation, table loading with `WarmupMode::QueryPlanning`, common
view and dimension registration are outside timers. Query entry precedes logical
planning. Query completion ends at stream completion after every batch drains.
The harness separately records planning, execution first batch, query first
batch and execution completion, using the unchanged #1380 boundaries. Result
retention, plan rendering, metric reading, JSON serialization and destruction
stay outside timing intervals.

After the frozen committed rebuild and coordinator release, run once:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py --casts-1385 measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19/before-once
python3 crates/delta-arrow-reader-spark-sql/benches/execution/summarize.py --casts-1385 /home/hanbo/.local/share/delta-arrow-reader-review/casts-1385-2026-11-19/before-once crates/delta-arrow-reader-spark-sql/benches/reports
```

Each retained iteration must match its exact preflight row count, source files
and byte counts. For each metric, take the median of three iterations within
each process, then the median of three process medians. Report minimum, maximum
and IQR across process medians. For each shape, report the absolute difference
between its native noise-arm medians as the same-against-same noise floor.

A Spark shape is above the completion line only when its query-completion median
is strictly greater than 1.25 times its native median AND its absolute difference
is strictly greater than 50 ms. Equality at either boundary is below the line;
self-checks cover those boundaries. Keep the unchanged #1380 pool comparison
(>1.25 times AND >16 MiB) as a separate diagnostic, not a completion decision.

The report `casts-1385.md` gives all nine before medians, noise floors, absolute
and relative Spark costs, classifications, source I/O, executed types, and the
non-additive attribution limit. Commit the compressed raw samples, full
preflight receipt, metadata, summary/CSV files and compressed physical plans and
diffs beside it. Report which isolated cast exceeds the completion line without
claiming an exact partition of combined overhead.

## Later cast changes and final gates

PR 2 may change only the DATE function and its row parser, retaining the existing
parser for inputs its ordinary-input path does not handle. PR 3 may similarly
change DECIMAL only if its PR 1 isolated shape is above the line, or if the
combined shape remains above the line after PR 2. Do not implement either in
PR 1. Each requires its committed differential inputs, unchanged semantics in
CAST/TRY_CAST and both ANSI modes, and provenance handling from `UPSTREAM.md`.

Before each later stage's timing, freeze a reviewed schedule using the same
settings, ordering rule and repetitions for its two affected shapes: DATE plus
combined for PR 2, DECIMAL plus combined for PR 3. Each such stage is bounded to
12 noise and 18 main processes, 30 warmups and 90 retained iterations. Do not
retime the unaffected isolated shape. The report carries its latest measured
number forward with the source stage clearly marked, never calling it a fresh
after measurement. Preserve PR 1 baseline assets and give later protocol,
receipt and raw-output artifacts distinct stage paths or immutable commit links.
Final full-value/type and exact source-I/O comparisons cover
all three shapes after the last product change.

The issue permits one reviewed change per cast. If a shape still exceeds the
line after the last eligible change, stop and report the remaining gap plus one
bounded fresh profile of each remaining above-line shape/configuration (at most
six), using the original #1380 user-space settings: three executions, 199 Hz,
`cpu-clock:u`, DWARF stacks of 8,192 bytes. No second optimization round is
implied. STRING-to-TIMESTAMP is excluded; the final report records whether source
inspection shows the same parser construction pattern, without timing it.

Run the local Linux gates serially from the Spark workspace with the same owned
cache and two Cargo jobs:

```sh
export CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/casts-1385-2026-11-19
export CARGO_BUILD_JOBS=2
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 clippy --locked -p delta-arrow-reader-spark-sql --all-targets --no-deps -- -D warnings
cargo +1.96.0 test --locked --workspace
```

No existing outcome changes. The author applies Bulletproof Review to the scope,
names/callers, correctness, guard self-checks, maintainability and report claims;
a separate clean reviewer checks the concrete freeze and final evidence. The
coordinator owns that review, commits, PRs, merges and issue updates. Frontend,
Sail and dependency-fork product sources, CLI/Python activation, licenses,
#620/#1099/#626/#1340, and merging staging into `main` remain outside PR 1.
