# Spark Delta execution measurement protocol (#1380)

Prepared before timing. Bounded fixture preparation and the full untimed
preflight are complete. The [fixture receipt](execution-1380-fixture.json),
[exact preparation commands](execution-1380-fixture-commands.json), and
[bound preflight receipt](../execution/preflight-receipt.json) record their
identities. Protocol, queries, receipts and harness must be committed before
query timing; the exact committed source must then be rebuilt.

This reference gives maintainers query-level cost evidence for the combined
Spark frontend. Product source is pinned to staging merge
`8954dcec63f934359c7bcdb74b5220c842d898e4`. No frontend, Sail, dependency-fork,
CLI, Python, or reader optimization is part of this measurement.

## Fixture and preparation bounds

Use the existing native `benches/selective_read` writer's SF1
`wide.clustered` table: all 16 TPC-H-derived lineitem fields plus its 64
unchanged nullable INT64 payloads. The optional 90-column Q4 production path
requires a separate producer/controller; the native 80-column route supplies
the same public lineitem provenance with fewer preparation steps.

SF1 supplies 6,001,215 genuine lineitems without repeated or padded rows.
The six unchanged wide Parquet files contain 3,808,281,862 bytes. They and the
narrow source fit this worker's
approximately 23 GiB of available RAM and available local disk. The complete
fixture, exact-comparison exports, and scratch allowance is 64 GiB outside Git.
Generation has a separate 32 GiB preparation-only disk ceiling, a 16 GiB address
space ceiling already enforced by the writer, a 4 GiB sort pool, and a
1,800-second deadline. Preparation is not query timing.

The existing fixture package requires Rust 1.98; this does not change the Spark
workspace's Rust 1.96.0 measurement toolchain. Serialize all Cargo work and use
two build jobs. From the repository root:

```sh
RUSTFLAGS='-C target-cpu=x86-64' \
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/fixtures \
cargo +1.98.1 build --release --locked -j2 \
  --manifest-path benches/selective_read/Cargo.toml --bin selective-read-fixtures
```

Inspect capacity before generation, using a new destination outside Git:

```sh
WRITER --profile large --scale-factor 1 --fixture wide.clustered \
  --preparation-only --sort-memory-mib 4096 --disk-limit-mib 32768 \
  --elapsed-limit-seconds 1800 --preflight --output BASE_SF1
```

After the capacity check passes, run the identical request without `--preflight`.
Retain its completed manifest, generator source/lock/executable hashes, exact
command, exit status, Parquet row counts and SHA-256 values. A failed or existing
destination is not resumed or overwritten.

### Genuine partition column and typed common inputs

The original writer's snapshot is unpartitioned. A benchmark-only preparation
step creates a new version-0 Delta log and hardlinks the unchanged Parquet
objects. It appends one virtual INT column, `file_bucket`, equal to each
clustered file's fixed path-sorted ordinal. The new log declares
`partitionColumns=["file_bucket"]` and each add action supplies its matching
partition value. This is an 81-column snapshot with 80 stored columns, not a
renamed data-column filter. Source rows and original statistics remain intact.
Retain all base/derived log and data checksums and the file-to-bucket mapping.

The [frozen manifest](../execution/queries.json) includes one common native
`ViewTable` definition over that snapshot. It derives nullable DOUBLE operands
from bounded remainders of `payload_00` and `payload_01`, three nullable Boolean
columns from independent nullable payload predicates, a nullable IN lookup, and
valid DATE/DECIMAL text from the original fields. These are actual typed nullable
view columns. The same native input plan is visible in both paths; its casts and
predicates remain in execution and in the physical-plan evidence. It is not
precomputed or silently excluded from execution cost.

A deterministic 10,000-row in-memory supplier dimension uses BIGINT keys
1..10,000, INT `region=key%5`, and STRING `label='supplier-' || key`. It is
registered outside timers with the same values for all paths.

The actual derived log SHA-256 is
`21d2257a5c6d201f74c2c3516a134095c2f4efb680595cbd56228ae6581e71cb`.
The fixture receipt records each unchanged data-file hash. On this worker,
derive and validate the snapshot with the exact commands:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/execution/prepare.py /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/base-sf1 /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot crates/delta-arrow-reader-spark-sql/benches/reports/execution-1380-fixture.json
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py preflight /home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/planning/release/deps/execution-28890602179f7ff9 /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/preflight-v1
```

These completed destinations are retained, not overwritten. Use new destinations
when reproducing preparation or an untimed preflight.

## Queries and configurations

The 18 IDs and exact native/Spark SQL are frozen in
[queries.json](../execution/queries.json). They cover every issue shape:
five-column full projection; physical partition, statistics-prunable and empty
filters; filtered ORDER BY/LIMIT; low/high-cardinality grouping with three
aggregates; COUNT(DISTINCT); partitioned ROW_NUMBER; dimension and unique-key
self joins; DECIMAL division/ROUND; STRING-to-DATE/DECIMAL casts; SUBSTRING,
CONCAT and UPPER; nullable DOUBLE NULLIF; IN10 and IN1000; and mixed AND/OR over
three nullable Boolean columns.

All paths use a session built with `configure_spark_sql`, the same registered
snapshot and common view. `native` uses `SessionContext::sql`; `spark-off` and
`spark-on` use `plan_query` and `create_physical_plan` with UTC and the respective
ANSI setting. References may use explicit equivalent SQL where dialect type
conventions require it; equivalence must be established before timing.

`nullif_nullable` returns direct-zero, column/column, computed-right, and
scalar-left NULLIF forms. Its query-level release result addresses #601 and
provides the requested #620 data point at the fixed source NULL fraction and
batch size. It does not isolate a pre-repair kernel, implement an optimization,
or complete #620's separate multi-fraction, multi-batch and checked-caller
acceptance. IN10/IN1000 address #671; the mixed Boolean shape addresses #669.

## Untimed correctness and I/O preflight

Use one result export per query/configuration: 54 executions and 36
native-versus-Spark comparisons. A preflight-only global sort gives deterministic
row order for unordered results. Canonicalization may normalize Arrow string
storage representations and schema metadata/nullability, while preserving SQL
primitive types and values. Compare every row/value across streams with unequal
batch boundaries; preserve multiplicity, NULLs, and executed SQL types. Do not
infer equality from raw IPC bytes, row counts, or hashes alone.

A small negative self-check must reject changed cells, changed SQL types,
missing rows, and duplicate rows, and accept equivalent uneven batch boundaries
and string storage. Retain sorted Arrow IPC exports outside Git, their hashes,
full schemas, row counts, comparison receipts, input NULL observations, and raw
source-read evidence. The preflight sort/export is absent from timed plans.

Before freeze, stop for coordinator handling of confirmed wrong rows/types or
extra source files/bytes. Do not patch product code, silently change an observed
workload, or open a performance issue. Legitimate dialect conventions require
an explicit equivalent reference; they do not establish a Spark defect.

Use the existing recording local object-store pattern. Clear setup reads before
query entry and retain the observed Parquet ranges. Distinct files means distinct
Parquet object paths with data reads, not OS opens, GET calls, or file tasks.
Repeated self-join reads count once as a distinct object but contribute every
actual delivered byte. Report GET/range counts and scan file-task counters
separately. Source I/O excludes the registered Delta-log/setup work and any local
DataFusion spill traffic; record plan spill metrics separately if present.

The completed [full preflight receipt](execution-1380-preflight.json.gz)
contains all 54 result exports and 36 successful decoded-value comparisons.
All source counters were final after stream drain, all observed range-byte sums
matched concrete received-byte counters, and no path opened extra files or
received extra source bytes. `partition_filter` read one of six files and
returned 1,048,576 rows in every configuration; `statistics_filter` read one
of six files and returned 2,528 rows. Their received-byte counts were 10,016,945
and 973,157 respectively in every configuration. The computed NULLIF right
operand remains `double_b + 1` in all three physical plans.

The frozen receipt binds queries, schedule, compiled harness/binary, product
baseline, fixture receipt and exact snapshot log. Before timing, verify every
immutable Parquet checksum against the fixture receipt. Every timed and
concurrent row count must match the exact preflight expectation. Audit paired
main samples' actual source files/bytes against native and stop for coordinator
handling of a newly observed increase, retaining all evidence.

## Timing boundaries and memory

Session/runtime creation, dimension registration, snapshot loading with
`WarmupMode::QueryPlanning`, and common-view registration are outside timers.
The filesystem cache is not flushed; each process has one full warmup execution
of its one query before three retained iterations.

- Query entry is immediately before native logical planning or Spark
  `plan_query` starts. Planning ends when physical planning returns.
- Execution entry is immediately before opening/draining the physical result
  stream. Execution-first-batch ends at its first yielded RecordBatch; retain
  that batch's row count. If no batch is yielded, first-batch latency is
  unavailable. A yielded empty batch retains its actual latency and zero rows.
- Query-first-batch uses query entry as its origin and is reported separately.
- Execution completion ends at stream completion after draining every batch.
- Required query completion/total uses query entry through that same stream end.
  The >1.25x and >50 ms completion threshold applies to this planning-plus-
  execution interval. Execution-only completion is a separate measure.

Drain batches promptly, count rows, and retain no result data in timed runs.
Read metrics, render plans, serialize JSON, and destroy retained plan objects
outside the timing intervals. Preserve the stream-completion event exactly;
do not sum separately aggregated medians to invent a total.

Wrap the existing memory pool with concurrency-safe grow/try_grow/shrink
tracking and record its actual maximum reservation, including brief peaks.
Serialize reservation updates so a concurrent shrink cannot hide a peak.
Reservation is distinct from resident memory and may exclude reader/kernel
allocations; report both. The pool must be empty before resetting its peak for
the next iteration. A small real-reservation self-check covers growth, shrink,
and rejected growth.

Each measured process owns exactly one query and configuration. Process peak
RSS therefore cannot contain an earlier query's high-water mark. Report the
process peak across startup, registration, warmup, and its three samples,
alongside the setup RSS. Pool peaks are per iteration; RSS is per process and
is not presented as a resettable per-sample counter. Linux reads `/proc`; Rust
bench code must still compile on macOS/Windows with unavailable RSS there.

## Fixed process and resource schedule

[schedule.json](../execution/schedule.json) fixes CPU affinity 2,3,4,5 (four
physical cores), two runtime workers/target partitions, batch size 8,192, one
warmup and three retained samples. Each process is fresh for its query/path.

- Noise first: three alternating native A/B pairs per query, 108 processes.
- Main: three processes per query/configuration, with the explicit rotated path
  order, 162 processes. Together these give 270 process records and 810 retained
  query iterations; the 270 warmups are separate.
- Each query process has a 180-second limit; the complete fixed schedule has a
  three-hour limit. The preflight has 300 seconds per query/configuration and
  one hour overall. A failure stops and preserves partial evidence; no unchanged
  schedule is rerun to chase a preferred number.
- Single-query pool limit: 2 GiB; preflight pool limit: 4 GiB; spill ceiling:
  8 GiB. Address-space ceilings are 12 GiB and 16 GiB respectively. RSS and pool
  measurements report actual use, not these limits.
- One concurrency check uses four independently planned executions of
  `group_high`, then four of `nullif_nullable`, in each configuration: six
  fresh processes total. Within each process the four executions start through
  one barrier and share the session's 8 GiB tracked pool, a four-worker runtime,
  and the same four-core affinity. Address space is capped at 20 GiB. Report
  group elapsed time, aggregate peak reservation/RSS, and each execution's row
  count. This is one bounded observation per case/path, not a throughput curve.

The source scale and counts are chosen for the worker's capacity before query
timing, not from observed speedups. Three process medians provide a bounded
local spread; no claim of a universal confidence interval is made.

## Build, aggregation, plan evidence, and profiles

From the Spark workspace, use the same release settings as #1379:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/planning \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --release --bench execution --message-format=json-render-diagnostics
```

Record full `rustc +1.96.0 --version --verbose`, Ryzen 7 8845HS CPU/kernel,
queries/schedule/fixture hashes, pinned product commit, protocol commit and
binary SHA-256. Commit protocol, queries, harness and completed fixture receipt
before any timings; rebuild that exact committed source and retain its receipt.

After coordinator inspection and the protocol commit, use the exact committed
release binary and a new once-only timing destination:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/execution/run.py measure /home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/planning/release/deps/execution-28890602179f7ff9 /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/run-once
python3 crates/delta-arrow-reader-spark-sql/benches/execution/summarize.py /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/run-once crates/delta-arrow-reader-spark-sql/benches/reports
```

For each measure, take each process's median of three samples, then the median
of the three process medians, with minimum/maximum/IQR. Process RSS is already
one peak per process. State the same-against-same noise for each query/measure.
Store compressed raw samples beside the report under `benches/reports`.

Retain each raw physical plan and a native-to-Spark text diff for both ANSI
modes, identifying operators/expressions present only in Spark. Plans are
captured before the preflight sorting wrapper and are rendered outside timers.
List every query/configuration whose query-completion total is both >1.25x and
>50 ms above native, or whose peak pool reservation is both >1.25x and >16 MiB
above native. These are decision inputs, not failures.

For each listed query/configuration, take one fresh bounded diagnostic profile
with three complete executions, using user-space CPU sampling at 199 Hz:
`perf record -e cpu-clock:u --user-callchains --call-graph dwarf,8192 -F 199`.
At most 36 outlier profiles can be selected by the frozen rule. Retain commands,
raw data and decoded stacks, and relate observed execution callers to the plan
diff without claiming causality that the single profile cannot establish.
The coordinator verified these user-only call-chain settings with a non-product
probe under unchanged perf policy. Historical #1379 profiles remain leaf-only;
no #1379 measurement or profile is repeated.

## Completion gates

The report must give every requested timing, rows, pool/RSS, distinct files,
source bytes and output partition count for every query/path, with all physical
plan diffs, the noise floor, threshold lists, the #601/#671/#669 query-level
classifications and the #620 release point. Include the bounded concurrency
results and limitations. No existing test or recorded outcome changes.

Run these local Linux gates, serialized, from the Spark workspace:

```sh
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 clippy --locked -p delta-arrow-reader-spark-sql --all-targets --no-deps -- -D warnings
cargo +1.96.0 test --locked --workspace
```

The coordinator arranges independent clean-context review and owns commits,
PR, merge, issue/hierarchy updates and cleanup. #1381 does not start in this slice.
