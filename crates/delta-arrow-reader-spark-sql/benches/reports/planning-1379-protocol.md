# Spark planning measurement protocol (#1379)

This reference is for maintainers deciding whether to adopt the combined Spark
frontend on staging. It fixes the inputs and procedure before timings. It does
not propose an optimization or measure query execution, dependency fork cost,
build cost, the CLI, or Python integration.

## Source, host, and build

- Product source: `feat/spark-sql-staging` at
  `7e41302dd74e857373c0e993231b2a106ab27041`.
- Worker: local Linux x86_64, AMD Ryzen 7 8845HS, 8 cores / 16 threads.
- Compiler: `rustc 1.96.0 (ac68faa20 2026-05-25)`, LLVM 22.1.2.
- All benchmark processes use logical CPU 2. CPU 10 is its SMT sibling; no other
  benchmark or build will run concurrently. The observed governor is `powersave`;
  the run records the host configuration and does not alter system settings.
- The binary uses a current-thread Tokio runtime, one target partition, output
  batch size 256, one provider partition, and input batch size 32.
- Builds use two Cargo jobs and a dedicated owned target directory. Release debug
  information permits diagnostic symbol attribution; every configuration uses the
  same binary, optimization settings, and linked dependency forks.

Run from `crates/delta-arrow-reader-spark-sql`:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/measurements-2026-11-18/target/planning \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --release --bench planning --message-format=json-render-diagnostics
```

After committing the final protocol, manifests, eligibility, and harness, run
this exact build command again so the executable embeds the committed query list.
Retain a receipt tying that commit and clean source state to the executable hash.
The Cargo JSON output supplies the executable path. Record its SHA-256, the
protocol commit, the query/eligibility/schedule SHA-256 values, full compiler
version, kernel, CPU information, and generated Delta file hashes with the run.

## Frozen queries and inputs

[`../planning/queries.json`](../planning/queries.json) contains the exact SQL,
input schemas and rows, query IDs, ANSI modes, and hashes of the source manifests.
The harness reads this file at compile time. A shared input decoder from the
existing sweeps registers those recorded Arrow values without changing tests or
recorded outcomes.

There are 25 paired statements, 5 stress statements, and 279 ordinary sweep
identities. Ordinary identities remain separate even when two IDs share SQL.
Identical ANSI-off/on definitions for one ID share a single frozen statement with
both modes. Statements whose SQL changes with ANSI mode retain separate variant
IDs. Each requested `(ID, configuration)` gets an explicit included or excluded
planning record in `../planning/eligibility.jsonl` before timing.

| Ordinary collection | Frozen identities | Source definitions |
| --- | ---: | --- |
| `aggregate_query_results` | 18 | `aggregate-query-results-cases.json` |
| `aggregate_window_results` | 41 | `aggregate-window-results/cases.json` |
| `relational_queries` | 85 | 64 legacy and 21 coverage statements |
| `nested_results` | 72 | 38 coverage and 34 primitive generator statements |
| `json_hash_functions` | 63 | `json-hash-results/requests.json` |

The relational input inventory includes legacy providers and `bag_left` and
`bag_right`. The nested inventory includes coverage providers plus the original
`inputs` and `collections` generator providers. Each ordinary collection has a
separate session because unrelated sweeps reuse table names for different data.
The paired session registers the recorded relational and nested inputs it uses,
a four-row table with exactly 400 integer columns, and `delta_snapshot`.

The paired IDs cover projection, filters including NULLs, ordering and limit,
DISTINCT, global and grouped aggregates, HAVING, inner/left/three-way joins,
EXISTS, ROW_NUMBER, an aggregate window, a three-level CTE chain, a correlated
scalar subquery, UNION ALL, INTERSECT, EXCEPT, struct/map access and array values, and Delta
projection/filter/aggregate planning. Explicit casts give ROW_NUMBER, SUM, and the stress CASE the
same result-type contract in the unchanged SQL text on all four paths. Nullable
ordering keys have explicit `NULLS FIRST` on both paths.

The five stress IDs are `stress/wide_400`, `stress/case_200`, `stress/in_1000`,
`stress/join_six`, and `stress/cte_eight`. Their exact expanded SQL is frozen in
the manifest. The six-table join and eight-level CTE use the Delta snapshot.

### Deterministic local Delta snapshot

`planning prepare DELTA_ROOT` creates version 0 outside Git. It refuses an
existing destination. The fixture has one real Parquet file with 1,024 rows,
`id = 0..1023` (INT), `value = id * 3` (BIGINT), and
`label = 'group-' || (id % 8)` (STRING). The protocol and metadata use fixed IDs,
version numbers, timestamps, schema, statistics, and file names. Record the
actual generated file hashes, since writer versions can affect Parquet bytes.
No external storage, cloud data, or credentials are used.

Each paired/stress session loads that same snapshot through `DeltaTableBuilder` with
`WarmupMode::QueryPlanning`, then registers `DeltaTableProvider`. Registration,
file creation, log loading, and snapshot warmup occur outside all timers. These
measurements describe planning over an already registered, warm snapshot; they
do not measure cold metadata discovery.

## Untimed preflight and freeze boundary

Before accepting timings, run `planning preflight DELTA_ROOT` once. This mode
contains no timing samples. For each paired and stress statement, it separately
plans and executes the same SQL using native-plain, native-configured, Spark
ANSI off, and Spark ANSI on. It checks rows, multiplicity, NULLs, and executed
result types against native-plain. Ordered paired results retain order; other
results are compared as multisets. The existing schema encoder ignores
nullability/metadata and treats Arrow string storage representations as the same
SQL type; raw Arrow type names are retained in the preflight evidence.

This execution is only a correctness check, outside the measurement path. A row
or result-type difference is investigated before freeze. An intentional dialect
type convention may be expressed with the same explicit casts on both paths;
a confirmed compatibility defect is surfaced to the maintainer. No result is
silently declared equivalent or removed after timing.

The preflight also plans every ordinary statement in each applicable Spark ANSI
mode. A failure preserves the query ID, configuration, and full error as an
exclusion. A runtime-error statement that plans successfully remains included.
The planning eligibility file is frozen and committed with the harness and
protocol before the schedule starts. The final report lists all exclusions by
ANSI mode and links every included ID to its source collection.

For Delta plans, the harness checks that physical planning starts no scan
partition or file task and receives zero data-file bytes. Preflight evidence
also records executed file tasks, ranged/full GETs and bytes for comparison
across configurations. Any extra scan I/O is reported to the maintainer.

The final [untimed preflight](planning-1379-preflight.jsonl.gz) passed all 120
paired/stress comparisons (30 statements on four configurations), with no extra
Delta I/O. All 558 ordinary sweep ANSI requests planned successfully: 279 per
ANSI mode, with zero exclusions. The frozen eligibility file preserves every ID.

### Pre-freeze set-operation selection

The original `paired/intersect` and `paired/except` candidates combined
`left_inputs.k` (INT) and `right_inputs.k` (BIGINT). Rows matched, but native
planning returned INT and Spark returned BIGINT in both ANSI modes. Spark 4.2
widens INTERSECT and EXCEPT inputs to a common type in its
[WidenSetOperationTypes rule](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercionBase.scala).
This was a mixed-width candidate-contract difference, not an established Spark
compatibility defect. Before freeze, both operands received the same explicit
`CAST(k AS BIGINT) AS k` in both SQL paths. IDs and set-operation shapes stayed
in the workload, matching the existing independent `set_integer_*` references.
No new Spark execution reference was captured.

The [original untimed preflight](planning-1379-preflight-original-mixed-width.jsonl.gz)
is preserved. Its uncompressed SHA-256 is
`7a208f9edaf01056328a19387e37d51fd16d12559ad75d433fe114c8c3e40252`;
the original query manifest SHA-256 is
`703341ef9613093b6dd1c8ca9f04158fdf333564aca79b5c07fd4ea5855658ae`.
The external evidence directory retains that original manifest and full hashes.

## Configurations and phase boundaries

| Configuration | Session builder | SQL entry point |
| --- | --- | --- |
| `native-plain` | `SessionStateBuilder::new().with_default_features()` with the shared config | `SessionContext::sql` |
| `native-configured` | same builder passed through `configure_spark_sql` | `SessionContext::sql` |
| `spark-off` | same configured builder | `plan_query`, ANSI false, UTC |
| `spark-on` | same configured builder | `plan_query`, ANSI true, UTC |

Native logical time surrounds only `SessionContext::sql`. Native physical time
surrounds only the returned frame's `create_physical_plan`. Spark logical time
surrounds only `plan_query` (substitution, parsing, resolution); Spark physical
time surrounds only `SparkSqlQuery::create_physical_plan` (analysis,
optimization, physical planning, and renaming). Plans are dropped after both
timers stop. Scan-metric inspection and JSON output are outside timers.

The `measure` path never calls execute or collect. Each query has two warmup
iterations followed by nine retained iterations. Each iteration records logical,
physical, and their sum as total nanoseconds. Query order is the frozen manifest
order. There is no query shuffling, adaptive repetition, or early stop based on a
measured number.

## Bounded fresh-process schedule

[`../planning/schedule.json`](../planning/schedule.json) fixes all 30 processes,
CPU affinity, warmups, samples, and a 300-second timeout per process.
`../planning/run.py` runs it once from a clean, committed tree, refusing an
existing output destination. A failed process stops the schedule and preserves
its logs; an unchanged schedule is not repeated to obtain a preferred number.

1. Noise control: five fresh native-configured A/B pairs over paired and stress
   statements. Pair order alternates A/B and B/A. Both arms use the same binary,
   configuration, query text, inputs, warmup count, and sample count.
2. Main: five fresh processes per configuration in the explicit rotating and
   reversing order in the schedule. Native-plain times the 25 paired queries;
   native-configured also times the five stress shapes; each Spark configuration
   times paired, stress, and all included ordinary statements for its ANSI mode.

Invoke only after the protocol/harness/query/eligibility commit:

```sh
python3 benches/planning/run.py --check
python3 benches/planning/run.py BINARY NEW_RUN_DIRECTORY DELTA_ROOT
```

The script saves each process's JSON lines and stderr, a metadata record, a
completion record, and deterministic gzip-compressed combined raw samples. The
compressed samples are committed beside the report. Retain preflight and full
build/validation/profile logs under the external evidence directory.

## Aggregation, thresholds, and diagnostic profiles

For each query/configuration/phase, take the median of nine samples in each
process, then the median of the five process medians. Report milliseconds per
query with the process-median minimum, maximum, and interquartile range. Compute
total medians from each iteration's logical-plus-physical samples; do not add
separately aggregated phase medians. The noise table applies the same aggregation
to A and B, giving the absolute difference and ratio for every query and phase,
plus the median, 95th percentile, and maximum absolute difference across cases.
These observed controls describe this worker's noise, not a universal error bar.

List every paired query, separately for each Spark ANSI mode, whose total is both
more than 3 times and more than 2 ms above native-configured. List every ordinary
or stress Spark case with total above 20 ms, and every paired query where
native-configured is both more than 10% and more than 0.1 ms above native-plain.
These are maintainer decision inputs, not test failures.

Choose the five slowest distinct Spark query identities by total median, using
the slower ANSI mode for each identity. Name the larger phase and make one
bounded diagnostic profile per selected identity: 100 repetitions of that phase,
without query execution. `perf record` uses user-space `cpu-clock` sampling,
999 Hz, and leaf instruction samples. Release debug information supplies
function names. User-space leaf sampling succeeded in the capability check; both
DWARF and frame-pointer call graphs were denied by the current perf policy. No
sysctl was changed. Attribution is limited to rules/functions visible as sampled
leaves in that one profile; it cannot establish a complete caller chain.
Profiles include session registration and snapshot setup before the repeated
phase. Shared allocator, registration, and setup leaves are not evidence that
a particular rule caused the phase cost. When the sampled function cannot be
separated from setup or other callers, the report leaves attribution uncertain. Do not rerun a profile to chase
an attribution or change product/dependency code.

The final report includes protocol/source provenance, noise, phase tables, all
threshold lists, five distinct slow-case attributions, and limitations. Complete
these local Linux checks from the Spark workspace, then obtain an independent
clean-context review before requesting a PR into
`feat/spark-sql-staging`. Existing tests and their recorded outcomes stay intact.

```sh
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 clippy --locked -p delta-arrow-reader-spark-sql --all-targets --no-deps -- -D warnings
cargo +1.96.0 test --locked --workspace
```

Here the root package is `delta-arrow-reader-spark-sql`; no separate main
workspace, CLI, or Python activation is part of this slice.
