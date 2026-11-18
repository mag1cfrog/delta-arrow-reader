# Spark planning measurements (#1379)

None of the 25 paired queries crossed the combined Spark/native-configured or
native-configured/native-plain reporting thresholds on this frozen workload.
No ordinary or stress Spark query had a total median above 20 ms. The slowest
case was the 400-column projection at 18.8742 ms in Spark ANSI off. These are
observations on one worker and registered snapshot, not a performance guarantee.

## Provenance and reproducibility

- Product source: `7e41302dd74e857373c0e993231b2a106ab27041` on
  `feat/spark-sql-staging`.
- Protocol/harness/query freeze: `1896475c4ac7788d57b2b41ff5686a45d9aa3fc3`,
  committed before the timing run. The exact committed source was rebuilt before
  that run, with a clean worktree.
- One release binary, SHA-256
  `c0f8558411a866b477633c4fdb688e219b7c88a00a11fed1ebbf94a5398de1cc`.
- Rust 1.96.0 (`ac68faa20`, 2026-05-25), LLVM 22.1.2; local Linux
  `7.2.14-100.fc43.x86_64`, AMD Ryzen 7 8845HS.
- Logical CPU 2, governor `powersave`, current-thread Tokio runtime, one target
  partition, one provider partition, input batch size 32, output batch size 256.
  Blocking planning tasks inherit the same CPU affinity. No competing Cargo or
  benchmark job ran during the schedule.

The [committed protocol](planning-1379-protocol.md) gives the exact release build
command, phase boundaries, snapshot creation, counts, process order, and
preflight selection note. [Metadata and build receipt](planning-1379-metadata.json)
include compiler/kernel/CPU details, source and binary hashes, generated Delta
file hashes, the complete process schedule, and the committed-source build receipt.

The local Delta table is version 0 with one real Parquet file and 1,024 fixed
rows. Each paired/stress session registers `DeltaTableProvider` over that snapshot. Creation,
registration, log loading, and `WarmupMode::QueryPlanning` snapshot loading occur
outside timers. No cloud data or credentials are used.

## Correctness and coverage

The [final untimed preflight](planning-1379-preflight.jsonl.gz) captured 120
paired/stress results: 30 statements through both native sessions and both Spark
ANSI modes. Every statement matched across configurations in rows, multiplicity,
NULLs, and executed SQL types;
raw Arrow type names and Delta I/O counters remain in the evidence. It observed
no extra file tasks, GETs, or bytes relative to native-plain. The two set-operation
candidates use identical explicit BIGINT casts on both operands so their mixed
INT/BIGINT source keys have the same result-type contract on both SQL paths.
The [selection note](planning-1379-protocol.md#pre-freeze-set-operation-selection)
preserves the original untimed observation and its source-based classification;
no new executed Apache Spark reference is claimed.

All ordinary sweep requests planned successfully. There are zero exclusions in
either ANSI mode. The [frozen eligibility inventory](../planning/eligibility.jsonl)
retains every ID and configuration; the [query manifest](../planning/queries.json)
contains unchanged source SQL, source IDs/manifests/hashes, and input values.

| Ordinary collection | Identities | ANSI-off included | ANSI-on included | Excluded |
| --- | ---: | ---: | ---: | ---: |
| `aggregate_query_results` | 18 | 18 | 18 | 0 |
| `aggregate_window_results` | 41 | 41 | 41 | 0 |
| `relational_queries` (legacy + coverage) | 85 | 85 | 85 | 0 |
| `nested_results` (coverage + primitive generators) | 72 | 72 | 72 | 0 |
| `json_hash_functions` | 63 | 63 | 63 | 0 |
| Total | 279 | 279 | 279 | 0 |

The paired set has 25 same-text statements and the stress set has five shapes:
400 columns, 200 CASE branches, 1,000 IN literals, a six-table join, and eight
syntactically nested WITH levels. The paired set covers every shape requested
by the issue, including real Delta projection, filter, and aggregate planning.

## Method and noise

The bounded schedule ran once: ten alternating native-configured noise processes
(five A/B pairs), then five fresh processes for each of native-plain,
native-configured, Spark ANSI off, and Spark ANSI on in the committed order.
Every query in a process had two warmups and nine retained planning iterations.
All 30 processes completed, yielding 3,665 raw records and 32,985 retained
query-planning iterations. No unchanged timing schedule was repeated.

Logical and physical API calls were timed separately. No timed path executed or
collected a query. Delta scan metrics also checked that planning started no scan
partition or file task and received no data-file bytes. The separate correctness
preflight is the only benchmark mode that collects query rows.

Each reported value is the median of five process medians. The full phase data
also gives the minimum, maximum, and interquartile range of those five medians.
Total samples are formed by adding logical and physical time within each
iteration before taking medians. They are not sums of separately aggregated
phase medians.

The following noise values summarize the absolute A/B median difference over
30 paired/stress control identities, in milliseconds. The complete
[noise table](planning-1379-noise.csv) gives each query and phase's A/B medians,
ratio, difference, and process-median ranges.

| Phase | Median absolute difference | 95th percentile | Maximum |
| --- | ---: | ---: | ---: |
| logical | 0.000496 | 0.005180 | 0.008125 |
| physical | 0.001483 | 0.010192 | 0.017513 |
| total | 0.001503 | 0.005832 | 0.043821 |

The maximum in each phase came from `stress/case_200`; its total B/A ratio was
0.99236. These are observed same-configuration controls on this worker, not a
universal error bar or confidence interval.

## Phase results and reporting thresholds

The [complete phase table](planning-1379-results.csv) is the report's data
appendix: 673 query/configuration rows, each with logical, physical, and total
median/minimum/maximum/IQR columns in milliseconds. It contains every paired,
stress, and included ordinary query. The [compressed raw samples](planning-1379-samples.jsonl.gz)
retain all nine logical/physical/paired-total samples for each of 3,665 process
records. The [machine-readable summary](planning-1379-summary.json) retains the
threshold lists and selected diagnostic cases.

| Required list | Cases checked | Cases listed |
| --- | ---: | ---: |
| Paired Spark total >3x native-configured and >2 ms higher | 50 | 0 |
| Ordinary/stress Spark total >20 ms | 568 | 0 |
| Paired native-configured total >10% and >0.1 ms above native-plain | 25 | 0 |

There are no query IDs to list under these thresholds. The largest observed absolute
native-configured increase over native-plain was `correlated_scalar`:
0.609421 to 0.661599 ms, an increase of 0.052178 ms (8.56%).
The slowest paired Spark total was the same query in ANSI off at 1.298371 ms.
Thresholds are maintainer decision inputs, not pass/fail compatibility tests.

For comparison across the 25 paired statements, these are total medians in
milliseconds. Every phase and its spread remains in the complete phase table.

| Paired ID | Native plain | Native configured | Spark ANSI off | Spark ANSI on |
| --- | ---: | ---: | ---: | ---: |
| `projection` | 0.0963 | 0.1076 | 0.3394 | 0.3369 |
| `filter` | 0.1873 | 0.2042 | 0.4868 | 0.4830 |
| `null_filter` | 0.1669 | 0.1810 | 0.4368 | 0.4368 |
| `ordered_limit` | 0.2009 | 0.2163 | 0.4982 | 0.4992 |
| `distinct` | 0.1516 | 0.1632 | 0.3409 | 0.3406 |
| `global_aggregates` | 0.4597 | 0.4359 | 0.8680 | 0.8607 |
| `grouped_aggregates` | 0.4790 | 0.4505 | 0.9272 | 0.9200 |
| `having` | 0.4089 | 0.3952 | 0.8569 | 0.8506 |
| `inner_join` | 0.3525 | 0.3813 | 0.7656 | 0.7616 |
| `three_way_join` | 0.5152 | 0.5656 | 1.1213 | 1.1122 |
| `left_join` | 0.3355 | 0.3687 | 0.7370 | 0.7329 |
| `exists` | 0.3398 | 0.3637 | 0.7836 | 0.7818 |
| `row_number` | 0.3501 | 0.3522 | 0.7042 | 0.6964 |
| `window_sum` | 0.3520 | 0.3546 | 0.7057 | 0.7051 |
| `cte_chain` | 0.4971 | 0.5228 | 1.2064 | 1.1950 |
| `correlated_scalar` | 0.6094 | 0.6616 | 1.2984 | 1.2794 |
| `union_all` | 0.2104 | 0.2046 | 0.5323 | 0.5251 |
| `intersect` | 0.3520 | 0.3902 | 0.7392 | 0.7282 |
| `except` | 0.3418 | 0.3804 | 0.7179 | 0.7076 |
| `struct_access` | 0.2810 | 0.2972 | 0.5560 | 0.5537 |
| `array_projection` | 0.0964 | 0.1073 | 0.3588 | 0.3532 |
| `map_access` | 0.2278 | 0.2407 | 0.4831 | 0.4843 |
| `delta_projection` | 0.1880 | 0.1962 | 0.5055 | 0.5049 |
| `delta_filter` | 0.3130 | 0.3314 | 0.6892 | 0.6841 |
| `delta_aggregate` | 0.4089 | 0.4312 | 0.7461 | 0.7367 |

## Five slowest distinct Spark cases

Each identity appears once, using its slower ANSI mode. Cells show the phase or
total median followed by the minimum and maximum process medians, in ms. The
complete phase table supplies IQRs as well.

| Query ID | Configuration | Logical ms [min, max] | Physical ms [min, max] | Total ms [min, max] | Larger phase |
| --- | --- | ---: | ---: | ---: | --- |
| `stress/wide_400` | spark-off | 3.1092 [2.9939, 3.1323] | 15.7486 [15.6546, 15.8381] | 18.8742 [18.6485, 18.9762] | physical |
| `stress/in_1000` | spark-off | 10.7652 [10.6454, 10.8161] | 4.6812 [4.6051, 4.8900] | 15.4780 [15.2791, 15.6551] | logical |
| `stress/case_200` | spark-off | 9.1807 [9.0110, 9.2505] | 5.2122 [5.1982, 5.2794] | 14.4105 [14.2818, 14.4448] | logical |
| `sweep/nested_results/array_literal_access` | spark-on | 1.0262 [1.0177, 1.0365] | 2.5347 [2.5212, 2.5723] | 3.5682 [3.5514, 3.5967] | physical |
| `sweep/nested_results/nested_array_struct_access` | spark-on | 0.4511 [0.4458, 0.4596] | 1.9629 [1.9497, 2.0309] | 2.4052 [2.3979, 2.4852] | physical |

One user-space `cpu-clock` leaf profile was taken per selected phase, with
100 repetitions at 999 Hz using the same binary and CPU affinity. No query was
executed. [Profile commands, raw perf data, reports, and stderr](planning-1379-profiles.tar.gz)
are retained together. DWARF and frame-pointer call graphs were denied by the
worker's current perf policy; no system policy was changed.

| Case | Leaf samples | Observed functions | Attribution limit |
| --- | ---: | --- | --- |
| `wide_400`, physical | 1,585 | `__memcmp_evex_movbe` 36.53%; `TableReference::resolved_eq` 5.62%; `TreeNode::transform_down_impl` 4.42%; `DFSchema::field_with_qualified_name` 3.03% | Name/schema comparison and traversal are visible; the shared comparison leaf does not identify an analyzer or optimizer rule. |
| `in_1000`, logical | 1,099 | Chumsky `InputRef::add_alt_err` 11.92%, `RichPattern::eq` 11.46%; Sail `parse_keyword` and `parse_operator` 4.82% each | Parser/error-pattern functions are visible; no complete caller chain is available. |
| `case_200`, logical | 920 | Chumsky `InputRef::add_alt_err` 10.65%, `RichPattern::eq` 9.89%; Sail `parse_keyword` 4.67%, `parse_operator` 4.46% | Parser/error-pattern functions are visible; no complete caller chain is available. |
| `array_literal_access`, physical | 255 | `memmove` and `_int_malloc` 5.10% each; `core::fmt::write` 4.71%; `ExprSchemable::to_field` and `fields_with_udf` 1.57% each | Shared allocation, movement, and formatting leaves do not establish one expensive rule. |
| `nested_array_struct_access`, physical | 198 | `memmove` and `core::fmt::write` 3.54% each; `SchemaDisplay::fmt` 2.02%; `ExprSchemable::to_field` 1.52% | The short profile and shared leaves do not establish one expensive rule. |

Percentages describe the whole profiled process, including registration and
snapshot setup before the repeated phase. Shared allocator, comparison, movement,
and formatting functions can include that setup. The samples identify observed
functions; they do not apportion exact phase milliseconds to those functions or
prove a particular caller rule. No optimization or product/dependency change
was attempted.

## Validation and limits

All required local Linux gates passed from the Spark workspace with Rust 1.96.0:

```sh
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 clippy --locked -p delta-arrow-reader-spark-sql --all-targets --no-deps -- -D warnings
cargo +1.96.0 test --locked --workspace
```

The complete workspace result was 3,162 passed, zero failed, and three existing
ignored tests across 229 result groups. The retained receipt contains all 3,165
named outcomes; the coordinator confirmed that each outcome and group summary
matches the accepted checkpoint inventory. The ignored items are
`floating_extrema_bounded_cost` and the `interval_none` and `match_string_type`
Sail function doctests.

The release build, final untimed preflight, manifest/schedule checks, and
aggregation checks (including missing/duplicate process rejection) also passed.
An independent clean-context review verified the numbers, coverage, preflight,
profiles, and connected behavior without actionable findings. Full gate logs,
the workspace inventory/result receipt, and the review ledger are retained in
the external evidence directory.

The only post-measurement harness correction simplifies an equivalent boolean
condition in untimed preflight selection for Clippy. The timed path, frozen
queries, schedule, and samples are unchanged.

These results cover an already registered, warm local snapshot and small recorded
Arrow inputs with one target partition on one pinned logical CPU. They do not
measure cold Delta metadata discovery, remote/cloud I/O, query execution time,
production data sizes, other partition counts, or a build without the dependency
forks. Those costs cannot be inferred from this planning-only run. Near-threshold
cases such as the wide projection remain workload/worker observations; the
schedule was not repeated to chase a number.
