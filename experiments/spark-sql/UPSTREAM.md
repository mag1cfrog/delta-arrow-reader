# Sail source provenance

`vendor/sail/` contains a subset of [Sail v0.7.1](https://github.com/lakehq/sail/tree/v0.7.1), commit `9544c9253e981a82c5f9e493c43ce98a4d9d41b7`, under its Apache-2.0 [LICENSE](vendor/sail/LICENSE). That upstream revision has no root NOTICE file. Existing source notices remain intact. Modified files carry a header pointing here.

The import retains the upstream Cargo.toml, README.md, LICENSE and these 8 crate directories:

```text
sail-common              sail-common-datafusion
sail-function            sail-logical-plan
sail-plan                sail-sql-analyzer
sail-sql-macro           sail-sql-parser
```

`upstream.patch` records the changes within that selection: 2,139 added lines and 48,559 deleted lines, including 145 deleted files. It also adds `sail-plan/src/function/table/range_exec.rs`, copied from upstream `sail-physical-plan/src/range.rs`. The initial copy preserved all 145 source lines apart from the modification notice. A later cut removes its uncalled getters and unused original-schema storage; range execution and projection behavior remain. Crates outside the selection are omitted, rather than represented as deletions in the patch.

Execution integration adds `sail-plan/src/physical_plan/spark_partition_id.rs` from the pinned `sail-physical-plan/src/spark_partition_id.rs`. Its uncalled getters are omitted, the Stream trait uses the existing tokio-stream dependency, and a boundary test is added. `physical_plan/mod.rs` adapts the query/extension planner, partition-ID and two sorting branches from `sail-session/src/planner.rs`, using DataFusion's default planner for native plans. It additionally disables automatic round-robin repartitioning on a cloned planning state for RequiredSortNode plans that demand global order; the README records the reproduced upstream ordering problem and parallelism tradeoff. The extension adapter is private so callers use the complete query planner. These files are counted inside the retained `sail-plan` crate; neither upstream crate is added to the build.

`sail-plan/src/physical_plan/monotonic_id.rs` is copied from the pinned `sail-physical-plan/src/monotonic_id.rs`, with its planning branch adapted from `sail-session/src/planner.rs`. Uncalled getters are omitted and Stream uses tokio-stream. Local changes add an Int32 partition-range check, avoid overflow in the row-capacity check and add a boundary test. The 33-bit row counter, batch streaming, plan properties and statistics come from Sail. The README records SQL coverage and remaining upstream limitations.

The retained source is adapted at these boundaries:

- Named tables and views use DataFusion's registry. Spark builtin functions keep precedence, session catalog/schema functions read native configuration, and output names are restored after planning.
- Python execution, Spark Connect services, catalogs, storage/writes, streaming/checkpoints, DataFrame-only transforms and eager query collection are removed. Unsupported entrypoints reject before resolving their inputs. Unused inline-UDF definitions, PySpark evaluation modes and state/watermark payload structs are removed; minimal variants retain child inputs/arguments for rejection checks. SQL TABLESAMPLE retains its Bernoulli filter.
- Analysis and resolution exchange QueryPlan directly. Protocol serializers, error envelopes, unused getters, command descriptors, the orphan NULL-literal helper and inline Arrow imports are removed. Required Arrow metadata codecs remain.
- Scalar expansion, first/last window evaluation and NTILE bucket allocation reuse DataFusion. NTILE retains Sail's parameter checks. Runtime SQL function semantics and field/type handling remain in the retained crates.
- The unused AST-to-SQL printer and JSON union encoder/builder are removed. TreeParser, TreeSyntax, keyword generation, SQL value formatting, JSON union input readers and Arrow result streams remain. The syntax test compares directly with the unchanged snapshot.
- Datetime patterns retain their existing input validation while unreachable formats and unread fields are removed. SQL value display uses the fixed Spark format; unused generic display options and their alternative formatters are removed.
- Unused dependencies and features are disabled, including DataFusion's additional Avro/Parquet sources and compressed-file transport. Delta reading and its Parquet/compression dependencies remain in the host reader.

The [experiment README](README.md) records each checkpoint, current crate roles, validation and known limitations.

To reproduce the import, copy the three root files and the 8 directories above from the pinned upstream commit into a temporary Git checkout, then apply `upstream.patch` with `git apply`. An archive of that selection plus the patch was checked against all 364 vendored files byte-for-byte. Normal builds use the committed files directly; they need no upstream checkout.

## Source measurements

`inventory.json` records current per-crate counts and a hash of all vendored files. The import baseline was committed as `fc402c8`. The approved Python removal slice was committed as `4674528`, catalog/session removal as `4987c12`, and storage/write removal as `63dee61`. Service/streaming removal was committed as `e9f5911`. Later cuts are recorded in the README. The measured history is:

| Checkpoint | Sail crates | Rust files | Gross lines | Nonblank | Production | Tests | Build scripts | All-target packages | Linux build packages |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Import baseline | 12 | 489 | 126,013 | 115,841 | 114,700 | 10,528 | 785 | 618 | 548 |
| After Python removal | 10 | 460 | 120,537 | 110,803 | 109,224 | 10,528 | 785 | 610 | 540 |
| After catalog removal | 8 | 434 | 113,834 | 104,657 | 103,767 | 9,282 | 785 | 599 | 529 |
| After storage removal | 8 | 419 | 107,168 | 98,530 | 98,009 | 8,612 | 547 | 599 | 529 |
| After service removal | 8 | 375 | 102,630 | 94,522 | 94,281 | 8,269 | 80 | 557 | 487 |
| DataFrame statistics/display | 8 | 371 | 101,311 | 93,298 | 92,962 | 8,269 | 80 | 549 | 483 |
| Command analysis | 8 | 371 | 99,020 | 91,052 | 90,671 | 8,269 | 80 | 549 | 483 |
| Inline Arrow/transport | 8 | 362 | 97,492 | 89,618 | 89,481 | 7,931 | 80 | 546 | 480 |
| DataFrame transforms | 8 | 358 | 96,442 | 88,626 | 88,431 | 7,931 | 80 | 546 | 480 |
| Command spec types | 8 | 358 | 95,555 | 87,790 | 87,544 | 7,931 | 80 | 546 | 480 |
| Orphan dependencies and test helpers | 8 | 357 | 95,376 | 87,625 | 87,364 | 7,932 | 80 | 543 | 477 |
| Variant write helpers and Avro source | 8 | 357 | 94,554 | 86,871 | 86,658 | 7,816 | 80 | 540 | 474 |
| Connect relation and parameter scopes | 8 | 356 | 94,175 | 86,515 | 86,279 | 7,816 | 80 | 540 | 474 |
| DataFrame expression plumbing | 8 | 354 | 93,643 | 86,035 | 85,747 | 7,816 | 80 | 540 | 474 |
| Unused adapter helpers | 8 | 354 | 93,256 | 85,699 | 85,360 | 7,816 | 80 | 539 | 472 |
| Unused codec getters and wrapper modules | 8 | 352 | 93,094 | 85,567 | 85,198 | 7,816 | 80 | 539 | 472 |
| Reuse DataFusion scalar helper | 8 | 352 | 93,062 | 85,538 | 85,166 | 7,816 | 80 | 539 | 472 |
| Native first/last window functions | 8 | 351 | 92,879 | 85,376 | 84,983 | 7,816 | 80 | 539 | 472 |
| Unused protocol enum conversions | 8 | 351 | 92,699 | 85,210 | 84,803 | 7,816 | 80 | 537 | 465 |
| Unused spec JSON serialization | 8 | 351 | 92,593 | 85,107 | 84,697 | 7,816 | 80 | 537 | 465 |
| Unused AST-to-SQL text generation | 8 | 349 | 92,272 | 84,808 | 84,382 | 7,810 | 80 | 537 | 465 |
| Orphan protocol/storage error variants | 8 | 349 | 92,223 | 84,765 | 84,333 | 7,810 | 80 | 537 | 465 |
| Reuse native NTILE evaluator | 8 | 349 | 92,228 | 84,781 | 84,248 | 7,900 | 80 | 537 | 465 |
| Unused JSON union encoder | 8 | 349 | 92,169 | 84,728 | 84,183 | 7,906 | 80 | 537 | 465 |
| Unused Serde feature removal | 8 | 349 | 92,169 | 84,728 | 84,183 | 7,906 | 80 | 537 | 465 |
| Unused DataFusion file-source features | 8 | 349 | 92,169 | 84,728 | 84,183 | 7,906 | 80 | 524 | 452 |
| Remaining protocol configuration getters | 8 | 349 | 92,069 | 84,658 | 84,083 | 7,906 | 80 | 524 | 452 |
| Unused JSON union builder | 8 | 349 | 91,945 | 84,547 | 83,937 | 7,928 | 80 | 524 | 452 |
| Unreachable datetime format paths | 8 | 349 | 91,604 | 84,218 | 83,476 | 8,048 | 80 | 524 | 452 |
| Orphan constants, macro helper and configuration | 8 | 349 | 91,559 | 84,176 | 83,431 | 8,048 | 80 | 524 | 452 |
| Unused generic display options | 8 | 349 | 91,226 | 83,872 | 83,066 | 8,080 | 80 | 524 | 452 |
| Unused internal parameter plumbing | 8 | 349 | 91,194 | 83,840 | 83,034 | 8,080 | 80 | 524 | 452 |
| Orphan recursive NULL-literal helper | 8 | 348 | 91,000 | 83,653 | 82,840 | 8,080 | 80 | 524 | 452 |
| Rejected UDF and streaming payloads | 8 | 348 | 90,871 | 83,534 | 82,711 | 8,080 | 80 | 524 | 452 |
| Partition-ID execution integration | 8 | 350 | 91,164 | 83,796 | 82,981 | 8,103 | 80 | 524 | 452 |
| Sorting execution integration | 8 | 350 | 91,232 | 83,864 | 83,049 | 8,103 | 80 | 524 | 452 |
| Monotonic-ID execution integration | 8 | 351 | 91,529 | 84,137 | 83,288 | 8,161 | 80 | 524 | 452 |

Gross/production/test/build-script counts include comments and blank lines. The test count includes files under `tests/` and formatted `#[cfg(test)]` modules/constants. The counter rejects unrecognized test-item shapes; it uses upstream indentation to identify module boundaries. Production count means source outside those test sections and build scripts, not live code reached by this corpus. Unused functions still count.

The following Delta/lifecycle checkpoint changes only host tests, their dependency declarations and documentation. It imports the existing reader fixture helper from `tests/reader/support/real_parquet_delta_table.rs` and adapts scenarios from `tests/reader/datafusion_adapter.rs`. Its 380 test-module lines and three cfg(test) runner lines are outside the vendored-source count. The source hash, inventory, upstream patch and measured binary size remain unchanged. Direct test references to delta_kernel and parquet add no resolved package, version or feature.

The tested build generated one Rust file / 269 lines from sail-sql-parser. Service removal removed sail-common-datafusion's five generated files / 416 lines and its 467-line build script. It also removed two application/system YAML files / 1,295 lines and one protobuf file / 31 lines, counted separately from Rust. The previous storage cut removed the help-metadata generator's one generated file / 566 lines and 24 YAML files / 12,447 lines. Procedural macro expansion was not measured; macro source remains counted.

Service removal deleted 343 test lines belonging to removed helpers: 148 actor, 21 application-config, 127 catalog-config and 47 streaming-marker lines. The previous storage cut removed 670 test lines. The frozen corpus and its two checked-in baselines remain unchanged; the latest runner, planner and Python test totals are recorded in the README. Transport removal deletes another 338 test lines belonging to the removed placeholder/cast helpers. The standalone upstream parser syntax test passes using the README commands. Variant write-helper removal deletes 116 writer test lines; the retained Variant function suite passes all 23 tests.

Dependency counts include the runner and reader. The all-target graph also includes development dependencies. The reduction checkpoints add or upgrade no package; the tables above record removals. Neither resolved graph contains the removed Python/catalog crates, PyO3, tonic/prost, figment, fastrace, serde_arrow, Avro source, DataFusion Parquet source, async-compression or num_enum packages. The reader's own Parquet and compression libraries remain.

`upstream.patch` describes changes within the eight retained crates. The four omitted Python/catalog crates are excluded from its selection; their removal remains visible in the repository diff against the import commit.

The checkpoint was built with Rust 1.97.1, DataFusion 54.1.0 and Arrow 58.4.0, with `PROTOC`, `PYO3_PYTHON` and `PYO3_CONFIG_FILE` set to nonexistent paths. Sail declares Rust 1.96.0; the reader's lower MSRV is unchanged. Builds reused a local Cargo cache, so no cold-build timing claim is made. The debug-profile runner with debug information disabled is 412,605,440 bytes; this is a host binary measurement, not a wheel-size estimate or release-build measurement. The result is a demonstrated subset, not a minimum.
