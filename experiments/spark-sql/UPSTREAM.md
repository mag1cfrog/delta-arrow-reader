# Sail source provenance

`vendor/sail/` contains a subset of [Sail v0.7.1](https://github.com/lakehq/sail/tree/v0.7.1), commit `9544c9253e981a82c5f9e493c43ce98a4d9d41b7`, under its Apache-2.0 [LICENSE](vendor/sail/LICENSE). That upstream revision has no root NOTICE file. Existing source notices remain intact. Modified files carry a header pointing here.

The import retains the upstream Cargo.toml, README.md, LICENSE and these 8 crate directories:

```text
sail-common              sail-common-datafusion
sail-function            sail-logical-plan
sail-plan                sail-sql-analyzer
sail-sql-macro           sail-sql-parser
```

`upstream.patch` records the changes within that selection: 1,056 added lines and 46,833 deleted lines, including 144 deleted files. It also adds `sail-plan/src/function/table/range_exec.rs`, copied from upstream `sail-physical-plan/src/range.rs`. The initial copy preserved all 145 source lines apart from the modification notice. A later cut removes its uncalled getters and unused original-schema storage; range execution and projection behavior remain. Crates outside the selection are omitted, rather than represented as deletions in the patch.

The adaptation changes table lookup to the native DataFusion registry, rejects table modifiers, restores output names, and removes command/streaming/explain and data-source/cache/physical-plan dependencies. It also handles DataFusion's SQL error variant and SQL-enabled wildcard AST types. SQL runtime function implementations are retained. Python UDF execution/configuration and its two crates are removed. The two catalog crates, catalog command/display types and PlanService are also removed; SparkPlanFormatter remains as a direct utility. Spark builtin functions take precedence over the native registry. Session catalog/schema functions read the native session configuration. Shared catalog/data-source/write helpers, unused schema-evolution/time-travel logic and function-help generation are removed. Sail's actor/server runtime, application configuration, telemetry, session/streaming/checkpoint modules and system-table/protobuf generation are removed. Streaming read and watermark specs are rejected explicitly. DataFrame NA/statistics/value-replacement resolvers and unused ShowString/SchemaPivot nodes are removed. Command analysis rejects non-query ASTs before translating their bodies; write/catalog/metadata command conversion is removed. Inline Arrow import, unused serialization/placeholder helpers and remote error/stream-UDF infrastructure are removed. DataFrame transforms, their explicit-repartition node and eager tail/PIVOT-inference execution are removed; SQL TABLESAMPLE retains the existing Bernoulli filter. Unused command spec types and their Plan/CommandPlan wrapper are removed; analysis and resolution exchange QueryPlan directly. Unused direct/workspace dependencies and uncalled helpers are removed. The parser syntax test compares the complete unchanged snapshot directly instead of using the shared gold-test framework. Variant write-time shredding/inference/alignment helpers and unused DataFusion serde/Avro features are removed; Variant SQL metadata and conversion remain. Spark Connect reference-plan/alias and SQL parameter scopes are removed; direct specs reject explicitly. SQL-owned subqueries, CTEs, multi-column IN and constant IDENTIFIER evaluation remain. DataFrame plan-ID tracking, regex selection, struct update/drop expressions and an unused external-schema rewriter are removed. SQL field IDs, hidden-field handling and nested-field access remain. Unused adapter/parser/config/codec/format helpers and legacy HEX code are removed. SQL HEX uses its existing DataFusion Spark implementation; UNHEX and query execution remain. Tokio features are reduced. Unused codec getters, DebugBinary and the unregistered timestamp wrapper are removed. Registered timestamp functions and the host-facing TableInput accessor remain. The copied scalar-expansion helper now delegates to DataFusion. First/last window functions use native DataFusion null treatment; the obsolete Protobuf workaround is removed. Unused Connect enum-number conversions and two protocol-only interval enums are removed, along with num_enum. Unused in-process spec JSON codecs and half serde support are removed; required metadata codecs remain. Unused AST-to-SQL printing, its TreeText macro and raw string-token copies are removed. Unproduced protocol/storage error variants and constructors are removed. NTILE uses DataFusion 54.1's corrected bucket evaluator behind Sail's existing parameter checks. The unused JSON union encoder is removed; its builder test uses native Arrow access. Unused workspace Serde feature requests for Arrow schemas, ordered floats and Chrono were removed. Parser keyword generation, the syntax snapshot test, SQL value formatting and Arrow result streaming remain.

To reproduce the import, copy the three root files and the 8 directories above from the pinned upstream commit into a temporary Git checkout, then apply `upstream.patch` with `git apply`. An archive of that selection plus the patch was checked against all 362 vendored files byte-for-byte. Normal builds use the committed files directly; they need no upstream checkout.

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

Gross/production/test/build-script counts include comments and blank lines. The test count includes files under `tests/` and formatted `#[cfg(test)]` modules/constants. The counter rejects unrecognized test-item shapes; it uses upstream indentation to identify module boundaries. Production count means source outside those test sections and build scripts, not live code reached by this corpus. Unused functions still count.

The tested build generated one Rust file / 269 lines from sail-sql-parser. Service removal removed sail-common-datafusion's five generated files / 416 lines and its 467-line build script. It also removed two application/system YAML files / 1,295 lines and one protobuf file / 31 lines, counted separately from Rust. The previous storage cut removed the help-metadata generator's one generated file / 566 lines and 24 YAML files / 12,447 lines. Procedural macro expansion was not measured; macro source remains counted.

Service removal deleted 343 test lines belonging to removed helpers: 148 actor, 21 application-config, 127 catalog-config and 47 streaming-marker lines. The previous storage cut removed 670 test lines. The frozen corpus and its two checked-in baselines remain unchanged; the latest runner, planner and Python test totals are recorded in the README. Transport removal deletes another 338 test lines belonging to the removed placeholder/cast helpers. The standalone upstream parser syntax test passes using the README commands. Variant write-helper removal deletes 116 writer test lines; the retained Variant function suite passes all 23 tests.

Dependency counts include the runner and reader, rather than only dependencies added by Sail. The all-target graph includes development dependencies. Neither resolved graph contains sail-python-udf, sail-pyarrow, PyO3, sail-catalog, sail-catalog-memory, tonic/prost packages, figment or fastrace. Service removal deleted 42 resolved packages. DataFrame/display removal deletes eight more all-target packages and four Linux build packages. Neither cut adds or upgrades a package. Command-analysis removal leaves the resolved graph unchanged. Transport removal deletes serde_arrow, marrow and bytemuck_derive, without adding or upgrading any package. Orphan-dependency cleanup removes serde_yaml, unsafe-libyaml and rand_chacha 0.10 with no added/upgraded packages. Disabling the unused Avro source removes arrow-avro, datafusion-datasource-avro and strum_macros 0.28, also without added/upgraded packages. Narrowing Tokio features removes signal-hook-registry from the all-target graph and two Linux build packages, with no package additions/upgrades. `upstream.patch` describes changes within the eight retained crates; the four omitted Python/catalog crates are excluded from its selection. Their removal remains visible in the repository diff against the import commit.

The checkpoint was built with Rust 1.97.1, DataFusion 54.1.0 and Arrow 58.4.0, with `PROTOC`, `PYO3_PYTHON` and `PYO3_CONFIG_FILE` set to nonexistent paths. Sail declares Rust 1.96.0; the reader's lower MSRV is unchanged. Builds reused a local Cargo cache, so no cold-build timing claim is made. The debug-profile runner with debug information disabled is 429,250,552 bytes; this is a host binary measurement, not a wheel-size estimate or release-build measurement. The result is a demonstrated subset, not a minimum.
