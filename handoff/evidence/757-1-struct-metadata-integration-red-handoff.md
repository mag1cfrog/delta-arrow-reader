# STRUCT and metadata integration: test-only candidate

This reference is for the root coordinator and the next independent reviewer. It identifies the complete #757 replay, the preserved #710 foundation, and the commands the coordinator must run serially. It does not certify a green integration order or a landable PR.

Documentation Writer supplied the reference structure; Humanizer supplied the final prose pass. The existing task authorized this evidence inventory and recipe. No product documentation changed.

## Immutable inputs and output

- Planning staging: `91e6eaca9720f30097a04256cdb9d47e14a6e59b`.
- Metadata foundation: `1df41b7a1b4eaad3843b9f14fc6d12c68bed2e1c`.
- Frozen STRUCT reference: `077af718ed738273d5b8e016ebdbb4bd12a0ea39`.
- Test-only candidate: `bb2139c486a6de87aafe860cde4486ffbf8786b2`.
- Branch: `fix/spark-struct-metadata-integration`.
- Worktree: `/home/hanbo/repo/delta-arrow-reader-spark-struct-metadata-integration`.
- Full file, blob, SHA-256, raw identity and source inventory: [struct-metadata-integration-inventory-red.json](/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/struct-metadata-integration-inventory-red.json).

The candidate adds nine test, fixture and capture files. It changes no production file, no existing nested-field assertion and none of the 46 #710 foundation paths. The original metadata and STRUCT worktrees remain untouched. This worktree has no target cache.

The actual post-#803 staging base and exact #803 source must be recorded before the red replay. The coordinator has not yet supplied those results. No parent issue is complete.

## Complete #757 ownership

All four raw fixtures and all four capture sources are byte-identical to `077af718ed738273d5b8e016ebdbb4bd12a0ea39`. No raw case, metadata field, generated label, type, nullability declaration, error condition or duplicate row was removed or changed.

| Fixture in `tests/fixtures` | Raw observations | Comparisons at batches 1/32 | SHA-256 |
| --- | ---: | ---: | --- |
| `struct-expression-names-spark.jsonl` | 150 | 300 | `5102b0a99c72233a572e23b789c42fc7b198784e63bc24ef5744cd76e2d7556e` |
| `struct-identifier-names-spark.jsonl` | 48 | 96 | `42439032447d6ae188bb86f9c88a53ec21e487e0e533d328642accb824990a90` |
| `struct-identifier-wildcard-spark.jsonl` | 4 | 8 | `df5acb25997ad231b29c69a8d8a2d09f7d87ab28da34f5d1dcafb80f5c55d7b3` |
| `review-757-ordinary-wildcard-spark-v1.jsonl` | 8 | 16 | `cd90273417ba7bdc1354f7f6b42ccabb10b881f49e64458be8b8c38396490d58` |

The 210 observations produce 420 comparisons. They include all six `FIELD_NOT_FOUND` references and all 48 historical metadata/NULL observations. Generated labels and `unnamed` stay within the complete replay.

Capture source provenance:

- `capture_struct_expression_names_spark.py`: SHA-256 `b25f29b8aa2eb488f8f3bb47c08e9bb066d2da168e74bb1626b86574f4a40109`.
- `capture_struct_identifier_names_spark.py`: SHA-256 `98c52a2bf024fd6b1ae6f3ff8b019c3796e4ebdea5b8bf32d2efcdddcea6c3f3`.
- `capture_struct_identifier_wildcard_spark.py`: SHA-256 `f4bc17ab00ea990b525115c540accddd323b767d5a5e5cfc329e6767995a56ce`.
- `capture_review_757_ordinary_wildcard_spark_v1.py`: SHA-256 `0f94de4b00b777cf17f4ca2ffe275b25ccf470b25db9520812c76b3d3a2191f2`.

All captures import the existing `capture_floating_group_keys_spark.py`; its bytes match the frozen STRUCT reference. The two wildcard captures read the tracked STRUCT reference header. Capture sources are retained as provenance and were not executed.

The test file now executes every raw record in each fixture. It uses `capture_spark_query_observation`, which validates full resolved versus physical names, types, metadata and permitted top-level nullability narrowing, then validates every returned batch and recursively encodes NULL values. Full public result schemas and duplicate-preserving sorted rows compare exactly with the original Spark records. Only a typed DataFusion error containing `[FIELD_NOT_FOUND]` satisfies that recorded error condition; other errors remain failures. Each comparison prints its complete observation and result for diagnostics.

The native control retains complete native schemas and batches before and after a Spark query on the configured session. The existing #710 observer and batch validator were not replaced by the old STRUCT NULL encoder.

## Production composition boundary

These are the 14 distinct production paths under `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/` identified by the supplied dependency decision:

- `expression_errors.rs` (#710 foundation retained).
- `function/aggregate.rs` (#710 foundation retained).
- `function/common.rs` (#710 foundation retained).
- `function/scalar/datetime.rs` (#710 foundation retained).
- `function/scalar/math.rs` (#710 foundation retained).
- `function/scalar/struct.rs` (STRUCT composition target).
- `resolver/expression/cast.rs` (#710 foundation retained).
- `resolver/expression/function.rs` (STRUCT composition target).
- `resolver/expression/misc.rs` (STRUCT composition target).
- `resolver/expression/mod.rs` (#710 foundation retained).
- `resolver/query/aggregate.rs` (#710 foundation retained).
- `resolver/query/project.rs` (#710 foundation retained).
- `resolver/query/sort.rs` (#710 foundation retained).
- `resolver/query/values.rs` (#710 foundation retained).

Metadata production source commits are `c851d745b658c8a8ed80f6285811c4b5754d3284`, `c9824b0b403b0776abf3380d1e43a4489e720712`, `58ae23b1390f2ab6fa51c916e4028105ea5ac8b2`, `9817f19a3fd9d19bfd3a303f7327ae57ac2d8669` and `1df41b7a1b4eaad3843b9f14fc6d12c68bed2e1c`. STRUCT production source commits are `b15640e5054a1786192a39822fe1d7bb028e28e9`, `402d48d1050a823a9ea46aa26928654a53f277d1`, `716aedbfa1c1c235701cd3919cdbcd3e9b6c6131` and `2764de99f96891a8933cbe8b44288b18b738c02e`; `077af718` repairs the capture reference path.

Static tracing found one required composition change: the old STRUCT alias helper cannot bypass #710's `direct_alias_source` classification. That classification must still inspect the original syntax before resolution. STRUCT needs the resolved source display for `source AS alias` labels, and the post-resolution alias application must retain the original classification so permanent and `analysis_only` metadata annotations keep their existing lifetimes. The composition must resolve each child once and preserve #810's merged `named_struct` builder, #717's DIV labels and #803's extraction validation. No production edit has been made yet.

## #710 obligations retained

All broad replays remain strict and open. There is no new acceptance partition and no ignored, filtered or marker-stripped replacement for the 210-record v1, 240-record v2 or supplemental references. Native, supplied metadata, extension types, volatile evaluation, grouping, conditionals, CAST visibility, NULL/error behavior, public/executed schema stages and downstream aliases remain required.

In particular, `generated_metadata_preserves_null_and_error_semantics` retains all 48 raw records. If its 12 scalar-subquery generated-name comparisons remain red, they are a source prerequisite. A green #757 replay cannot waive that failure or close #710/#790.

The complete inherited inventory is:

- `crates/delta-arrow-reader-spark-sql/tests/fixtures/capture_generated_alias_metadata_accepted_if_spark.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/capture_generated_alias_metadata_boundaries_spark.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/capture_generated_alias_metadata_historical_errors_spark.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/capture_generated_alias_metadata_spark.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/capture_generated_alias_metadata_spark_v2.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/generated-alias-metadata-accepted-if-spark.jsonl` (16 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/generated-alias-metadata-boundaries-spark.jsonl` (28 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/generated-alias-metadata-historical-errors-spark.jsonl` (24 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/generated-alias-metadata-reviewed-contracts.json`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/generated-alias-metadata-spark-v2.jsonl` (240 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/generated-alias-metadata-spark.jsonl` (210 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-cast-visibility-spark.jsonl` (8 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-conditional-schema-spark.jsonl` (8 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-identifier-extraction-spark.jsonl` (16 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-literal-grouping-spark.jsonl` (10 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-metadata-semantics-spark.jsonl` (48 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-nested-positive-alias-forms-spark.jsonl` (24 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-nested-positive-alias-spark.jsonl` (12 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review-710-positive-alias-spark.jsonl` (24 raw observations).
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_cast_visibility_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_conditional_schema_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_identifier_extract_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_literal_grouping_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_metadata_semantics_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_nested_positive_alias_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_nested_positive_alias_forms_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/review_710_positive_alias_capture.py`.
- `crates/delta-arrow-reader-spark-sql/tests/fixtures/reviewed-corrections.json`.
- `crates/delta-arrow-reader-spark-sql/tests/generated_alias_metadata.rs`.
- `crates/delta-arrow-reader-spark-sql/tests/nested_field_labels.rs`.
- `crates/delta-arrow-reader-spark-sql/tests/pivot_names.rs`.
- `crates/delta-arrow-reader-spark-sql/tests/single_row_correlated.rs`.
- `crates/delta-arrow-reader-spark-sql/tests/support/spark_results.rs`.
- `crates/delta-arrow-reader-spark-sql/tests/window_metadata.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/expression_errors.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/aggregate.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/common.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/scalar/datetime.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/scalar/math.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/resolver/expression/cast.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/resolver/expression/misc.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/resolver/expression/mod.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/resolver/query/aggregate.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/resolver/query/project.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/resolver/query/sort.rs`.
- `crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/resolver/query/values.rs`.

## Coordinator recipe

Only the root coordinator runs Cargo, Rust, Spark, JVM or build commands. After #803 lands, first rebase this branch onto the actual staging tip while preserving the exact #803 source, then record the resulting immutable red head. Use the coordinator's existing serialized engine environment and cache; no target cache was created here.

From `/home/hanbo/repo/delta-arrow-reader-spark-struct-metadata-integration/crates/delta-arrow-reader-spark-sql`, run serially and retain complete logs outside product docs:

```sh
cargo test --workspace -p delta-arrow-reader-spark-sql --test struct_expression_names -- --nocapture
cargo test --workspace -p delta-arrow-reader-spark-sql --test generated_alias_metadata -- --nocapture
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

The first command owns all 210 #757 records and 420 comparisons. The second retains every #710 guard, including the known scalar-name prerequisite. The full workspace command is required at the red and final candidate heads. A target or parent replay failure must be recorded at the tested head, not converted into a comparison exception. Production edits wait for the coordinator's red evidence. Any final candidate still needs all exact-final checks and a fresh clean-context Bulletproof Review.

## Static checks and limits

Executed: exact imported-byte comparisons, raw JSON parsing, fixture/header/identity/count checks, Python `ast.parse` for all four capture sources, unchanged checks for every #710 foundation path, staged `git diff --check`, and clean worktree/no-target checks. The commit contains nine test-only files.

Not executed: Rust compilation, Cargo tests/fmt/Clippy, Spark/JVM, fixture recapture, or independent review. The test-only candidate is intended to expose the full red contract; its actual failures await root evidence.

Skipped product edits, engine runs and GitHub writes. Risk: #757/#710 composition is not yet a demonstrated green sequence, and scalar names may require a separately justified source prerequisite.
