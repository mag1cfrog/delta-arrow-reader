# String function attribution (#1388, PR 1)

SUBSTRING and CONCAT exceed their native noise floors in both ANSI modes.
SUBSTRING has the larger isolated gap. The target is not yet met; this PR
establishes before measurements and code attribution without changing product code.

The product baseline is `8260c58ba48e0a65ca7cb7668dd2d8c64b1434f1`; the measured
harness/protocol commit is `518b695089533e04760a7b21261867aea6ce980f`. The
[manifest](../strings/queries.json) contains the original combined
`string_functions` plus `substring_only`, `concat_only` and `upper_only` over the
retained genuine SF1 snapshot, with 6,001,215 rows.

## Before measurements

Completion includes planning through stream drain. Values are medians of three
process medians, each from three retained iterations after one warmup. Min/max
and IQR describe those process medians. ns/row divides completion by 6,001,215;
displayed values are rounded, while decisions use unrounded nanoseconds.

| Shape | Mode | Completion ms | ns/row | Min-max ms | IQR ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Combined | Native | 350.202303 | 58.355234 | 345.659365-369.263736 | 11.8021855 |
| Combined | Spark ANSI off | 420.596906 | 70.085292 | 420.386486-426.388724 | 3.0011190 |
| Combined | Spark ANSI on | 418.043935 | 69.659883 | 411.170809-425.022967 | 6.9260790 |
| SUBSTRING | Native | 193.595235 | 32.259340 | 175.662970-194.837072 | 9.5870510 |
| SUBSTRING | Spark ANSI off | 240.511022 | 40.077055 | 218.560714-245.716952 | 13.5781190 |
| SUBSTRING | Spark ANSI on | 235.056081 | 39.168082 | 223.432983-242.942398 | 9.7547075 |
| CONCAT | Native | 127.570297 | 21.257412 | 127.476633-127.906080 | 0.2147235 |
| CONCAT | Spark ANSI off | 153.826142 | 25.632500 | 153.604661-154.523728 | 0.4595335 |
| CONCAT | Spark ANSI on | 154.908462 | 25.812850 | 154.020002-155.694742 | 0.8373700 |
| UPPER | Native | 224.241285 | 37.365981 | 223.690993-227.026627 | 1.6678170 |
| UPPER | Spark ANSI off | 216.873399 | 36.138249 | 214.517925-220.930446 | 3.2062605 |
| UPPER | Spark ANSI on | 208.124862 | 34.680454 | 205.501819-227.151029 | 10.8246050 |

Noise is the absolute difference between native A/B arm medians under the same
aggregation. The flag is strictly `Spark completion - native completion > noise`;
equality is false. There is no 1.25x or 50 ms cutoff.

| Shape | Noise ms | ANSI off gap ms | Off flag | ANSI on gap ms | On flag |
| --- | ---: | ---: | --- | ---: | --- |
| Combined | 0.437082 | 70.394603 | true | 67.841632 | true |
| SUBSTRING | 18.656630 | 46.915787 | true | 41.460846 | true |
| CONCAT | 1.242178 | 26.255845 | true | 27.338165 | true |
| UPPER | 13.292979 | -7.367886 | false | -16.116423 | false |

The observed 18.656630 ms SUBSTRING and 13.292979 ms UPPER noise floors remain in
the results. They describe this campaign's repeatability, not variation across
workers or dates. UPPER uses the same implementation in both paths and has no
positive-cost flag; its negative gaps do not establish a general speedup.
[Full metrics](strings-1388-metrics.json.gz) retain all 340 statistics, including
planning, first-batch/execution, memory, source counters and spread.
[Raw samples](strings-1388-samples.jsonl.gz) and
[summary flags](strings-1388-summary.json) retain the exact decision inputs.

## Source attribution

SUBSTRING's direct decoded chain is `count_chars -> Chars::count ->
SubstringItem::positional_len -> spark_substring_generic ->
SparkSubstring::invoke_with_args`, present in two combined and two isolated
samples. The row loop is **registry datafusion-spark 54.1.0**, not an existing
vendored fork. In `src/function/string/substring.rs`, line 246 counts all characters
and line 396 calculates that positional length unconditionally, even for positive
start 1. The [shared short-prefix policy](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/vendor/dependencies/datafusion-functions/src/unicode/substr.rs#L239)
sets `is_ascii=false` for length 12, so this full-comment pass runs before prefix
bounds. The [native view path](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/vendor/dependencies/datafusion-functions/src/unicode/substr.rs#L271)
calculates bounds and appends a view without the positional-length pass. Sail's
[STRING delegate](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-function/src/scalar/string/spark_substring.rs#L166)
calls the registry implementation. Its locked package checksum is
`a1ccd16a6949503e56c084df1b90c8889db826ec9347d2f0f51a7837d6fa011e`.
The [profile archive](strings-1388-profiles.tar.gz) includes the exact registry
source, source hashes and decoded excerpts. Counting, generic view construction
and shared bounds handling are not assigned separate millisecond costs.

CONCAT samples show `memmove -> GenericByteBuilder::append_option ->
cast_view_to_byte` at the [copy loop](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/vendor/dependencies/arrow-cast/src/cast/mod.rs#L2847),
and DataFusion's `ConcatFunc` at `concat.rs:251`, its Utf8 branch. Sail's
[STRING branch](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-function/src/scalar/collection/spark_concat.rs#L229)
forces arguments to Utf8 through
[cast_columnar_values](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-function/src/scalar/collection/spark_concat.rs#L365),
then delegates to concat, converts the result back to Utf8View and applies NULL
selection. Native concat [accepts view columns directly](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/vendor/dependencies/datafusion-functions/src/string/concat.rs#L209).
This links copying to Sail's conversion policy through the isolated plan and
source; no decoded sample retains the Sail SparkConcat parent. It is a
plan/source inference, not a direct caller chain or allocation of the full
27.338165 ms gap.

The measured binary aliases `ConcatStringBuilder::write` and
`ConcatLargeStringBuilder::write` at `0x8272650` and `0x8272c40`. The Large label
does not prove LargeUtf8 data: the decoded caller selects Utf8. The actual symbol
listing is archived. The combined profile contains both cost paths, alongside
UPPER and shared Parquet/ZSTD work. Isolated gaps are not an additive decomposition
of the combined gap, and no positive UPPER cost is attributed.

## Profile limits

Only combined/ANSI off, SUBSTRING/ANSI off and CONCAT/ANSI on ran, once each.
Each had one unsampled warmup and three executions at 199 Hz with 8,192-byte DWARF
stacks. All stayed within 180 seconds/512 MiB each and 900 seconds overall.
All reported zero lost samples; that does not establish complete unwinding.

| Profile | Samples | Raw bytes | Empty chains | Unresolved frames/all | Affected samples | Unknown leaves | ??:0/all :0 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Combined/off | 467 | 4,011,940 | 8 | 158/3165 | 111 | 4 | 116/157 |
| SUBSTRING/off | 226 | 1,973,740 | 0 | 64/1702 | 61 | 3 | 29/40 |
| CONCAT/on | 171 | 1,510,796 | 0 | 135/956 | 78 | 3 | 37/68 |

Frames include inlines. Affected samples contain unresolved symbols; unknown
leaves are unresolved first frames among nonempty chains. The eight empty
combined chains are separate from its 111 affected samples and four unknown
leaves. `??:0` is a subset of all zero-line source annotations, not a symbol-frame
count. Combined and CONCAT also have retained addr2line "could not read first
record" warnings.

The sampled calls include recorder resets and post-drain instrumentation.
One CONCAT sample retains `source_evidence`/JSON destruction at
[execution.rs:705](https://github.com/mag1cfrog/delta-arrow-reader/blob/518b695089533e04760a7b21261867aea6ce980f/crates/delta-arrow-reader-spark-sql/benches/execution.rs#L705), after completion was fixed at
line 695. This outside-timer work is excluded from candidate product cost.
No native or UPPER profile ran, and missing callers prevent exact gap allocation.

## Validation and provenance

Twelve full exports and eight decoded native/Spark comparisons passed: each
configuration has 6,001,215 equal rows, values, NULLs, multiplicity and executed
primitive types (Int64/Int32 keys and Utf8View outputs). Each reads six identical
source paths, has two output partitions and final counters with equal
received/range byte totals:

| Shape | Source bytes in each configuration |
| --- | ---: |
| Combined | 81,807,546 |
| SUBSTRING | 80,224,680 |
| CONCAT | 27,144,204 |
| UPPER | 80,224,680 |

All 180 retained samples matched preflight rows, partitions and exact source I/O.
The [full preflight](strings-1388-preflight.json.gz) retains ranges/counters; its
[bound receipt](../strings/preflight-receipt.json) holds compact expectations.
The [plans archive](strings-1388-plans.tar.gz) has twelve pre-export-sort plans
and eight native/Spark diffs.

The [frozen protocol](strings-1388-protocol.md) uses Rust 1.96.0 release opt3/debug1,
no compiler overrides, affinity `2,3,4,5`, two runtime workers/target partitions,
batch size 8,192 and the unchanged common view/session. One 60-process schedule
ran with 60 warmups and 180 retained samples, without retries, cache flushing,
concurrency or old 18-shape timings. The exact committed executable and independent
read-only seal both hash to
`ca86a5dc0313d80ec7311e642f86d23a906cc4aad063c7445b6b085201064dfc`.

Local Linux fmt, strict root-package all-target Clippy and the Spark workspace
passed. All 3,167 prior outcomes in 230 groups are unchanged: 3,164 passed, zero
failed and three ignored. The Rust candidate remained unchanged through later
controller/metadata refinements. Independent audits checked committed bindings,
all 340 statistics, 180 I/O samples and three profiles. All seven frozen files,
86 historical assets and nine protected engine objects remain unchanged.
[Validation](strings-1388-validation.json) and
[compressed details](strings-1388-validation-details.json.gz) retain the full
named inventory and audits; [metadata](strings-1388-metadata.json) binds commands,
toolchain, hashes and archive manifests. IPC exports and callback logs stay outside
Git. Full metrics are published compressed with both raw and gzip hashes.

## Next bounded slices

The target is not yet met. SUBSTRING is the largest isolated attributed cost,
followed by eligible CONCAT. The issue permits one reviewed change for each,
with semantic proof and frozen affected-shape measurements. No implementation is
chosen here. Remaining gaps after those bounded changes must be reported; a
second optimization round requires a separate maintainer decision.
