# Spark SQL function execution (#1391)

The Linux run produced accepted timing for 458 function cases and both follow-up workloads. 95 paired function cases ran slower than native beyond the measured noise floor in at least one ANSI mode. This report records execution costs for the staging product at `cde51da6`.

- [Results and exclusions by function and ANSI mode](functions-1391-results.csv)
- [Paired functions above the native noise floor and all timed Spark-only functions, ranked](functions-1391-rankings.csv)
- [Raw execution and projection samples](functions-1391-samples.jsonl.gz)
- [Physical plans](functions-1391-plans.tar.gz)
- [Protocol](functions-1391-protocol.md) and [frozen preflight receipt](../functions/preflight-receipt.json)

## Two follow-ups from #1380

Both controls use the new in-memory table and the same executed rows and types in native and Spark. Values below are median nanoseconds per input row above the plain projection. There are one million input rows, so one ns per input row equals one ms on this fixture.

| Workload | ANSI | Native | Spark | Spark minus native | Effective noise floor | Difference above floor |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| DECIMAL division with ROUND | off | 22.50 | 25.99 | +3.49 | 2.71 | Spark slower |
| DECIMAL division with ROUND | on | 22.50 | 31.75 | +9.25 | 2.67 | Spark slower |
| COUNT, SUM, MAX by 250,000 groups | off | 31.29 | 31.84 | +0.55 | 4.56 | Within floor |
| COUNT, SUM, MAX by 250,000 groups | on | 31.29 | 32.47 | +1.18 | 4.60 | Within floor |

The inputs and scan cost differ from the real Delta workload in #1380. These measurements apply to the in-memory controls.

## Largest measured costs

The rankings record 180 above-floor paired function/configuration rows in descending Spark-minus-native cost and all 246 measured Spark-only configuration rows in descending cost. The results file also retains the other paired measurements, process spread and ratios.

| Paired function | ANSI | Spark minus native, ns/input row | Noise floor |
| --- | --- | ---: | ---: |
| `array_join` (window) | off | 644.04 | 13.28 |
| `listagg` (window) | off | 638.97 | 5.57 |
| `string_agg` (window) | off | 637.70 | 13.57 |
| `array_join` (window) | on | 637.50 | 13.28 |
| `string_agg` (window) | on | 637.04 | 13.57 |

### Source review of the 30 highest-cost Spark-only cases

Ranked by the higher of the two ANSI medians. Costs include the window or generator output, casts and other functions called by the frozen SQL. Several Variant expressions include parsing JSON. The source column identifies row work and shared setup. All costs describe the current implementation.

When a five-row sliding frame moves and the accumulator cannot retract, the executor creates a new accumulator and replays the frame. [Sliding-window execution](../../vendor/dependencies/datafusion-physical-expr/src/window/sliding_aggregate.rs#L202).

| Rank | Function | ANSI off | ANSI on | Work for each row | Work that can be shared | Source |
| ---: | --- | ---: | ---: | --- | --- | --- |
| 1 | `from_xml` (scalar) | 1,458.53 | 1,462.99 | Parse each nonnull XML document into fields in a shared Arrow builder. | Options and schema are parsed once per invocation, before the row loop. | [from_xml](../../vendor/sail/crates/sail-function/src/scalar/xml/from_xml.rs#L260) |
| 2 | `histogram_numeric` (window) | 1,204.91 | 1,212.62 | Cast the current frame, insert each nonnull value into the histogram and build an array of bins for the output row. | Bin count is fixed. Each frame needs its own state and bins. The current fallback replays the frame. | [histogram_numeric](../../vendor/sail/crates/sail-function/src/aggregate/histogram_numeric.rs#L252) |
| 3 | `theta_union_agg` (window) | 976.75 | 979.87 | Merge each nonnull binary sketch into the current serialized state, then clone that state for the result. | Can deserialize inputs in a batch, but frame membership and results change. The current accumulator does not retract old values. | [theta_sketch](../../vendor/sail/crates/sail-function/src/aggregate/theta_sketch.rs#L262) |
| 4 | `hll_union_agg` (window) | 933.41 | 937.34 | Normalize or merge each input's sketch bytes, then clone the resulting bytes per frame. | Configuration is fixed. Batch decoding could avoid rereading some inputs, while each frame still needs the correct union. | [hll_sketch](../../vendor/sail/crates/sail-function/src/aggregate/hll_sketch.rs#L225) |
| 5 | `schema_of_variant` (scalar) | 925.16 | 923.17 | Parse JSON, infer the nested type of each Variant and format it as a schema string. | A builder serves the batch. Type inference and string contents vary per input. | [spark_schema_of_variant](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_schema_of_variant.rs#L84) |
| 6 | `count_min_sketch` (window) | 886.85 | 891.57 | Accumulate hash counters for each nonnull value, then serialize the sketch table for each output frame. | Parameters are fixed, but frame membership and serialized output vary. The sliding executor currently recreates the accumulator and replays the frame when its start moves. | [count_min_sketch](../../vendor/sail/crates/sail-function/src/aggregate/count_min_sketch.rs#L87) |
| 7 | `variant_explode` (generator) | 882.33 | 863.69 | Parse JSON, turn each object or array into position/key/value entries and unnest them. | Builders are reused within a batch. Output entries depend on each input. | [spark_variant_explode](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_variant_explode.rs#L186) |
| 8 | `variant_explode_outer` (generator) | 879.65 | 863.73 | Parse JSON and expand object or array entries through the same implementation as variant_explode. Both calls produced 1,875,000 rows, with NULL inputs contributing zero rows. | Builders are batch-level. The output multiplicity depends on each input. | [spark_variant_explode](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_variant_explode.rs#L186) |
| 9 | `variant_to_json` (scalar) | 844.13 | 838.45 | Serialize each decoded Variant to JSON through the string builder. | The builder is shared by a batch. Serialization depends on each row's value. | [spark_variant_to_json](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_variant_to_json.rs#L19) |
| 10 | `schema_of_variant_agg` (aggregate) | 738.95 | 748.36 | Parse JSON in the input expression, recursively infer the type of each nonnull Variant and merge it into the accumulated schema. | A batch can infer partial schemas, but each value must be visited. Final string formatting is already per state or result. | [schema_of_variant_agg](../../vendor/sail/crates/sail-function/src/aggregate/schema_of_variant_agg.rs#L78) |
| 11 | `theta_sketch_agg` (window) | 724.41 | 721.03 | Hash nonnull values into a sketch and compact and serialize it per output frame. | Configuration is fixed. Sketch contents and output vary, and a moving frame is replayed by the window executor. | [theta_sketch](../../vendor/sail/crates/sail-function/src/aggregate/theta_sketch.rs#L190) |
| 12 | `variant_get` (scalar) | 639.12 | 630.40 | Parse JSON to Variant in the input expression, then look up the path in each row's Variant. | Path and target type are already interpreted once per invocation. | [spark_variant_get](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_variant_get.rs#L99) |
| 13 | `try_variant_get` (scalar) | 623.77 | 625.11 | Parse JSON to Variant in the input expression, then extract with safe cast options. | Path and target type are already interpreted once per invocation. | [spark_variant_get](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_variant_get.rs#L99) |
| 14 | `is_variant_null` (scalar) | 574.45 | 575.73 | Parse JSON in the input expression, then test each Variant for its null tag, treating SQL NULL as false. | The boolean builder is already batch-level. The input parse and the tag test depend on the row. | [spark_is_variant_null](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_is_variant_null.rs#L75) |
| 15 | `parse_json` (scalar) | 523.29 | 529.87 | Parse each nonnull JSON string into Variant and convert the built Variant array to its binary layout. | The builder and the layout conversion are batch-level. The parse depends on the row. | [spark_parse_json](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_parse_json.rs#L276) |
| 16 | `try_to_number` (scalar) | 516.38 | 528.31 | Match and parse each string, using NULL for unmatched or malformed values. | Format interpretation and regex construction are already once per batch. | [spark_to_number](../../vendor/sail/crates/sail-function/src/scalar/string/spark_to_number.rs#L150) |
| 17 | `try_parse_json` (scalar) | 527.05 | 524.38 | Try the JSON-to-Variant parse for each nonnull string and append NULL on parse failure. | The builder and binary layout conversion are batch-level. The safe parse is per row. | [spark_parse_json](../../vendor/sail/crates/sail-function/src/scalar/variant/spark_parse_json.rs#L276) |
| 18 | `to_number` (scalar) | 513.90 | 513.29 | Match each string against the compiled number regex and convert the captures into a decimal. | Format interpretation and regex construction are already once per batch. | [spark_to_number](../../vendor/sail/crates/sail-function/src/scalar/string/spark_to_number.rs#L150) |
| 19 | `from_csv` (scalar) | 495.57 | 492.91 | Split and trim each nonnull line, parse or cast its fields, then convert the collected scalars into columns. | Schema and options are already prepared once per batch. Field parsing depends on each row. | [spark_from_csv](../../vendor/sail/crates/sail-function/src/scalar/csv/spark_from_csv.rs#L257) |
| 20 | `hll_sketch_agg` (window) | 419.70 | 426.41 | Update the sketch with each nonnull input and serialize it per output frame. | Sketch configuration is already fixed. Changing frames require different state and currently cause replay. | [hll_sketch](../../vendor/sail/crates/sail-function/src/aggregate/hll_sketch.rs#L149) |
| 21 | `str_to_map` (scalar) | 407.72 | 370.47 | Split strings into pairs, split each pair into key and value, then deduplicate keys per row. | The constant delimiter can use the scalar regex path. Row content and duplicate checks still vary. | [str_to_map](../../vendor/sail/crates/sail-function/src/scalar/map/str_to_map.rs#L95) |
| 22 | `mode` (window) | 384.77 | 386.65 | Count nonnull integers in a hash table for the frame, then scan those counts and apply the tie rule. | The tie rule is fixed. Counts vary with the frame and are rebuilt by the current sliding executor. | [mode](../../vendor/sail/crates/sail-function/src/aggregate/mode.rs#L265) |
| 23 | `map_concat` (scalar) | 373.37 | 366.29 | Extract key and value arrays, concatenate them and rebuild a per-row deduplicated map, preserving NULLs. | Array extraction and gather are batch operations. Duplicate detection depends on each row's keys. | [map](../../vendor/sail/crates/sail-plan/src/function/scalar/map.rs#L109) |
| 24 | `skewness` (window) | 251.57 | 253.13 | Merge each frame value into its running first three moments and compute skewness for the frame. | The moments vary by frame. Reusing work across frames would need to preserve the floating-point operation order. | [skewness](../../vendor/sail/crates/sail-function/src/aggregate/skewness.rs#L177) |
| 25 | `kurtosis` (window) | 249.66 | 250.72 | Merge each frame value into the running moments and evaluate the fourth-moment formula for that frame. | The moments vary by frame. Reusing work across frames would need to preserve the floating-point operation order. | [kurtosis](../../vendor/sail/crates/sail-function/src/aggregate/kurtosis.rs#L142) |
| 26 | `json_tuple` (scalar) | 239.39 | 240.22 | Parse each nonnull JSON object, look up the requested fields and put their strings in a struct that Sail unnests. | The builders and field arrays are shared by a batch. Field contents depend on each row. | `datafusion-spark 54.1.0 json/json_tuple.rs:111` |
| 27 | `product` (window) | 237.64 | 236.70 | Multiply the nonnull values in each frame and produce that frame's product. | Products depend on the active rows. The current implementation lacks retraction, so moving frames are replayed. | [product](../../vendor/sail/crates/sail-function/src/aggregate/product.rs#L100) |
| 28 | `from_json` (scalar) | 231.22 | 229.92 | Parse each row's JSON and append the typed value or permissive fallback to the output builder. | The schema, options and builder are already batch-level. | [from_json](../../vendor/sail/crates/sail-function/src/scalar/json/from_json.rs#L219) |
| 29 | `theta_intersection_agg` (aggregate) | 227.92 | 228.12 | Normalize and intersect each nonnull sketch with the current serialized state. | Sketch decoding and intersection could be grouped per batch, while each input's hashes must contribute. | [theta_sketch](../../vendor/sail/crates/sail-function/src/aggregate/theta_sketch.rs#L340) |
| 30 | `try_parse_url` (scalar) | 180.86 | 181.50 | Parse each nonnull URL with the same component extractor and return NULL on error. | The component choice is constant here. Error handling and extraction depend on each URL. | [spark_try_parse_url](../../vendor/sail/crates/sail-function/src/scalar/url/spark_try_parse_url.rs#L65) |

## Eligibility

The four effective registries contain 631 category/name entries, representing
559 distinct spellings. The matrix also includes two workload controls. The
manifest accounts for aliases and four shadowed window registrations. The
fixed-seed table has one million rows in 123 batches and 36 input columns.
Nullable data columns each have 62,500 NULLs.

| Preflight outcome | Cases |
| --- | ---: |
| Paired and eligible, including two workload controls | 337 |
| Spark only and eligible | 132 |
| Verified same execution function | 24 |
| Native row or executed-type comparison differed | 34 |
| Full-input execution or process failed | 8 |
| Excluded by registry or statement requirements | 98 |

Both ANSI configurations consumed all one million rows before a paired query
could enter timing. The receipt records 790 complete native-to-Spark
comparisons and 3,051 physical-plan hashes. The plan archive also contains
four plans and stderr logs left by failed preflight subprocesses, with hashes
in its `failed-preflight` directory. Thirty-three cases differed in
rows in at least one ANSI mode. The other case, `arrays_zip`, differed in
its executed nested type. These are differences from the selected native
statements on this fixture. Native and Spark SQL ran from the same DAR benchmark build.

The first untimed preflight exposed six native date controls that built
invalid intervals from a NULL and two map controls that used NULL keys. The
committed setup correction guarded the native intervals and used distinct
nonnull map keys with nullable values. The same release executable, verified
byte-for-byte, rechecked all eight changed statements. All 625 unchanged
case records were carried forward from the original preflight.

The 98 static exclusions include 69 placeholder registrations. The rest
require forms such as literal-only arguments, a session or partition context,
or an intentional error. The result table and manifest give each reason.

| Case | Preflight failure on this input |
| --- | --- |
| `map_entries` scalar | The native reference panicked while building a list with differing map-entry field names. |
| `aes_encrypt`, `aes_decrypt` | The input is a column array. The implementation expects one value. |
| `bitmap_and_agg`, `bitmap_construct_agg`, `bitmap_or_agg` windows | The encoding and comparison process reached the 12 GiB address-space limit. |
| `grouping` window | Neither execution plan implements this aggregate as a window. |
| `theta_intersection_agg` window | A frame with only NULL sketches cannot produce a result. |

The `try_aes_encrypt` and `try_aes_decrypt` timings cover the cost of
catching the scalar-only error and returning NULL for this column input.

## Cases excluded during timing

All scheduled configurations must complete. Where a timed cell reached a bound or failed validation, the raw samples preserve completed iterations and the result has no accepted timing. The 120-second case timer is cooperative and can return after the next yield. The subprocess has a separate 2,000-second hard deadline.

| Case | First timed failure |
| --- | --- |
| `scalar.xml.xpath` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_boolean` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_double` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_float` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_int` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_long` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_number` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_short` | spark-off, case_timeout. fixed 120-second case bound |
| `scalar.xml.xpath_string` | spark-off, case_timeout. fixed 120-second case bound |

The XPath implementation creates a document and constructs the path query per nonnull row. The path was constant on these inputs. [List form](../../vendor/sail/crates/sail-function/src/scalar/xml/xpath.rs#L117), [typed forms](../../vendor/sail/crates/sail-function/src/scalar/xml/xpath_typed.rs#L150).

## Validation and interpretation

The schedule used 450 fresh processes across 450 slots. The archive contains 19,764 retained samples. Each accepted mode has three process medians with three retained iterations apiece. Execution and projection are separately timed from stream creation through exhaustion. Small negative differences reflect the subtraction and run variation.

One local x86-64 Linux release build used Rust 1.96.0, an AMD Ryzen 7 8845HS, CPUs 2-5 and one DataFusion execution worker. The native A/A controls and the measured process spread set the effective floor for paired comparisons. The measurements describe these inputs, query shapes and machine.

- Product baseline `cde51da6dca2203f53dfe702dc06914931bd7845`
- Frozen protocol and receipt commit `24c951eaa7e419c85617ab69bc55623cce547b2f`
- Release executable SHA-256 `38cd15ca45110e61b3a684747a84113815e45b81027667cbe365caebaca706dc`
- Preflight receipt SHA-256 `01eb1a0a98dd89802565977b743b563f98440b08563ec75909ad920dbd9963dd`
