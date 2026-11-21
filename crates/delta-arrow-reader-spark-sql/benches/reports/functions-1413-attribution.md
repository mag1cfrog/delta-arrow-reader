# Remaining paired function attribution (#1413)

All 37 frozen function cases match native in both ANSI modes on the
million-row fixture. Every execution consumed 1,000,000 input rows.
The 74 Spark-to-native comparisons match the complete rows and executed
types. The two grouping-set statements each produce 4,097 rows.
No function was excluded.

The [case inventory](functions-1413-attribution-cases.csv) records each
assignment, result type, and the hashes of the three actual plans.
The [profile schedule](../remaining_functions_1413/profile-groups.json)
orders the groups by the sum of their larger frozen ANSI gaps from #1391.
It picks the function with the largest gap as each representative.
That ranking chooses work order. It does not compare two builds.

| Group | Functions | Observed execution and source | CPU samples |
| --- | --- | --- | ---: |
| split-regex | split | Sail `SparkSplit` uses `spark_split_inner_downcast` and `split_to_array`. The profile shows regex split iteration, vector collection and allocation/free. The native splitter handles a delimiter directly. | 1,723 |
| string-view-casts | overlay, lpad, rpad | The plan casts stored strings to Utf8View before overlay and padding. The overlay profile shows Arrow byte-view construction and DataFusion `overlay_one`. Output construction also uses byte views, so this sample alone cannot apportion both costs. | 1,245 |
| regexp-literal | regexp, regexp_like, rlike | Sail `predicate::rlike` lowers all three names to a regex comparison. The DataFusion binary expression dispatch reaches Arrow `regexp_is_match_scalar` and regex search in the profile. Native simplifies the literal pattern to a contains predicate. | 883 |
| window-projections | row_number, stddev_samp | The plans use the same named ROW_NUMBER and STDDEV kernels. Sail scan and result aliases add projections; Spark and native also choose different NULL placement on the nonnull sort key. The profile shows sliding-window result work. It does not isolate the alias or sort contribution. | 4,538 |
| year-extraction | year | Sail `SparkYear::invoke_with_args` calls the fallible `date32_to_year` per stored date. Chrono day addition and date construction appear in the profile. Native uses DataFusion DatePartFunc. | 880 |
| output-aliases | char, repeat, replace, right | The same named CHR, REPEAT, REPLACE and RIGHT kernels have the same displayed arguments. `rename_physical_plan` adds a Spark result projection. DataFusion REPLACE is sampled; the profile does not isolate a separate runtime penalty for this alias layer. | 1,018 |
| ilike-lowercase | ilike | Sail `predicate::build_like_expr` lowercases both operands before SparkLike and wraps the Boolean in ILikeResult to retain nullability. Native ILIKE is one physical predicate. The profile is too broad to apportion the lowercase and predicate costs. | 880 |
| like-pattern | like | Sail `SparkLike::invoke_with_args` applies Spark wildcard and escape rules. Native uses an Arrow-backed physical LIKE. The profile does not isolate the extra validation or matching work from the driver. | 833 |
| log-domain | ln, log, log10, log1p, log2 | Sail `ln`, `log`, `log10`, `log1p` and `log2` add a positive-or-NULL CASE before the same named logarithm kernel. LOG1P also has a common-expression projection. The plan establishes the additional selection work. | 821 |
| percentile-reduction | median, percentile | Sail `median` and `percentile` both reach `NumericPercentileAccumulator`. The profile shows `apply_count` updating the value-frequency table. Both native controls use different reducers. | 841 |
| base64-decoder | unbase64 | Sail `SparkUnbase64::invoke_with_args` is sampled. It runs the tolerant decoder over a UTF-8 cast, while native DECODE reads BinaryView. The two decoders need different behavior on malformed input. | 1,039 |
| grouping-count-filter | grouping, grouping_id | Sail `rewrite_aggregate` adds a count and filter to preserve grouping-set behavior with empty input. The profile shows grouping hash work. It does not isolate the count and filter share. | 934 |
| trim-null-propagation | rtrim | Sail `NullPropagatingStringFunction` wraps the same native RtrimFunc. The physical expression text hides the wrapper; the executable UDF evidence shows it. | 828 |
| date-part-units | day, dayofyear, weekofyear, years | Both paths use DataFusion DatePartFunc with different spelling of the same unit and a Spark result projection. Chrono date construction is sampled. The profile does not isolate a cost for unit letter case or the projection. | 810 |
| ordered-minimum | min_by | Sail `function::aggregate::min_by` uses LastValue over reversed sort order and keeps the Spark filter. The frozen native control uses FirstValue with ascending order. The profile does not apportion the ordered reducer and filter costs. | 780 |
| regression-metadata | regr_intercept, regr_slope, regr_sxx, regr_syy | Both use the Regr reducer. SparkResultMetadata wraps a cast argument and Spark adds projections. The profile shows `RegrAccumulator::update_batch`; the metadata wrapper contribution is unresolved. | 795 |
| character-count | character_length | Sail dispatches to the locked DataFusion Spark `SparkLengthFunc`, which appears in the profile. Native uses CharacterLengthFunc. Spark also admits binary input through its length function. | 771 |

## Evidence

The 17 captures each use the pinned plain Linux release executable.
Profiles include fixture, planning and projection work. They identify
execution paths and complement the plan differences. Sample counts
do not measure the slowdown attributable to an individual node.
Matched release comparisons will quantify the effect of each trial.

- Staging source: `a044f1e58310f71195073f0a6616cb10693aaef1`.
- Profile protocol: `bfedaf24295579f8c3bb77aeb6a4901ea68573cc`.
- Functions executable SHA256: `8626775c9317b2e6479662ca98068e8ae81dfb2bedee9b913f50512a57668e8c`.
- Raw plan texts, result rows, source-guard records and perf captures
  are retained locally under `costs-168-2026-11-20/1413`.
