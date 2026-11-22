# Source attribution for the final #1414 comparison

The final #1414 pair reported 41 slower mode comparisons across 29
statement forms. The [archive](functions-1515-original-1414-samples.jsonl.gz)
retains all 3,024 samples, including the faster and within-floor modes.

The runtime diff between `e1580ef0` and `51b0c8ea` changes 11 Sail
function modules. The planner and dependency forks have identical
source. The added paths implement the measured literal arguments in
STR_TO_MAP, HLL_UNION, THETA_UNION, the timestamp and interval
constructors, MONTHS_BETWEEN, TIMEDIFF, TIME_DIFF, BROUND and the
two TRY_AES forms. The retained
TIMEDIFF change also moved the arithmetic used by the existing
column/column timestamp difference.

The table follows the SQL selected for the 29 statements. "Untouched"
means that their traced evaluation paths keep the same source. The
shared-library build can still affect their timing. The repeat uses
the same historical binaries to check whether the shift recurs.

| Statement | Executed path | Retained edit |
| --- | --- | --- |
| BITMAP_CONSTRUCT_AGG | BitmapConstructAggFunction and its aggregate accumulator | Untouched |
| COUNT_MIN_SKETCH | CountMinSketchFunction and its aggregate accumulator | Untouched |
| HLL_SKETCH_AGG | HLL aggregate and shared sketch updates | Untouched. HLL_UNION changes a scalar caller. |
| KURTOSIS | KurtosisAccumulator | Untouched |
| THETA_SKETCH_AGG | Theta sketch aggregate and shared sketch updates | Untouched. THETA_UNION changes a scalar caller. |
| THETA_UNION_AGG | Theta union state merge in the aggregate | Untouched. The scalar union argument handling is separate. |
| POSEXPLODE_OUTER | Outer positional generator and explode implementation | Untouched |
| MAKE_DT_INTERVAL | SparkMakeDtInterval in datafusion-spark | Untouched. The edited constructors build different interval types. |
| TIME_TRUNC | SparkTimeTrunc | Untouched. It does not evaluate TIME_DIFF. |
| TIMESTAMPDIFF | SparkTimestampDiff array/array fallback and difference_in_local_time | TIMEDIFF literal-end edit at e8614487 |
| WINDOW | SparkWindowBuckets in the time-window lowering | Untouched |
| ST_ASBINARY | StAsBinary WKB projection | Untouched |
| JSON_ARRAY_LENGTH | json_array_length_udf | Untouched |
| JSON_OBJECT_KEYS | json_object_keys_udf | Untouched |
| EXISTS | SPARK_ARRAY_EXISTS_UDF and its predicate | Untouched |
| FORALL | SPARK_ARRAY_FORALL_UDF and its predicate | Untouched |
| MAP_FROM_ENTRIES | SparkMapEntries followed by SparkMapFromEntries | Untouched. STR_TO_MAP does not run. |
| BIN | SparkBin | Untouched |
| WIDTH_BUCKET | Locked datafusion-spark width bucket kernel | Untouched |
| HLL_SKETCH_ESTIMATE | HllSketchEstimateFunction and hll_sketch_estimate_inner | Untouched. The edited path is scalar HLL_UNION. |
| THETA_INTERSECTION | ThetaIntersectionFunction and theta_intersection_inner | Untouched. The edited path is scalar THETA_UNION. |
| ELT | SparkElt in datafusion-spark | Untouched |
| QUOTE | SparkQuote | Untouched |
| SOUNDEX | Soundex | Untouched |
| TRY_VALIDATE_UTF8 | Column projection. The physical TRY_CAST removes the same-type cast on the Utf8 input. | Untouched |
| VALIDATE_UTF8 | Column projection. The physical CAST removes the same-type cast on the Utf8 input. | Untouched |
| TRY_URL_DECODE | Dedicated URL decode UDF with tolerant error handling | Untouched |
| URL_DECODE | Dedicated URL decode UDF with strict error handling | Untouched |
| TRY_PARSE_JSON | SparkParseJson with tolerant error handling | Untouched |

The source comparison separates the two TIMESTAMPDIFF modes for
isolation. The other 39 modes go in one bounded fresh repeat. The
[protocol](functions-1515-protocol.md) states how both sessions reach a
decision and preserves the scope of the previously measured statements.
