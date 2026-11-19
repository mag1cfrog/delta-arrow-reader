# Runtime parser inventory (#1389)

This reference records the source at staging
`298015f9def26cc97a271bdbc0316e64083d7874`. The measurement PR changes no product
source. Paths below are relative to `vendor/sail/crates/`. The inventory follows
the actual calls to both macros, their imports, UDF constructors and planner
delegates; the issue's initial list is not the search boundary.

## Macro entry points and construction

In [sail-sql-analyzer/src/parser.rs](../../vendor/sail/crates/sail-sql-analyzer/src/parser.rs),
`parse!` creates ParserOptions, a rich-error lexer and a rich-error token parser
at every call. `parse_simple!` creates a rich-error character parser at every
call. The complete macro invocation list is:

| Macro | Entry function | Reached during row execution |
| --- | --- | --- |
| `parse!` | `parse_data_type` | Schema setup once per JSON/XML/CSV batch; not once per data row |
| `parse!` | `parse_statements` | No; SQL admission/planning |
| `parse!` | `parse_object_name` | No; name resolution |
| `parse!` | `parse_interval_literal` | Yes, through the interval conversion chain below |
| `parse_simple!` | `parse_date` | Yes, through SparkDate's fallback |
| `parse_simple!` | `parse_timestamp` | Yes, through SparkTimestamp and SparkDateFormat |
| `parse_simple!` | `parse_time` | No; literal resolution and tests only |

`parse_interval` is not itself a macro call. Its full runtime chain is
`parse_interval -> parse_unqualified_interval_string -> parse_interval_string
-> parse_interval_literal -> parse!`, in parser.rs and
[sail-sql-analyzer/src/literal/interval.rs](../../vendor/sail/crates/sail-sql-analyzer/src/literal/interval.rs).
The `NonZeroValues` family rule is part of this runtime chain. SQL interval
literals use their separate written-unit rule.

## Direct row consumers

These are all production UDF consumers of those parser entry points. NULL
elements skip parsing. Ordinary, Large and View strings take the same parser;
dictionary strings are decoded by the planner where supported. A scalar input
may be folded during planning, so the shapes use columns.

| Row consumer in sail-function/src/scalar/datetime | Chain to macro | Shape |
| --- | --- | --- |
| [SparkDate::invoke_with_args](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_date.rs), `string_to_date32` | `parse_date -> parse_simple!` when the exact ten-byte ASCII date fast path does not succeed | `date_fallback` |
| [SparkTimestamp::invoke_with_args](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_timestamp.rs), `TimestampParser::string_to_microseconds` | `parse_timestamp -> parse_simple!`; LTZ localizes, NTZ keeps wall time | `timestamp_ltz`, `timestamp_ntz` |
| [SparkYearMonthInterval::invoke_with_args](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_interval.rs), `string_to_year_month_interval` | Full interval chain above | `interval_year_month` |
| Same file, `SparkDayTimeInterval::invoke_with_args`, `string_to_day_time_interval` | Full interval chain; result is Duration(Microsecond) | `interval_day_time` |
| Same file, `SparkCalendarInterval::invoke_with_args`, `string_to_calendar_interval` | Full interval chain; keeps mixed month/day/nanosecond fields | `interval_calendar` |
| [SparkDateFormat::invoke_with_args](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_date_format.rs), `format_string_array_as_timestamp`, `parse_string_array_to_timestamp`, `parse_timestamp_string` | STRING is parsed with `parse_timestamp -> parse_simple!` before display formatting | `date_format_string` |

SparkDate's shorter month/day, year-only, padded, and date-with-time forms all
reach the same fallback parser. PR 1 times a non-padded date. It does not claim
that this represents the performance distribution of every date grammar branch.
PR 2 must compare all forms directly with the original parser, including errors.
Likewise timestamp fractions/zones/offsets/leap seconds need differential tests,
not extra ordinary-input timing shapes in PR 1.

SparkDateFormat's scalar-pattern and dynamic-pattern array branches both parse
STRING values through `parse_timestamp_string`; a scalar STRING is parsed once
per invocation. Its DATE/TIMESTAMP branches format already typed values and do
not reach either macro. The Java output-pattern parser is a separate scanner,
not `parse!` or `parse_simple!`. The timed output pattern changes separators and
omits seconds so full result parity also demonstrates formatting.

## Transitive runtime paths

Every entry below builds an expression that delegates row conversion to a
consumer above. Aliases and surrounding operators do not add a distinct parser
implementation, so the seven shapes isolate the shared kernels instead of
timing each wrapper. This table includes conditional, nested and planner-added
paths, rather than restricting the inventory to public conversion functions.

| Planner source | Functions and SQL reachability | Row parser |
| --- | --- | --- |
| [resolver/expression/cast.rs](../../vendor/sail/crates/sail-plan/src/resolver/expression/cast.rs) | STRING CAST/TRY_CAST to DATE, TIMESTAMP/LTZ/NTZ and all three interval families | Corresponding direct consumer; interval wrappers currently share their parser regardless of TRY/ANSI flags |
| [function/scalar/conversion.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/conversion.rs) | `cast_to_date` / DATE, `cast_to_timestamp` / TIMESTAMP | Date or LTZ timestamp |
| [function/scalar/datetime.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/datetime.rs) | `to_date`, `try_to_date`, `to_date_with_parser`, `coerce_date_parser_argument` without input format | Date; other scalar types may first become Spark text |
| Same file | `to_timestamp`, `try_to_timestamp`, `timestamp_with_try`; TO_TIMESTAMP, TO_TIMESTAMP_LTZ, TO_TIMESTAMP_NTZ, TRY_TO_TIMESTAMP and configured default type, without format | LTZ/NTZ timestamp |
| Same file | `spark_date_argument`, `implicit_date_argument`, `date_field`, `last_day`, `trunc`, `interval_arithmetic`, `date_offset_by_days`, two-argument `dateadd`, `datediff_with_name`, `next_day`, date-field branch of `extract_part` | Date; ADD_YEARS, ADD_MONTHS, ADD_DAYS, DATE_ADD/DATEADD, DATE_SUB, DATEDIFF/DATE_DIFF, DAY/DAYOFMONTH, DAYOFWEEK, DAYOFYEAR, MONTH, QUARTER, YEAR, WEEKDAY, WEEKOFYEAR, LAST_DAY, TRUNC, NEXT_DAY, EXTRACT/DATE_PART/DATEPART |
| Same file | `string_timestamp_argument`, `time_field`, `date_trunc`, `months_between`, non-date branch of `extract_part` | LTZ timestamp; HOUR, MINUTE, DATE_TRUNC, MONTHS_BETWEEN, EXTRACT/DATE_PART/DATEPART |
| Same file | `timestampadd_with_name`, `timestampadd`, three-argument `dateadd`; `cast_timestamp_diff_argument`, `timestamp_diff_with_name`, `timestampdiff`, `timediff`, three-argument `datediff_with_name` | LTZ timestamp; TIMESTAMPADD, DATE_ADD/DATEADD, TIMESTAMPDIFF, TIMEDIFF, DATEDIFF/DATE_DIFF |
| Same file | `ntz_timestamp_and_unit`, `convert_timezone`; `utc_ntz_timestamp_and_unit`, `from_utc_timestamp`, `to_utc_timestamp` | NTZ or LTZ timestamp for STRING arguments |
| Same file | `date_format`, `date_format_with_args`, public DATE_FORMAT with STRING | SparkDateFormat parse-before-display path |
| [function/comparison.rs](../../vendor/sail/crates/sail-plan/src/function/comparison.rs), [comparison/string_temporal.rs](../../vendor/sail/crates/sail-plan/src/function/comparison/string_temporal.rs), [common.rs](../../vendor/sail/crates/sail-plan/src/function/common.rs) | `string_comparison_cast`, `SparkStringTemporalComparisonCast::try_new` / `invoke_with_args`, `spark_binary_expr`, `spark_in_list`; STRING/temporal comparisons and eligible ANSI DATE IN operands | Date or timestamp; comparison wrapper delegates rather than using a separate parser |
| [function/scalar/conditional/nullif.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/conditional/nullif.rs) | `SparkNullIf::invoke_with_args` calls `string_comparison_cast` on the reached comparison operands | Date or timestamp |
| [function/result_coercion.rs](../../vendor/sail/crates/sail-plan/src/function/result_coercion.rs), [scalar/conditional.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/conditional.rs) | `coerce_to_temporal`, `cast_result_value`, `coerce_values`, `coerce_result_values`, `coerce_if_result_values`, `coerce_coalesce_result_values`, `coerce_set_operation_result_values`; `case`, `if_expr`, `nvl2`, `coalesce` | Date/timestamp when result typing chooses temporal; ANSI STRING/temporal results parse, legacy results may instead choose STRING |
| [function/result_coercion/nested.rs](../../vendor/sail/crates/sail-plan/src/function/result_coercion/nested.rs) | `coerce_nested_result_values`, `cast_nested_arguments`, `cast_nested_value` / `cast_nested_value_impl`, `ResultCast::new_with_policy` builds leaf `cast_result_value`, then casts selected arrays; constructor alignment and offset-default alignment can reach the same leaf | Date/timestamp per reached nested STRING element |
| [resolver/query/set_op.rs](../../vendor/sail/crates/sail-plan/src/resolver/query/set_op.rs), [resolver/query/values.rs](../../vendor/sail/crates/sail-plan/src/resolver/query/values.rs) | Set-operation result coercion and VALUES temporal coercion delegate to the result helpers | Date/timestamp for runtime values; constants may fold |
| [function/scalar/array.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/array.rs), [scalar/map.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/map.rs), [resolver/expression/window.rs](../../vendor/sail/crates/sail-plan/src/resolver/expression/window.rs) | `sequence`, `sequence_cast`; ARRAY/MAP constructor alignment, `arrays_overlap` through nested casts, `coerce_offset_window_default` through `align_offset_window_arguments` | Sequence STRING endpoints and eligible nested STRING leaves use date/timestamp consumers |
| [function/scalar/math.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/math.rs) | `spark_plus`, `add_day_time_interval_to_string`; STRING + day-time interval, either argument order | LTZ timestamp, then interval addition and string display |

Some references to `coerce_to_temporal` cannot select its STRING branch:
`date_timestamp_common_type` accepts only DATE/TIMESTAMP/NULL; ARRAY's direct
temporal widening and IN's corresponding widening use that predicate.
`extremum_with_temporal_widening` also requires only temporal/NULL arguments.
Their nested/result-coercion branches remain listed where STRING leaves can
actually reach a parser. The listing distinguishes callers from reachable rows.

The plain `cast` inside `window` is a DataFusion expression, not a call to the
Spark resolver's CAST rule; its time-column cast does not reach these macros.
Its interval duration strings call `parse_interval` while planning.

## Other calls and exclusions

| Source | Calls and frequency | Treatment |
| --- | --- | --- |
| [scalar/json/from_json.rs](../../vendor/sail/crates/sail-function/src/scalar/json/from_json.rs) | `parse_schema_to_data_type` / `json_type_name_to_data_type` call analyzer `parse_data_type`; return-field planning and `from_json_inner` schema setup occur before `parse_rows` iterates | Once per batch/schema, not per input row; no additional row-parser shape |
| [scalar/xml/from_xml.rs](../../vendor/sail/crates/sail-function/src/scalar/xml/from_xml.rs) | `parse_xml_schema` in return-field planning and `spark_from_xml_inner`, before its XML-row loop | Same per-batch distinction |
| [scalar/csv/spark_from_csv.rs](../../vendor/sail/crates/sail-function/src/scalar/csv/spark_from_csv.rs) | `parse_fields` / local `parse_data_type` use analyzer `parse_data_type`; runtime schema is parsed before CSV-row iteration | Same per-batch distinction |
| [resolver/expression/literal.rs](../../vendor/sail/crates/sail-plan/src/resolver/expression/literal.rs) | `parse_date`, `parse_timestamp`, `parse_time` resolve literal values | Planning, not row conversion |
| Analyzer parser/query and planner query/read/misc | `parse_statements`, `parse_one_statement`, `parse_object_name`; generated SQL rewrites, table and identifier resolution | Ordinary SQL planning excluded |
| [function/scalar/datetime.rs](../../vendor/sail/crates/sail-plan/src/function/scalar/datetime.rs) | `window_interval_micros` calls `parse_interval` for duration literals | Planning, not row conversion |
| [scalar/datetime/spark_time.rs](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_time.rs) | `string_to_time_us_default` iterates Chrono formats; does not call analyzer `parse_time` | No macro path, no TIME shape |
| Date/time functions with explicit input format; JSON/XML/CSV date-time fields | DateTimeFormat/Chrono parsing, including local functions also named `parse_date` or `parse_timestamp` | Explicit input-format parsing is excluded and is not an analyzer-macro consumer |
| `to_char`, `to_varchar`, `from_unixtime` and typed DATE_FORMAT | Planner calls the date-format display helper with typed DATE/TIMESTAMP values; TO_CHAR over STRING instead selects numeric formatting | Display-only branches do not reparse STRING through these macros |

This inventory attributes source construction, not measured time inside Chumsky.
The report must keep query completion costs distinct from an exact kernel-cost
or additive decomposition claim. The [protocol](parsers-1389-protocol.md) fixes
the measurement and failure rules.
