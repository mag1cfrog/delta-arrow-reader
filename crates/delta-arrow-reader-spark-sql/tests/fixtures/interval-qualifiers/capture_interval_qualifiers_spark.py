"""Capture Spark interval qualifiers, generated names and typed interval values.

The query corpus is the original #749 capture (candidate commit c54d99c3,
script SHA256 12cc8fd6...) plus review probes for NULL coercion, signs and
boundaries written inside interval literals and quoted unit strings, the
#951 storage limits of every interval qualifier, the #781 and #985 scaling of
year-month and day-time intervals by constant and column numbers, the #957
negation of every qualifier's limits, and the #765 casts between interval qualifiers. Values are recorded from
Spark's internal rows instead of string casts. Run from the crate root in the
reference environment described in README.md; the output path must not exist.
"""
import argparse
import decimal
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
INPUT_ROWS = [(0, 2), (1, 3), (2, None), (3, 5)]

INTERVALS = [
    ("year", "INTERVAL '2' YEAR", "INTERVAL '0' YEAR"),
    ("year_month", "INTERVAL '2-3' YEAR TO MONTH", "INTERVAL '0-0' YEAR TO MONTH"),
    ("month", "INTERVAL '27' MONTH", "INTERVAL '0' MONTH"),
    ("day", "INTERVAL '2' DAY", "INTERVAL '0' DAY"),
    ("day_hour", "INTERVAL '2 03' DAY TO HOUR", "INTERVAL '0 00' DAY TO HOUR"),
    ("day_minute", "INTERVAL '2 03:04' DAY TO MINUTE", "INTERVAL '0 00:00' DAY TO MINUTE"),
    ("day_second", "INTERVAL '2 03:04:05.123456' DAY TO SECOND",
     "INTERVAL '0 00:00:00' DAY TO SECOND"),
    ("hour", "INTERVAL '51' HOUR", "INTERVAL '0' HOUR"),
    ("hour_minute", "INTERVAL '51:04' HOUR TO MINUTE", "INTERVAL '0:00' HOUR TO MINUTE"),
    ("hour_second", "INTERVAL '51:04:05.123456' HOUR TO SECOND",
     "INTERVAL '0:00:00' HOUR TO SECOND"),
    ("minute", "INTERVAL '3064' MINUTE", "INTERVAL '0' MINUTE"),
    ("minute_second", "INTERVAL '3064:05.123456' MINUTE TO SECOND",
     "INTERVAL '0:00' MINUTE TO SECOND"),
    ("second", "INTERVAL '183845.123456' SECOND", "INTERVAL '0' SECOND"),
]


def queries():
    timestamp = "TIMESTAMP '2020-01-01 00:00:00.123456'"
    for name, interval, zero in INTERVALS:
        for kind, expression in [("literal", interval), ("zero", zero),
                                 ("negative", f"-({interval})")]:
            yield f"{name}_{kind}", "literal", f"SELECT {expression}"
        for operation, expression in [
            ("left_multiply", f"id*({interval})"),
            ("right_multiply", f"({interval})*id"),
            ("divide", f"({interval})/2"),
            ("add", f"({interval})+({zero})"),
        ]:
            yield f"{name}_{operation}", "arithmetic", f"SELECT {expression} FROM inputs"
        for operation, expression in [
            ("add", f"{timestamp}+({interval})"),
            ("reverse_add", f"({interval})+{timestamp}"),
            ("subtract", f"{timestamp}-({interval})"),
            ("computed", f"{timestamp}+id*({interval})"),
        ]:
            yield f"timestamp_{name}_{operation}", "timestamp", f"SELECT {expression} FROM inputs"
    for name, interval in [
        ("years", "INTERVAL 2 YEARS"), ("months", "INTERVAL 27 MONTHS"),
        ("days", "INTERVAL 2 DAYS"), ("hours", "INTERVAL 51 HOURS"),
        ("minutes", "INTERVAL 3064 MINUTES"), ("seconds", "INTERVAL 1.123456 SECONDS"),
        ("week", "INTERVAL 1 WEEK"), ("millisecond", "INTERVAL 1 MILLISECOND"),
        ("microsecond", "INTERVAL 1 MICROSECOND"),
        ("multi_ym", "INTERVAL 2 YEARS 3 MONTHS"),
        ("multi_zero_ym", "INTERVAL 0 YEARS 0 MONTHS"),
        ("multi_dt", "INTERVAL 2 DAYS 3 HOURS 4 MINUTES 5.123456 SECONDS"),
        ("multi_hour_minute", "INTERVAL 0 HOURS 0 MINUTES"),
        ("multi_zero_day_hour", "INTERVAL 0 DAYS 0 HOURS"),
        ("multi_cancel_ym", "INTERVAL 1 YEAR -12 MONTHS"),
        ("multi_cancel_dt", "INTERVAL 1 DAY -24 HOURS"),
        ("string", "INTERVAL '2 days 3 hours'"),
        ("mixed", "INTERVAL 1 MONTH 1 DAY"),
        ("mixed_zero", "INTERVAL 0 MONTHS 0 DAYS"),
        ("precision", "INTERVAL '0.123456789' SECOND"),
    ]:
        yield f"syntax_{name}", "syntax", f"SELECT {interval}"
    for name, expression in [
        ("day_hour", "INTERVAL 1 DAY + INTERVAL 1 HOUR"),
        ("hour_day", "INTERVAL 1 HOUR + INTERVAL 1 DAY"),
        ("year_month", "INTERVAL 1 YEAR + INTERVAL 1 MONTH"),
        ("month_year", "INTERVAL 1 MONTH - INTERVAL 1 YEAR"),
        ("fractional_multiply", "INTERVAL 1 DAY * 0.5"),
        ("fractional_divide", "INTERVAL 1 DAY / 3"),
        ("null_multiply", "INTERVAL 1 DAY * CAST(NULL AS INT)"),
    ]:
        yield f"arithmetic_{name}", "arithmetic", f"SELECT {expression}"
    for name, _, _ in INTERVALS:
        qualifier = name.replace("_", " TO ").upper()
        family = "YEAR TO MONTH" if name in ["year", "year_month", "month"] else "DAY TO SECOND"
        source = "INTERVAL '2-3' YEAR TO MONTH" if family == "YEAR TO MONTH" else (
            "INTERVAL '2 03:04:05.123456' DAY TO SECOND")
        yield f"cast_{name}", "cast", f"SELECT CAST({source} AS INTERVAL {qualifier})"
        yield f"cast_null_{name}", "cast", f"SELECT CAST(NULL AS INTERVAL {qualifier})"
    key = "TIMESTAMP '2020-01-01 00:00:00'+id*INTERVAL 1 DAY"
    for name, bound in [
        ("day", "INTERVAL 1 DAY"), ("hour", "INTERVAL 24 HOURS"),
        ("day_hour", "INTERVAL '1 00' DAY TO HOUR"),
        ("day_second", "INTERVAL '1 00:00:00' DAY TO SECOND"),
        ("year", "INTERVAL 1 YEAR"), ("month", "INTERVAL 1 MONTH"),
        ("numeric", "1"), ("mixed", "INTERVAL 1 MONTH 1 DAY"),
    ]:
        expression = f"SUM(v) OVER (ORDER BY {key} RANGE BETWEEN {bound} PRECEDING AND CURRENT ROW)"
        yield f"range_{name}", "window", f"SELECT id,{expression} FROM inputs"
        yield f"range_{name}_aliased", "window_alias", f"SELECT id,{expression} AS r FROM inputs"
    for name, bound in [("day", "INTERVAL 1 DAY"), ("hour", "INTERVAL 24 HOURS"),
                        ("day_hour", "INTERVAL '1 00' DAY TO HOUR"),
                        ("zero_year", "INTERVAL 0 YEAR")]:
        expression = ("SUM(v) OVER (ORDER BY DATE_ADD(DATE '2020-01-01',id) "
                      f"RANGE BETWEEN {bound} PRECEDING AND CURRENT ROW)")
        yield f"date_range_{name}", "date_window", f"SELECT id,{expression} AS r FROM inputs"


NULL_COERCION_PROBES = [
    ("null_left_multiply", "SELECT NULL * INTERVAL 1 DAY"),
    ("null_right_multiply", "SELECT INTERVAL 1 DAY * NULL"),
    ("typed_null_left_multiply", "SELECT CAST(NULL AS INT) * INTERVAL 1 DAY"),
    ("typed_null_right_multiply", "SELECT INTERVAL 1 DAY * CAST(NULL AS INT)"),
    ("timestamp_null_add", "SELECT TIMESTAMP '2020-01-01 00:00:00' + NULL"),
    ("null_timestamp_add", "SELECT NULL + TIMESTAMP '2020-01-01 00:00:00'"),
    ("timestamp_null_subtract", "SELECT TIMESTAMP '2020-01-01 00:00:00' - NULL"),
    ("timestamp_typed_interval_null_add",
     "SELECT TIMESTAMP '2020-01-01 00:00:00' + CAST(NULL AS INTERVAL DAY)"),
    ("timestamp_ntz_null_add", "SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00' + NULL"),
    ("interval_null_add", "SELECT INTERVAL '1' DAY + NULL"),
    ("null_timestamp_subtract", "SELECT NULL - TIMESTAMP '2020-01-01 00:00:00'"),
]

SIGNED_LITERAL_PROBES = [
    ("year_signed_value", "SELECT INTERVAL '-2' YEAR"),
    ("year_signed_string", "SELECT INTERVAL -'2' YEAR"),
    ("year_month_negative_months", "SELECT INTERVAL '-0-11' YEAR TO MONTH"),
    ("year_month_negative_years", "SELECT INTERVAL '-1-0' YEAR TO MONTH"),
    ("days_negative_unit", "SELECT INTERVAL -2 DAYS"),
    ("day_hour_mixed_signs", "SELECT INTERVAL -1 DAY 2 HOURS"),
    ("day_minute_negative", "SELECT INTERVAL '-1 02:03' DAY TO MINUTE"),
    ("second_negative_fraction", "SELECT INTERVAL '-00.000001' SECOND"),
    ("second_maximum", "SELECT INTERVAL '9223372036854.775807' SECOND"),
]

QUOTED_UNIT_PROBES = [
    ("zero_year_month", "SELECT INTERVAL '0 years 0 months'"),
    ("cancelled_day_hour", "SELECT INTERVAL '1 day -24 hours'"),
    ("mixed_zero_days", "SELECT INTERVAL '1 year 0 days'"),
    ("mixed_zero_months", "SELECT INTERVAL '0 months 5 days'"),
]

# Each qualifier at its storage limits and one unit past them (#951), and minimum
# qualified strings cast to their own type.
LIMIT_PROBES = [
    ("year_min", "INTERVAL '-178956970' YEAR"),
    ("year_max", "INTERVAL '178956970' YEAR"),
    ("year_below_min", "INTERVAL '-178956971' YEAR"),
    ("year_above_max", "INTERVAL '178956971' YEAR"),
    ("year_month_min", "INTERVAL '-178956970-8' YEAR TO MONTH"),
    ("year_month_min_outer_sign", "INTERVAL -'178956970-8' YEAR TO MONTH"),
    ("year_month_max", "INTERVAL '178956970-7' YEAR TO MONTH"),
    ("year_month_below_min", "INTERVAL '-178956970-9' YEAR TO MONTH"),
    ("year_month_above_max", "INTERVAL '178956970-8' YEAR TO MONTH"),
    ("month_min", "INTERVAL '-2147483648' MONTH"),
    ("month_min_outer_sign", "INTERVAL -'2147483648' MONTH"),
    ("month_max", "INTERVAL '2147483647' MONTH"),
    ("month_below_min", "INTERVAL '-2147483649' MONTH"),
    ("month_above_max", "INTERVAL '2147483648' MONTH"),
    ("months_min_units", "INTERVAL -2147483648 MONTHS"),
    ("years_months_min_units", "INTERVAL -178956970 YEARS -8 MONTHS"),
    ("months_min_quoted_units", "INTERVAL '-2147483648 months'"),
    ("day_min", "INTERVAL '-106751991' DAY"),
    ("day_max", "INTERVAL '106751991' DAY"),
    ("day_below_min", "INTERVAL '-106751992' DAY"),
    ("day_above_max", "INTERVAL '106751992' DAY"),
    ("day_hour_min", "INTERVAL '-106751991 04' DAY TO HOUR"),
    ("day_hour_max", "INTERVAL '106751991 04' DAY TO HOUR"),
    ("day_hour_below_min", "INTERVAL '-106751991 05' DAY TO HOUR"),
    ("day_hour_above_max", "INTERVAL '106751991 05' DAY TO HOUR"),
    ("day_minute_min", "INTERVAL '-106751991 04:00' DAY TO MINUTE"),
    ("day_minute_max", "INTERVAL '106751991 04:00' DAY TO MINUTE"),
    ("day_minute_below_min", "INTERVAL '-106751991 04:01' DAY TO MINUTE"),
    ("day_minute_above_max", "INTERVAL '106751991 04:01' DAY TO MINUTE"),
    ("day_second_min", "INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND"),
    ("day_second_max", "INTERVAL '106751991 04:00:54.775807' DAY TO SECOND"),
    ("day_second_below_min", "INTERVAL '-106751991 04:00:54.775809' DAY TO SECOND"),
    ("day_second_above_max", "INTERVAL '106751991 04:00:54.775808' DAY TO SECOND"),
    ("hour_min", "INTERVAL '-2562047788' HOUR"),
    ("hour_max", "INTERVAL '2562047788' HOUR"),
    ("hour_below_min", "INTERVAL '-2562047789' HOUR"),
    ("hour_above_max", "INTERVAL '2562047789' HOUR"),
    ("hour_minute_min", "INTERVAL '-2562047788:00' HOUR TO MINUTE"),
    ("hour_minute_max", "INTERVAL '2562047788:00' HOUR TO MINUTE"),
    ("hour_minute_below_min", "INTERVAL '-2562047788:01' HOUR TO MINUTE"),
    ("hour_minute_above_max", "INTERVAL '2562047788:01' HOUR TO MINUTE"),
    ("hour_second_min", "INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND"),
    ("hour_second_max", "INTERVAL '2562047788:00:54.775807' HOUR TO SECOND"),
    ("hour_second_below_min", "INTERVAL '-2562047788:00:54.775809' HOUR TO SECOND"),
    ("hour_second_above_max", "INTERVAL '2562047788:00:54.775808' HOUR TO SECOND"),
    ("minute_min", "INTERVAL '-153722867280' MINUTE"),
    ("minute_max", "INTERVAL '153722867280' MINUTE"),
    ("minute_below_min", "INTERVAL '-153722867281' MINUTE"),
    ("minute_above_max", "INTERVAL '153722867281' MINUTE"),
    ("minute_second_min", "INTERVAL '-153722867280:54.775808' MINUTE TO SECOND"),
    ("minute_second_max", "INTERVAL '153722867280:54.775807' MINUTE TO SECOND"),
    ("minute_second_below_min", "INTERVAL '-153722867280:54.775809' MINUTE TO SECOND"),
    ("minute_second_above_max", "INTERVAL '153722867280:54.775808' MINUTE TO SECOND"),
    ("second_min", "INTERVAL '-9223372036854.775808' SECOND"),
    ("second_min_outer_sign", "INTERVAL -'9223372036854.775808' SECOND"),
    ("second_below_min", "INTERVAL '-9223372036854.775809' SECOND"),
    ("second_above_max", "INTERVAL '9223372036854.775808' SECOND"),
    ("microseconds_min_units", "INTERVAL -9223372036854775808 MICROSECONDS"),
    ("seconds_min_units", "INTERVAL -9223372036854.775808 SECONDS"),
    ("seconds_min_quoted_units", "INTERVAL '-9223372036854.775808 seconds'"),
    ("cast_month_min", """CAST("INTERVAL '-2147483648' MONTH" AS INTERVAL MONTH)"""),
    ("cast_month_min_outer_sign", """CAST("INTERVAL -'2147483648' MONTH" AS INTERVAL MONTH)"""),
    ("cast_year_month_min",
     """CAST("INTERVAL '-178956970-8' YEAR TO MONTH" AS INTERVAL YEAR TO MONTH)"""),
    ("cast_day_second_min",
     """CAST("INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND" AS INTERVAL DAY TO SECOND)"""),
    ("cast_hour_second_min",
     """CAST("INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND" AS INTERVAL HOUR TO SECOND)"""),
    ("cast_minute_second_min", """CAST("INTERVAL '-153722867280:54.775808' MINUTE TO SECOND" """
                               """AS INTERVAL MINUTE TO SECOND)"""),
    ("cast_second_min", """CAST("INTERVAL '-9223372036854.775808' SECOND" AS INTERVAL SECOND)"""),
]


# Year-month intervals multiplied and divided by constant numbers (#781): the month count
# and the number's SQL. Each is multiplied in both operand orders and divided.
SCALING_PROBES = [
    ("tie", 27, "2"),
    ("negative_interval_tie", -27, "2"),
    ("negative_number_tie", 27, "-2"),
    ("negative_both_tie", -27, "-2"),
    ("below_tie", 25, "4"),
    ("above_tie", 27, "4"),
    ("negative_below_tie", -25, "4"),
    ("negative_above_tie", -27, "4"),
    ("tinyint", 27, "CAST(-2 AS TINYINT)"),
    ("smallint", 27, "CAST(4 AS SMALLINT)"),
    ("bigint", -27, "CAST(4 AS BIGINT)"),
    ("decimal_half", 3, "0.5"),
    ("decimal_negative_half", -3, "0.5"),
    ("double_half", 3, "CAST(0.5 AS DOUBLE)"),
    ("double_negative_half", -3, "CAST(0.5 AS DOUBLE)"),
    ("double_below_half", 1, "CAST(0.49999999999999994 AS DOUBLE)"),
    ("float_half", 3, "CAST(0.5 AS FLOAT)"),
    ("float_inexact", 5, "CAST(0.3 AS FLOAT)"),
    ("decimal_above_tie", 1, "CAST(0.50000000000000000000000000000000000001 AS DECIMAL(38,38))"),
    ("decimal_below_tie", 1, "CAST(0.49999999999999999999999999999999999999 AS DECIMAL(38,38))"),
    ("decimal_product_digits", 11,
     "CAST(0.95454545454545454545454545454545454545 AS DECIMAL(38,38))"),
    ("decimal_quotient_digits", 1, "CAST(2.0000000000000000000000000000000000001 AS DECIMAL(38,37))"),
    ("decimal_tiny", 1, "CAST(0.00000000000000000000000000000000000001 AS DECIMAL(38,38))"),
    ("decimal_wide", 2147483647, "CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0))"),
    ("maximum", 2147483647, "1"),
    ("minimum", -2147483648, "1"),
    ("int_overflow", 2147483647, "2"),
    ("tinyint_overflow", -2147483648, "CAST(2 AS TINYINT)"),
    ("bigint_int_overflow", 2147483647, "CAST(2 AS BIGINT)"),
    ("bigint_long_overflow", 2, "CAST(9223372036854775807 AS BIGINT)"),
    ("double_overflow", 2147483647, "CAST(2 AS DOUBLE)"),
    ("double_negative_overflow", -2147483648, "CAST(1.5 AS DOUBLE)"),
    ("decimal_overflow", 2147483647, "2.0"),
    ("minimum_negative_one", -2147483648, "-1"),
    ("minimum_negative_one_tinyint", -2147483648, "CAST(-1 AS TINYINT)"),
    ("minimum_negative_one_bigint", -2147483648, "CAST(-1 AS BIGINT)"),
    ("minimum_negative_one_decimal", -2147483648, "CAST(-1 AS DECIMAL(2,0))"),
    ("minimum_negative_one_double", -2147483648, "CAST(-1 AS DOUBLE)"),
    ("zero", 27, "0"),
    ("zero_bigint", 27, "CAST(0 AS BIGINT)"),
    ("zero_decimal", 27, "CAST(0 AS DECIMAL(2,1))"),
    ("zero_double", 27, "CAST(0 AS DOUBLE)"),
    ("negative_zero_double", 27, "CAST('-0.0' AS DOUBLE)"),
    ("nan", 1, "CAST('NaN' AS DOUBLE)"),
    ("infinity", 1, "CAST('Infinity' AS DOUBLE)"),
    ("negative_infinity", 1, "CAST('-Infinity' AS DOUBLE)"),
    ("typed_null", 27, "CAST(NULL AS INT)"),
    ("untyped_null", 27, "NULL"),
]

# NULL intervals, which skip the number, and errors in operands that Spark discards.
SCALING_NULL_PROBES = [
    ("null_interval_multiply", "CAST(NULL AS INTERVAL YEAR TO MONTH) * 2"),
    ("null_interval_divide_zero", "CAST(NULL AS INTERVAL YEAR TO MONTH) / 0"),
    ("null_interval_divide_error", "CAST(NULL AS INTERVAL YEAR TO MONTH) / (1 / 0)"),
    ("null_interval_multiply_error", "(1 / 0) * CAST(NULL AS INTERVAL YEAR TO MONTH)"),
    ("null_number_interval_error", "(INTERVAL '1' MONTH / 0) * CAST(NULL AS INT)"),
]

# Constant scaling errors in branches, filters and projections that Spark may prune.
SCALING_ERRORS = [
    ("divide_zero", "INTERVAL '1' MONTH / 0"),
    ("multiply_overflow", "INTERVAL '2147483647' MONTH * 2"),
    ("divide_minimum", "INTERVAL '-2147483648' MONTH / -1"),
]
SCALING_ERROR_CONTEXTS = [
    ("filtered", "SELECT {e} AS r FROM inputs WHERE false"),
    ("unused", "SELECT 1 AS r FROM (SELECT {e} AS x FROM inputs) t"),
    ("limit_zero", "SELECT {e} AS r FROM inputs LIMIT 0"),
    ("values", "SELECT x AS r FROM VALUES ({e}) AS t(x)"),
    ("values_filtered", "SELECT x AS r FROM VALUES ({e}) AS t(x) WHERE false"),
    ("constant_case", "SELECT CASE WHEN false THEN {e} ELSE INTERVAL '1' MONTH END AS r"),
    ("column_case",
     "SELECT CASE WHEN id < 0 THEN {e} ELSE INTERVAL '1' MONTH END AS r FROM inputs"),
    ("taken_column_case",
     "SELECT CASE WHEN id = 0 THEN {e} ELSE INTERVAL '1' MONTH END AS r FROM inputs"),
]

# Recorded provider columns for scaling: name, Spark type, nullability and values. Each
# interval column holds month counts.
SCALING_INPUTS = [
    ("scale_inputs", [
        ("m", "interval year to month", True), ("i", "integer", True), ("b", "byte", False),
        ("s", "short", False), ("l", "long", False), ("f", "float", False),
        ("d", "double", False), ("x", "decimal(10,2)", False),
        ("w", "decimal(38,37)", False), ("mo", "interval month", False),
    ], [
        [27, 2, 2, -2, 4, 0.5, -0.5, "0.50", "2.0000000000000000000000000000000000001", 27],
        [-27, 2, -4, 4, -4, 0.5, 0.5, "-0.50", "0.5000000000000000000000000000000000000", -27],
        [25, 4, 3, 3, 3, 0.3, 0.49999999999999994, "2.50", "1.0000000000000000000000000000000000000",
         25],
        [-1, 3, 3, -3, 3, 1.5, -1.5, "1.50", "1.5000000000000000000000000000000000000", -1],
        # A NULL interval skips its zero divisor.
        [None, 0, 1, 1, 1, 1.0, 1.0, "1.00", "1.0000000000000000000000000000000000000", 0],
        [27, None, 1, 1, 1, 1.0, 1.0, "1.00", "1.0000000000000000000000000000000000000", 1],
    ]),
    ("scale_limits", [
        ("high", "interval year to month", False), ("low", "interval year to month", False),
        ("zero", "integer", False), ("minus_one", "integer", False),
        ("long_minus_one", "long", False), ("long_max", "long", False),
        ("two", "integer", False), ("double_zero", "double", False),
        ("negative_zero", "double", False), ("nan", "double", False),
        ("decimal_zero", "decimal(2,1)", False),
    ], [
        [2147483647, -2147483648, 0, -1, -1, 9223372036854775807, 2, 0.0, -0.0, float("nan"),
         "0.0"],
    ]),
    # Day-time intervals hold microsecond counts (#985).
    ("dt_scale_inputs", [
        ("t", "interval day to second", True), ("i", "integer", True), ("b", "byte", False),
        ("s", "short", False), ("l", "long", False), ("f", "float", False),
        ("d", "double", False), ("x", "decimal(10,2)", False),
        ("w", "decimal(38,37)", False),
    ], [
        [27, 2, 2, -2, 4, 0.5, -0.5, "0.50", "2.0000000000000000000000000000000000001"],
        [-27, 2, -4, 4, -4, 0.5, 0.5, "-0.50", "0.5000000000000000000000000000000000000"],
        [25, 4, 3, 3, 3, 0.3, 0.49999999999999994, "2.50", "1.0000000000000000000000000000000000000"],
        [-1, 3, 3, -3, 3, 1.5, -1.5, "1.50", "1.5000000000000000000000000000000000000"],
        # A NULL interval skips its zero divisor.
        [None, 0, 1, 1, 1, 1.0, 1.0, "1.00", "1.0000000000000000000000000000000000000"],
        [27, None, 1, 1, 1, 1.0, 1.0, "1.00", "1.0000000000000000000000000000000000000"],
    ]),
    ("dt_scale_limits", [
        ("high", "interval day to second", False), ("low", "interval day to second", False),
        ("zero", "integer", False), ("minus_one", "integer", False),
        ("long_minus_one", "long", False), ("long_max", "long", False),
        ("two", "integer", False), ("double_zero", "double", False),
        ("negative_zero", "double", False), ("nan", "double", False),
        ("double_one", "double", False), ("decimal_zero", "decimal(2,1)", False),
    ], [
        [9223372036854775807, -9223372036854775808, 0, -1, -1, 9223372036854775807, 2, 0.0,
         -0.0, float("nan"), 1.0, "0.0"],
    ]),
]

SCALING_COLUMN_PROBES = [
    (f"{column}_{operation}", sql.format(n=column))
    for column in ["i", "b", "s", "l", "f", "d", "x", "w"]
    for operation, sql in [("left_multiply", "SELECT {n} * m AS r FROM scale_inputs"),
                           ("right_multiply", "SELECT m * {n} AS r FROM scale_inputs"),
                           ("divide", "SELECT m / {n} AS r FROM scale_inputs")]
] + [
    ("month_multiply", "SELECT mo * i AS r FROM scale_inputs"),
    ("month_divide", "SELECT mo / b AS r FROM scale_inputs"),
    ("constant_number", "SELECT m * 2, m / 2, 2 * m FROM scale_inputs"),
    ("constant_interval", "SELECT INTERVAL '2-3' YEAR TO MONTH / b AS r FROM scale_inputs"),
    ("unaliased", "SELECT m * i, i * m, m / i FROM scale_inputs"),
    ("timestamp", "SELECT TIMESTAMP '2020-01-01 00:00:00.123456' + m * i AS r FROM scale_inputs"),
    ("multiply_overflow", "SELECT high * two AS r FROM scale_limits"),
    ("long_overflow", "SELECT high * long_max AS r FROM scale_limits"),
    ("divide_zero", "SELECT high / zero AS r FROM scale_limits"),
    ("divide_double_zero", "SELECT high / double_zero AS r FROM scale_limits"),
    ("divide_negative_zero", "SELECT high / negative_zero AS r FROM scale_limits"),
    ("divide_decimal_zero", "SELECT high / decimal_zero AS r FROM scale_limits"),
    ("divide_minimum", "SELECT low / minus_one AS r FROM scale_limits"),
    ("divide_minimum_long", "SELECT low / long_minus_one AS r FROM scale_limits"),
    ("multiply_minimum", "SELECT low * minus_one AS r FROM scale_limits"),
    ("multiply_nan", "SELECT high * nan AS r FROM scale_limits"),
    ("filtered_divide_zero", "SELECT high / zero AS r FROM scale_limits WHERE false"),
    ("unused_divide_zero", "SELECT 1 AS r FROM (SELECT high / zero AS x FROM scale_limits) t"),
]

# Failing constant operands beside column operands, which Spark folds while optimizing even
# when no row reaches them, and NULL intervals that skip a number failing for that row.
SCALING_OPERAND_PROBES = [
    (f"{name}_{operation}{suffix}", f"SELECT {sql} AS r FROM scale_inputs{where}")
    for name, interval in [("divide_minimum", "(INTERVAL '-2147483648' MONTH / -1)"),
                           ("multiply_overflow", "(INTERVAL '2147483647' MONTH * 2)"),
                           ("divide_zero", "(INTERVAL '1' MONTH / 0)")]
    for operation, sql in [("left_multiply", f"i * {interval}"),
                           ("right_multiply", f"{interval} * i"),
                           ("divide", f"{interval} / b")]
    for suffix, where in [("", ""), ("_filtered", " WHERE false")]
] + [
    (f"number_divide_zero_{operation}{suffix}", f"SELECT {sql} AS r FROM scale_inputs{where}")
    for operation, sql in [("left_multiply", "(1 / 0) * m"), ("right_multiply", "m * (1 / 0)"),
                           ("divide", "m / (1 / 0)")]
    for suffix, where in [("", ""), ("_filtered", " WHERE false")]
] + [
    ("null_skips_divide", "SELECT m / (10 / i) AS r FROM scale_inputs"),
    ("null_skips_multiply", "SELECT (10 / i) * m AS r FROM scale_inputs"),
]


def seconds_literal(micros):
    """A SECOND interval literal for a microsecond count."""
    sign = "-" if micros < 0 else ""
    seconds, fraction = divmod(abs(micros), 1_000_000)
    return f"INTERVAL '{sign}{seconds}.{fraction:06d}' SECOND"


# Day-time intervals multiplied and divided by constant numbers (#985): the microsecond
# count and the number's SQL.
DT_SCALING_PROBES = [
    ("tie", 27, "2"),
    ("negative_interval_tie", -27, "2"),
    ("negative_number_tie", 27, "-2"),
    ("negative_both_tie", -27, "-2"),
    ("below_tie", 25, "4"),
    ("above_tie", 27, "4"),
    ("negative_above_tie", -27, "4"),
    ("tinyint", 27, "CAST(-2 AS TINYINT)"),
    ("smallint", 27, "CAST(4 AS SMALLINT)"),
    ("bigint", -27, "CAST(4 AS BIGINT)"),
    ("decimal_half", 3, "0.5"),
    ("decimal_negative_half", -3, "0.5"),
    ("double_half", 3, "CAST(0.5 AS DOUBLE)"),
    ("double_negative_half", -3, "CAST(0.5 AS DOUBLE)"),
    ("float_inexact", 5, "CAST(0.3 AS FLOAT)"),
    # FLOAT 0.7 widens to 0.699999988..., so 5 * 0.7 rounds to 3, not to the tie's 4.
    ("float_widened_tie", 5, "CAST(0.7 AS FLOAT)"),
    ("decimal_above_tie", 1, "CAST(0.50000000000000000000000000000000000001 AS DECIMAL(38,38))"),
    ("decimal_below_tie", 1, "CAST(0.49999999999999999999999999999999999999 AS DECIMAL(38,38))"),
    ("decimal_wide_product", 9223372036854775807,
     "CAST(0.99999999999999999999999999999999999999 AS DECIMAL(38,38))"),
    ("decimal_tiny", 1, "CAST(0.00000000000000000000000000000000000001 AS DECIMAL(38,38))"),
    ("maximum", 9223372036854775807, "1"),
    ("minimum", -9223372036854775808, "1"),
    ("long_overflow", 9223372036854775807, "2"),
    ("tinyint_overflow", -9223372036854775808, "CAST(2 AS TINYINT)"),
    ("double_overflow", 9223372036854775807, "CAST(2 AS DOUBLE)"),
    ("double_maximum", 9223372036854775807, "CAST(1 AS DOUBLE)"),
    ("decimal_overflow", 9223372036854775807, "2.0"),
    ("minimum_negative_one", -9223372036854775808, "-1"),
    ("minimum_negative_one_tinyint", -9223372036854775808, "CAST(-1 AS TINYINT)"),
    ("minimum_negative_one_decimal", -9223372036854775808, "CAST(-1 AS DECIMAL(2,0))"),
    ("minimum_negative_one_double", -9223372036854775808, "CAST(-1 AS DOUBLE)"),
    ("zero", 27, "0"),
    ("zero_decimal", 27, "CAST(0 AS DECIMAL(2,1))"),
    ("zero_double", 27, "CAST(0 AS DOUBLE)"),
    ("negative_zero_double", 27, "CAST('-0.0' AS DOUBLE)"),
    ("nan", 1, "CAST('NaN' AS DOUBLE)"),
    ("infinity", 1, "CAST('Infinity' AS DOUBLE)"),
    ("negative_infinity", 1, "CAST('-Infinity' AS DOUBLE)"),
    ("negative_zero_float", 27, "CAST('-0.0' AS FLOAT)"),
    ("typed_null", 27, "CAST(NULL AS INT)"),
    ("untyped_null", 27, "NULL"),
    ("day_qualifier", 93784000005, "2"),
]

DT_SCALING_NULL_PROBES = [
    ("null_interval_multiply", "CAST(NULL AS INTERVAL DAY TO SECOND) * 2"),
    ("null_interval_divide_zero", "CAST(NULL AS INTERVAL DAY) / 0"),
    ("null_interval_divide_error", "CAST(NULL AS INTERVAL HOUR) / (1 / 0)"),
    ("null_interval_multiply_error", "(1 / 0) * CAST(NULL AS INTERVAL SECOND)"),
    ("null_number_interval_error", "(INTERVAL '1' DAY / 0) * CAST(NULL AS INT)"),
]

DT_SCALING_ERRORS = [
    ("divide_zero", "INTERVAL '1' DAY / 0"),
    ("multiply_overflow", "INTERVAL '9223372036854.775807' SECOND * 2"),
    ("divide_minimum", "INTERVAL '-9223372036854.775808' SECOND / -1"),
]
DT_SCALING_ERROR_CONTEXTS = [(name, sql.replace("INTERVAL '1' MONTH", "INTERVAL '1' DAY"))
                             for name, sql in SCALING_ERROR_CONTEXTS]
# Spark folds NULL inspection of a required expression before it folds constants, so a
# failing constant under IS NOT NULL returns true (#957 review).
NULL_INSPECTION_CONTEXTS = [
    ("is_null", "SELECT ({e}) IS NULL AS r"),
    ("is_not_null", "SELECT ({e}) IS NOT NULL AS r"),
    ("column_is_not_null", "SELECT ({e}) IS NOT NULL AS r FROM inputs WHERE id = 0"),
]

DT_SCALING_COLUMN_PROBES = [
    (f"{column}_{operation}", sql.format(n=column))
    for column in ["i", "b", "s", "l", "f", "d", "x", "w"]
    for operation, sql in [("left_multiply", "SELECT {n} * t AS r FROM dt_scale_inputs"),
                           ("right_multiply", "SELECT t * {n} AS r FROM dt_scale_inputs"),
                           ("divide", "SELECT t / {n} AS r FROM dt_scale_inputs")]
] + [
    ("constant_number", "SELECT t * 2, t / 2, 2 * t FROM dt_scale_inputs"),
    ("untyped_null_left", "SELECT NULL * t AS r FROM dt_scale_inputs"),
    ("untyped_null_right", "SELECT t * NULL AS r FROM dt_scale_inputs"),
    ("untyped_null_divide", "SELECT t / NULL AS r FROM dt_scale_inputs"),
    ("constant_interval",
     "SELECT INTERVAL '1 02:03:04.000005' DAY TO SECOND / b AS r FROM dt_scale_inputs"),
    ("unaliased", "SELECT t * i, i * t, t / i FROM dt_scale_inputs"),
    ("timestamp",
     "SELECT TIMESTAMP '2020-01-01 00:00:00.123456' + t * i AS r FROM dt_scale_inputs"),
    ("multiply_overflow", "SELECT high * two AS r FROM dt_scale_limits"),
    ("long_overflow", "SELECT high * long_max AS r FROM dt_scale_limits"),
    ("double_maximum", "SELECT high * double_one AS r FROM dt_scale_limits"),
    ("divide_zero", "SELECT high / zero AS r FROM dt_scale_limits"),
    ("divide_double_zero", "SELECT high / double_zero AS r FROM dt_scale_limits"),
    ("divide_negative_zero", "SELECT high / negative_zero AS r FROM dt_scale_limits"),
    ("divide_decimal_zero", "SELECT high / decimal_zero AS r FROM dt_scale_limits"),
    ("divide_minimum", "SELECT low / minus_one AS r FROM dt_scale_limits"),
    ("divide_minimum_long", "SELECT low / long_minus_one AS r FROM dt_scale_limits"),
    ("multiply_minimum", "SELECT low * minus_one AS r FROM dt_scale_limits"),
    ("multiply_nan", "SELECT high * nan AS r FROM dt_scale_limits"),
    ("filtered_divide_zero", "SELECT high / zero AS r FROM dt_scale_limits WHERE false"),
    ("unused_divide_zero",
     "SELECT 1 AS r FROM (SELECT high / zero AS x FROM dt_scale_limits) t"),
]

DT_SCALING_OPERAND_PROBES = [
    (f"{name}_{operation}{suffix}", f"SELECT {sql} AS r FROM dt_scale_inputs{where}")
    for name, interval in [("divide_minimum", "(INTERVAL '-9223372036854.775808' SECOND / -1)"),
                           ("multiply_overflow", "(INTERVAL '9223372036854.775807' SECOND * 2)"),
                           ("divide_zero", "(INTERVAL '1' DAY / 0)")]
    for operation, sql in [("left_multiply", f"i * {interval}"),
                           ("right_multiply", f"{interval} * i"),
                           ("divide", f"{interval} / b")]
    for suffix, where in [("", ""), ("_filtered", " WHERE false")]
] + [
    (f"number_divide_zero_{operation}{suffix}", f"SELECT {sql} AS r FROM dt_scale_inputs{where}")
    for operation, sql in [("left_multiply", "(1 / 0) * t"), ("right_multiply", "t * (1 / 0)"),
                           ("divide", "t / (1 / 0)")]
    for suffix, where in [("", ""), ("_filtered", " WHERE false")]
] + [
    ("null_skips_divide", "SELECT t / (10 / i) AS r FROM dt_scale_inputs"),
    ("null_skips_multiply", "SELECT (10 / i) * t AS r FROM dt_scale_inputs"),
]


# Negation of each qualifier's storage limits (#957), as `-x` and `negative(x)`, of NULL, of
# column intervals including the limit tables, and of a failing minimum where Spark prunes
# or defers it.
NEGATION_LIMITS = [(name, interval) for name, interval in LIMIT_PROBES
                   if name.endswith(("_min", "_max")) and not name.startswith("cast_")
                   and "_below_" not in name and "_above_" not in name] + [
    # The signed-literal probes hold the SECOND maximum.
    ("second_max", "INTERVAL '9223372036854.775807' SECOND"),
]
NEGATION_PROBES = [
    ("null_year_month", "SELECT -(CAST(NULL AS INTERVAL YEAR TO MONTH)) AS r"),
    ("null_day_time", "SELECT negative(CAST(NULL AS INTERVAL DAY)) AS r"),
    ("column_year_month", "SELECT -m AS r FROM scale_inputs"),
    ("column_month", "SELECT negative(mo) AS r FROM scale_inputs"),
    ("column_day_time", "SELECT -t AS r FROM dt_scale_inputs"),
    ("column_day_time_function", "SELECT negative(t) AS r FROM dt_scale_inputs"),
    ("column_unaliased", "SELECT -m, negative(m) FROM scale_inputs"),
    ("column_year_month_maximum", "SELECT -high AS r FROM scale_limits"),
    ("column_year_month_minimum", "SELECT -low AS r FROM scale_limits"),
    ("column_day_time_maximum", "SELECT -high AS r FROM dt_scale_limits"),
    ("column_day_time_minimum", "SELECT -low AS r FROM dt_scale_limits"),
    ("column_minimum_filtered", "SELECT -low AS r FROM dt_scale_limits WHERE false"),
    ("timestamp", "SELECT TIMESTAMP '2020-01-01 00:00:00.123456' - (-t) AS r FROM dt_scale_inputs"),
    ("range_key_descending",
     "SELECT id, SUM(v) OVER (ORDER BY TIMESTAMP '2020-01-10 00:00:00' + (-(id * INTERVAL 1 DAY)) "
     "DESC RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS r FROM inputs"),
    ("range_key_ascending",
     "SELECT id, SUM(v) OVER (ORDER BY TIMESTAMP '2020-01-10 00:00:00' + (-(id * INTERVAL 1 DAY)) "
     "RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS r FROM inputs"),
]
NEGATION_MINIMA = [("year_month", "INTERVAL '-2147483648' MONTH", "INTERVAL '1' MONTH"),
                   ("day_time", "INTERVAL '-9223372036854.775808' SECOND", "INTERVAL '1' DAY")]
NEGATION_CONTEXTS = [
    ("filtered", "SELECT -({e}) AS r FROM inputs WHERE false"),
    ("unused", "SELECT 1 AS r FROM (SELECT -({e}) AS x FROM inputs) t"),
    ("limit_zero", "SELECT -({e}) AS r FROM inputs LIMIT 0"),
    ("values", "SELECT x AS r FROM VALUES (-({e})) AS t(x)"),
    ("constant_case", "SELECT CASE WHEN false THEN -({e}) ELSE {other} END AS r"),
    ("column_case", "SELECT CASE WHEN id < 0 THEN -({e}) ELSE {other} END AS r FROM inputs"),
    ("taken_column_case",
     "SELECT CASE WHEN id = 0 THEN -({e}) ELSE {other} END AS r FROM inputs"),
]


# Interval casts to every qualifier of the same family (#765): Spark keeps only the target
# range's units. Sources cover signs, zero, values below one target unit and the limits.
CAST_SOURCES = [
    ("year_month", ["'2-3'", "'-2-3'", "'0-11'", "'-0-11'", "'0-0'", "'178956970-7'",
                    "'-178956970-8'"], "YEAR TO MONTH", ["YEAR", "YEAR TO MONTH", "MONTH"]),
    ("day_time", ["'2 03:04:05.123456'", "'-2 03:04:05.123456'", "'0 00:00:00.000001'",
                  "'-0 23:59:59.999999'", "'106751991 04:00:54.775807'",
                  "'-106751991 04:00:54.775808'"], "DAY TO SECOND",
     ["DAY", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND", "HOUR", "HOUR TO MINUTE",
      "HOUR TO SECOND", "MINUTE", "MINUTE TO SECOND", "SECOND"]),
]
INTERVAL_CAST_PROBES = [
    ("null_day_time", "SELECT CAST(CAST(NULL AS INTERVAL DAY TO SECOND) AS INTERVAL HOUR) AS r"),
    ("null_year_month", "SELECT CAST(CAST(NULL AS INTERVAL YEAR TO MONTH) AS INTERVAL YEAR) AS r"),
    ("try_day_time", "SELECT TRY_CAST(INTERVAL '-2 03:04:05.123456' DAY TO SECOND AS INTERVAL "
                     "MINUTE) AS r"),
    ("try_year_month", "SELECT TRY_CAST(INTERVAL '-2-3' YEAR TO MONTH AS INTERVAL YEAR) AS r"),
    ("unaliased", "SELECT CAST(INTERVAL '-2 03:04:05.123456' DAY TO SECOND AS INTERVAL DAY), "
                  "CAST(INTERVAL '-2-3' YEAR TO MONTH AS INTERVAL YEAR)"),
    ("column_year", "SELECT CAST(m AS INTERVAL YEAR) AS r FROM scale_inputs"),
    ("column_month_year", "SELECT CAST(mo AS INTERVAL YEAR) AS r FROM scale_inputs"),
    ("column_limits_year", "SELECT CAST(high AS INTERVAL YEAR), CAST(low AS INTERVAL YEAR) "
                           "FROM scale_limits"),
    ("column_day", "SELECT CAST(t AS INTERVAL DAY) AS r FROM dt_scale_inputs"),
    ("column_limits_day", "SELECT CAST(high AS INTERVAL DAY), CAST(low AS INTERVAL HOUR), "
                          "CAST(low AS INTERVAL MINUTE TO SECOND) FROM dt_scale_limits"),
    ("column_scaled_minute", "SELECT CAST(t * 7000000 AS INTERVAL MINUTE) AS r "
                             "FROM dt_scale_inputs"),
    # Interval leaves of nested casts.
    ("nested_array", "SELECT CAST(array(INTERVAL '2 03:04:05.123456' DAY TO SECOND) "
                     "AS ARRAY<INTERVAL HOUR>)[0] AS r"),
    ("nested_struct", "SELECT CAST(named_struct('a', INTERVAL '2-3' YEAR TO MONTH) "
                      "AS STRUCT<a: INTERVAL YEAR>).a AS r"),
    ("nested_map", "SELECT CAST(map('k', INTERVAL '2 03:04' DAY TO MINUTE) "
                   "AS MAP<STRING, INTERVAL DAY>)['k'] AS r"),
    ("nested_column_array", "SELECT CAST(array(high) AS ARRAY<INTERVAL MINUTE>)[0] AS r "
                            "FROM dt_scale_limits"),
]


def interval_cast_definitions():
    for family, sources, source_qualifier, targets in CAST_SOURCES:
        for index, source in enumerate(sources):
            for target in targets:
                name = f"{family}_{index}_{target.lower().replace(' ', '_')}"
                yield (f"interval_cast_{name}", "interval_cast",
                       f"SELECT CAST(INTERVAL {source} {source_qualifier} AS INTERVAL {target}) "
                       "AS r")
    for name, sql in INTERVAL_CAST_PROBES:
        yield f"interval_cast_{name}", "interval_cast", sql


def negation_definitions():
    for name, interval in NEGATION_LIMITS:
        yield f"negation_{name}", "negation", f"SELECT -({interval}) AS r"
        yield f"negation_function_{name}", "negation", f"SELECT negative({interval}) AS r"
    for name, sql in NEGATION_PROBES:
        yield f"negation_{name}", "negation", sql
    for family, minimum, other in NEGATION_MINIMA:
        for context, sql in NEGATION_CONTEXTS:
            yield (f"negation_{family}_minimum_{context}", "negation_error",
                   sql.format(e=minimum, other=other))
        for context, sql in NULL_INSPECTION_CONTEXTS:
            yield (f"negation_{family}_minimum_{context}", "negation_error",
                   sql.format(e=f"-({minimum})"))
    for prefix, errors in [("scaling", SCALING_ERRORS), ("dt_scaling", DT_SCALING_ERRORS)]:
        for name, expression in errors:
            for context, sql in NULL_INSPECTION_CONTEXTS:
                yield (f"{prefix}_{name}_{context}", f"{prefix}_error",
                       sql.format(e=expression))


def dt_scaling_definitions():
    for name, micros, number in DT_SCALING_PROBES:
        interval = (seconds_literal(micros) if name != "day_qualifier"
                    else "INTERVAL '1 02:03:04.000005' DAY TO SECOND")
        for operation, expression in [("left_multiply", f"({number}) * ({interval})"),
                                      ("right_multiply", f"({interval}) * ({number})"),
                                      ("divide", f"({interval}) / ({number})")]:
            yield f"dt_scaling_{name}_{operation}", "dt_scaling", f"SELECT {expression} AS r"
    for name, expression in DT_SCALING_NULL_PROBES:
        yield f"dt_scaling_{name}", "dt_scaling", f"SELECT {expression} AS r"
    for name, expression in DT_SCALING_ERRORS:
        for context, sql in DT_SCALING_ERROR_CONTEXTS:
            yield f"dt_scaling_{name}_{context}", "dt_scaling_error", sql.format(e=expression)
    for name, sql in DT_SCALING_COLUMN_PROBES:
        yield f"dt_scaling_column_{name}", "dt_scaling_column", sql
    for name, sql in DT_SCALING_OPERAND_PROBES:
        yield f"dt_scaling_operand_{name}", "dt_scaling_operand", sql


def scaling_definitions():
    for name, months, number in SCALING_PROBES:
        interval = f"INTERVAL '{months}' MONTH"
        for operation, expression in [("left_multiply", f"({number}) * ({interval})"),
                                      ("right_multiply", f"({interval}) * ({number})"),
                                      ("divide", f"({interval}) / ({number})")]:
            yield f"scaling_{name}_{operation}", "scaling", f"SELECT {expression} AS r"
    for name, expression in SCALING_NULL_PROBES:
        yield f"scaling_{name}", "scaling", f"SELECT {expression} AS r"
    for name, expression in SCALING_ERRORS:
        for context, sql in SCALING_ERROR_CONTEXTS:
            yield f"scaling_{name}_{context}", "scaling_error", sql.format(e=expression)
    for name, sql in SCALING_COLUMN_PROBES:
        yield f"scaling_column_{name}", "scaling_column", sql
    for name, sql in SCALING_OPERAND_PROBES:
        yield f"scaling_operand_{name}", "scaling_operand", sql


def definitions():
    yield from queries()
    for name, sql in NULL_COERCION_PROBES:
        yield f"null_coercion_{name}", "null_coercion", sql
    for name, sql in SIGNED_LITERAL_PROBES:
        yield f"signed_literal_{name}", "signed_literal", sql
    for name, sql in QUOTED_UNIT_PROBES:
        yield f"quoted_units_{name}", "quoted_units", sql
    for name, interval in LIMIT_PROBES:
        yield f"limit_{name}", "limit", f"SELECT {interval}"
    yield from scaling_definitions()
    yield from dt_scaling_definitions()
    yield from negation_definitions()
    yield from interval_cast_definitions()


def encode_input(value, spark_type):
    """Encode a recorded input value: FLOAT and DOUBLE as big-endian bits in hex, other
    values, including interval month counts, as decimal strings."""
    if value is None:
        return None
    if spark_type in ("float", "double"):
        return struct.pack("!f" if spark_type == "float" else "!d", value).hex()
    return str(value)


def register_scaling_inputs(spark, types, functions):
    """Register the scaling providers as temporary views and describe them for the header."""
    plain = {"integer": types.IntegerType(), "byte": types.ByteType(),
             "short": types.ShortType(), "long": types.LongType(),
             "float": types.FloatType(), "double": types.DoubleType()}
    recorded = []
    for name, columns, rows in SCALING_INPUTS:
        fields, projections = [], []
        for column, spark_type, nullable in columns:
            if spark_type == "interval day to second":
                # Subtracting the epoch types the exact microsecond count as an interval.
                source_type = types.LongType()
                projection = f"timestamp_micros({column}) - TIMESTAMP '1970-01-01 00:00:00'"
                projections.append(functions.expr(projection).alias(column))
            elif spark_type.startswith("interval "):
                source_type = types.IntegerType()
                # make_ym_interval types the month count as INTERVAL YEAR TO MONTH.
                projection = (f"make_ym_interval(0, {column})"
                              if spark_type == "interval year to month"
                              else f"CAST({column} AS {spark_type.upper()})")
                projections.append(functions.expr(projection).alias(column))
            else:
                if spark_type.startswith("decimal("):
                    precision, scale = spark_type[8:-1].split(",")
                    source_type = types.DecimalType(int(precision), int(scale))
                else:
                    source_type = plain[spark_type]
                projections.append(functions.col(column))
            fields.append(types.StructField(column, source_type, nullable))
        values = [[decimal.Decimal(value) if isinstance(value, str) else value for value in row]
                  for row in rows]
        frame = spark.createDataFrame(values, types.StructType(fields)).select(*projections)
        declared = [[column, spark_type, nullable] for column, spark_type, nullable in columns]
        actual = [[field.name, field.dataType.jsonValue(), field.nullable]
                  for field in frame.schema.fields]
        assert actual == declared, (name, actual)
        frame.createOrReplaceTempView(name)
        recorded.append({"name": name, "schema": frame.schema.jsonValue(), "rows": [
            [encode_input(value, spark_type) for value, (_, spark_type, _) in zip(row, columns)]
            for row in rows]})
    return recorded


def encode_value(row, index, data_type, types, get_field):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, types.BooleanType):
        return "true" if row.getBoolean(index) else "false"
    if isinstance(data_type, (types.IntegerType, types.YearMonthIntervalType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.DayTimeIntervalType,
                              types.TimestampType, types.TimestampNTZType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.CalendarIntervalType):
        interval = row.getInterval(index)
        return [str(get_field(interval, field)) for field in ("months", "days", "microseconds")]
    raise AssertionError(f"unexpected result type {data_type}")


def error_condition(error, Py4JError):
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    while condition is None and cause is not None:
        try:
            condition = cause.getCondition()
        except Py4JError:
            pass
        cause = cause.getCause()
    return condition


def capture(spark, sql, types, get_field, Py4JError):
    record = {"schema_stages": {}}
    phase = "sql"
    try:
        frame = spark.sql(sql)
        execution = frame._jdf.queryExecution()
        record["schema_stages"]["analyzed"] = json.loads(execution.analyzed().schema().json())
        phase = "optimized"
        record["schema_stages"]["optimized"] = json.loads(execution.optimizedPlan().schema().json())
        phase = "collect"
        executed = execution.executedPlan()
        internal_rows = executed.executeCollect()
        record["schema_stages"]["executed"] = json.loads(executed.schema().json())
        fields = frame.schema.fields
        rows = [[encode_value(row, index, field.dataType, types, get_field)
                 for index, field in enumerate(fields)] for row in internal_rows]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        record["expected"] = {"status": "ok", "schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = error_condition(error, Py4JError)
        record["expected"] = {"status": "error", "condition": condition,
                              "message": str(error).splitlines()[0]}
        # PySpark converts most errors and keeps the Java throwable. A job that fails with a
        # plain Java exception reaches Python unconverted, and its first line names a py4j
        # object id, so describe its innermost cause instead.
        converted = getattr(error, "_origin", None)
        throwable = converted if converted is not None else getattr(error, "java_exception", None)
        assert throwable is not None, (sql, str(error))
        cause = throwable
        while cause.getCause() is not None:
            cause = cause.getCause()
        if converted is None:
            record["expected"]["message"] = cause.toString().splitlines()[0]
        if condition is None:
            # Some overflows raise a plain Java exception, which has no error condition;
            # record its class and message instead.
            record["expected"]["exception"] = cause.getClass().getName()
            record["expected"]["exception_message"] = cause.getMessage()
        record["spark_error_phase"] = phase
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output

    import pyspark
    from py4j.java_gateway import get_field
    from py4j.protocol import Py4JError
    from pyspark.sql import SparkSession, functions, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={SPARK_REVISION}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-interval-qualifiers")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField("id", types.IntegerType(), False),
                                   types.StructField("v", types.IntegerType(), True)])
        spark.createDataFrame(INPUT_ROWS, schema).createOrReplaceTempView("inputs")
        scaling_inputs = register_scaling_inputs(spark, types, functions)
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0], "timezone": "UTC",
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("integers, DATE days and YEAR-MONTH months as int; BIGINT, "
                               "DAY-TIME microseconds and TIMESTAMP UTC microseconds as long; "
                               "CALENDAR intervals as [months, days, microseconds]; "
                               "BOOLEAN as true or false"),
            "settings": {"spark.sql.optimizer.excludedRules":
                         spark.conf.get("spark.sql.optimizer.excludedRules")},
            # Recorded inputs encode integers as decimal strings.
            "inputs": [{"name": "inputs", "schema": schema.jsonValue(),
                        "rows": [[str(i), None if v is None else str(v)] for i, v in INPUT_ROWS]},
                       *scaling_inputs],
        }
        records = [header]
        cases = list(definitions())
        assert len({name for name, _, _ in cases}) == len(cases)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, context, sql in cases:
                records.append({"id": name, "context": context, "ansi": ansi, "sql": sql,
                                **capture(spark, sql, types, get_field, Py4JError)})
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({
            "observations": len(records) - 1,
            "errors": sum(r["expected"]["status"] == "error" for r in records[1:]),
            "output": str(args.output),
            "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
