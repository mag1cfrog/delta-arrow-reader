"""Capture conditional result types, fallbacks and CASE admission with Spark 4.2.0."""

import datetime
import gzip
import json
import os
import re
from pathlib import Path
import sys

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable


def fallback_cases():
    for sql_type, text in [
        ("DATE", "2026-01-02"),
        ("TIMESTAMP", "2026-01-02 03:04:05.123456"),
        ("TIMESTAMP_NTZ", "2026-01-02 03:04:05.123456"),
    ]:
        temporal = f"CAST('{text}' AS {sql_type})"
        missing = f"CAST(NULL AS {sql_type})"
        for name, arguments, source in [
            ("null_left", f"{missing}, '{text}'", ""),
            ("string_left", f"'{text}', {missing}", ""),
            ("null_string", f"CAST(NULL AS STRING), {temporal}", ""),
            ("both_null", f"{missing}, CAST(NULL AS STRING)", ""),
            ("dead_invalid", f"{temporal}, 'bad'", ""),
            ("live_invalid", f"{missing}, 'bad'", ""),
            ("column", f"CAST(v AS {sql_type}), s", f" FROM VALUES (0, '{text}', 'bad'), (1, NULL, '{text}'), (2, NULL, NULL) t(k,v,s) ORDER BY k"),
            ("empty", f"CAST(v AS {sql_type}), s", f" FROM VALUES ('{text}', 'bad') t(v,s) WHERE false"),
        ]:
            yield f"{sql_type.lower()}_{name}", arguments, source
    yield "integer", "v, 9", " FROM VALUES (0, 3), (1, NULL) t(k,v) ORDER BY k"
    yield "string", "v, 'fallback'", " FROM VALUES (0, 'value'), (1, NULL) t(k,v) ORDER BY k"
    yield "same_date", "CAST(NULL AS DATE), DATE '2026-01-02'", ""
    yield "numeric_promotion", "CAST(NULL AS INT), 2D", ""


def case_condition_queries():
    queries = [
        ("case_untyped_null", "SELECT CASE WHEN NULL THEN 1 ELSE 2 END AS r"),
        ("case_numeric_condition", "SELECT CASE WHEN 1 THEN 1 ELSE 2 END AS r"),
        ("case_string_condition", "SELECT CASE WHEN 'true' THEN 1 ELSE 2 END AS r"),
        ("case_array_condition", "SELECT CASE WHEN ARRAY(true) THEN 1 ELSE 2 END AS r"),
        ("case_date_condition", "SELECT CASE WHEN DATE '2026-01-02' THEN 1 ELSE 2 END AS r"),
        ("case_integer_null", "SELECT CASE WHEN CAST(NULL AS INT) THEN 1 ELSE 2 END AS r"),
        ("case_invalid_later_condition", "SELECT CASE WHEN true THEN 1 WHEN NULL THEN 2 ELSE 3 END AS r"),
        ("case_untyped_column", "SELECT CASE WHEN v THEN 1 ELSE 2 END AS r FROM VALUES (NULL),(NULL) t(v)"),
        ("case_untyped_subquery", "SELECT CASE WHEN (SELECT NULL) THEN 1 ELSE 2 END AS r"),
        ("case_boolean_null", "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN 1 ELSE 2 END AS r"),
        ("case_boolean_expression", "SELECT CASE WHEN NULL AND true THEN 1 ELSE 2 END AS r"),
        ("case_boolean_column", "SELECT CASE WHEN v THEN 1 ELSE 2 END AS r FROM VALUES (0,true),(1,false),(2,CAST(NULL AS BOOLEAN)) t(k,v) ORDER BY k"),
        ("case_boolean_subquery", "SELECT CASE WHEN (SELECT true) THEN 1 ELSE 2 END AS r"),
        ("case_dead_cast", "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN CAST('bad' AS INT) ELSE 2 END AS r"),
        ("case_live_cast", "SELECT CASE WHEN true THEN CAST('bad' AS INT) ELSE 2 END AS r"),
        ("case_dead_subquery", "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN (SELECT CAST(7 AS INT) DIV CAST(0 AS INT)) ELSE 7L END AS r"),
        ("if_untyped_null", "SELECT IF(NULL,1,2) AS r"),
        ("if_untyped_column", "SELECT IF(v,1,2) AS r FROM VALUES (NULL),(NULL) t(v)"),
        ("simple_case_null_operand", "SELECT CASE NULL WHEN 1 THEN 1 ELSE 2 END AS r"),
        ("simple_case_null_match", "SELECT CASE 1 WHEN NULL THEN 1 ELSE 2 END AS r"),
        ("simple_case_boolean", "SELECT CASE v WHEN true THEN 1 ELSE 2 END AS r FROM VALUES (0,true),(1,false),(2,CAST(NULL AS BOOLEAN)) t(k,v) ORDER BY k"),
        ("case_ifnull_condition", "SELECT CASE WHEN IFNULL(CAST(NULL AS BOOLEAN),true) THEN 1 ELSE 2 END AS r"),
    ]
    seen = set()
    with gzip.open(Path(__file__).with_name("accepted-runtime.jsonl.gz"), "rt") as source:
        for line in source:
            record = json.loads(line)
            sql = record["sql"]
            if re.search(r"\bWHEN\s+NULL\b", sql, re.IGNORECASE) and sql not in seen:
                seen.add(sql)
                name = record["id"].rsplit("/", 1)[1].removesuffix("_true").removesuffix("_false")
                queries.append((f"historical_case_{name}", sql))
    assert len(seen) == 8
    return queries


def fallback_evaluation_queries():
    forms = [
        ('null_first', 'CAST(NULL AS INT),7', ''),
        ('non_null_first', '3,7', ''),
        ('both_null', 'CAST(NULL AS INT),CAST(NULL AS INT)', ''),
        ('dead_cast', "1,CAST('bad' AS INT)", ''),
        ('live_cast', "CAST(NULL AS INT),CAST('bad' AS INT)", ''),
        ('column', 'v,7', ' FROM VALUES (0,1),(1,CAST(NULL AS INT)),(2,3) t(k,v) ORDER BY k'),
        ('rowwise_cast', "v,IF(v IS NULL,7,CAST('bad' AS INT))", ' FROM VALUES (0,1),(1,CAST(NULL AS INT)),(2,3) t(k,v) ORDER BY k'),
        ('empty', "v,CAST('bad' AS INT)", ' FROM VALUES (1) t(v) WHERE false'),
        ('dead_subquery', '1,(SELECT v FROM VALUES (2),(3) t(v))', ''),
        ('live_subquery', 'CAST(NULL AS INT),(SELECT v FROM VALUES (2),(3) t(v))', ''),
    ]
    for function in ["COALESCE", "IFNULL", "NVL"]:
        for case, arguments, source in forms:
            yield f"eval_{function.lower()}_{case}", f"SELECT {function}({arguments}) AS r{source}"
    yield 'eval_coalesce_three_columns', 'SELECT COALESCE(a,b,c) AS r FROM VALUES (0,1,2,3),(1,NULL,2,3),(2,NULL,NULL,3),(3,NULL,NULL,NULL) t(k,a,b,c) ORDER BY k'
    yield 'eval_coalesce_boolean', 'SELECT COALESCE(a,b) AS r FROM VALUES (0,true,false),(1,NULL,false),(2,NULL,true),(3,NULL,NULL) t(k,a,b) ORDER BY k'
    yield 'eval_coalesce_untyped_nulls', 'SELECT COALESCE(NULL,NULL,NULL) AS r'
    yield 'eval_coalesce_one_argument', 'SELECT COALESCE(v) AS r FROM VALUES (0,1),(1,NULL) t(k,v) ORDER BY k'
    yield 'eval_coalesce_dead_divide', 'SELECT COALESCE(1,7 DIV 0) AS r'
    yield 'eval_coalesce_live_divide', 'SELECT COALESCE(CAST(NULL AS BIGINT),7 DIV 0) AS r'


def nullif_evaluation_queries():
    forms = [
        ("null_left_bad_cast", "CAST(NULL AS INT),CAST('bad' AS INT)", ""),
        ("live_bad_cast", "1,CAST('bad' AS INT)", ""),
        ("bad_left_null_right", "CAST('bad' AS INT),CAST(NULL AS INT)", ""),
        ("null_left_divide", "CAST(NULL AS BIGINT),7 DIV 0", ""),
        ("live_divide", "1L,7 DIV 0", ""),
        ("bad_left_divide", "7 DIV 0,CAST(NULL AS BIGINT)", ""),
        ("both_null", "CAST(NULL AS INT),CAST(NULL AS INT)", ""),
        ("right_null", "7,CAST(NULL AS INT)", ""),
        ("equal", "7,7", ""),
        ("unequal", "7,8", ""),
        ("rowwise_cast", "v,CAST(s AS INT)", " FROM VALUES (0,NULL,'bad'),(1,7,'7'),(2,8,'7'),(3,NULL,'bad') t(k,v,s) ORDER BY k"),
        ("rowwise_live_cast", "v,CAST(s AS INT)", " FROM VALUES (0,NULL,'7'),(1,7,'bad') t(k,v,s) ORDER BY k"),
        ("all_null", "v,CAST('bad' AS INT)", " FROM VALUES (CAST(NULL AS INT)),(NULL) t(v)"),
        ("non_null", "v,7", " FROM VALUES (0,7),(1,8) t(k,v) ORDER BY k"),
        ("empty", "v,CAST('bad' AS INT)", " FROM VALUES (1) t(v) WHERE false"),
        ("dead_subquery", "CAST(NULL AS INT),(SELECT v FROM VALUES (2),(3) t(v))", ""),
        ("live_subquery", "1,(SELECT v FROM VALUES (2),(3) t(v))", ""),
        ("bad_left_subquery", "(SELECT v FROM VALUES (2),(3) t(v)),CAST(NULL AS INT)", ""),
        ("null_left_subquery_cast", "CAST(NULL AS INT),(SELECT CAST(v AS INT) FROM VALUES ('bad') t(v))", ""),
        ("null_left_subquery_divide", "CAST(NULL AS BIGINT),(SELECT 7 DIV 0)", ""),
        ("null_left_subquery_values", "CAST(NULL AS INT),(SELECT v FROM VALUES (CAST('bad' AS INT)) t(v) WHERE false)", ""),
        ("null_left_subquery_empty", "CAST(NULL AS INT),(SELECT CAST('bad' AS INT) FROM VALUES (0) t(v) WHERE false)", ""),
        ("null_left_subquery_columns", "CAST(NULL AS INT),(SELECT 1,2)", ""),
        ("one_argument", "1", ""),
        ("three_arguments", "1,2,3", ""),
    ]
    for name, arguments, source in forms:
        yield f"eval_nullif_{name}", f"SELECT NULLIF({arguments}) AS r{source}"


def error_condition(error):
    if hasattr(error, "getCondition"):
        return error.getCondition()
    cause = error.java_exception
    for _ in range(8):
        if cause is None:
            break
        try:
            condition = cause.getCondition()
            if condition:
                return condition
        except Exception:
            pass
        cause = cause.getCause()
    raise RuntimeError("No structured Spark condition in exception chain") from error


def encode_reference_value(value, data_type):
    if isinstance(value, datetime.datetime):
        # PySpark collects TIMESTAMP in the process timezone; NTZ is wall time.
        if data_type == "timestamp":
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        elapsed = value - datetime.datetime(1970, 1, 1)
        return str((elapsed.days * 86400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds)
    if isinstance(value, bool):
        return str(value).lower()
    return None if value is None else str(value)


def conditional_sql(function, condition, left, right):
    if function == "IF":
        return f"IF({condition}, {left}, {right})"
    if function == "CASE":
        return f"CASE WHEN {condition} THEN {left} ELSE {right} END"
    return f"{function}({left}, {right})"


def mixed_temporal_queries(timezone):
    if timezone in {"UTC", "America/Phoenix"}:
        scenarios = [("ordinary", "2026-01-02 03:04:05.123456")]
        pairs = [("DATE", "TIMESTAMP"), ("DATE", "TIMESTAMP_NTZ"), ("TIMESTAMP_NTZ", "TIMESTAMP")]
        selections = ["left", "right", "both_null", "columns", "empty"]
    elif timezone == "America/New_York":
        scenarios = [("spring_gap", "2026-03-08 02:30:00.123456"), ("fall_overlap", "2026-11-01 01:30:00.123456")]
        pairs = [("TIMESTAMP_NTZ", "TIMESTAMP")]
        selections = ["left", "columns"]
    else:
        scenarios = [("fixed_offset", "2026-01-02 03:04:05.123456")]
        pairs = [("TIMESTAMP_NTZ", "TIMESTAMP")]
        selections = ["left"]
    for scenario, text in scenarios:
        for pair in pairs:
            for left_type, right_type in [pair, pair[::-1]]:
                left_value = f"CAST('{text[:10] if left_type == 'DATE' else text}' AS {left_type})"
                right_value = f"CAST('{text[:10] if right_type == 'DATE' else text}' AS {right_type})"
                left_null = f"CAST(NULL AS {left_type})"
                right_null = f"CAST(NULL AS {right_type})"
                for selection in selections:
                    left = left_value if selection == "left" else left_null
                    right = right_value if selection == "right" else right_null
                    condition = "true" if selection == "left" else "false"
                    suffix = ""
                    if selection in {"columns", "empty"}:
                        left, right, condition = "a", "b", "id=0"
                        count = 3 if selection == "columns" else 0
                        suffix = f" FROM (SELECT id, CASE WHEN id=0 THEN {left_value} ELSE {left_null} END AS a, CASE WHEN id=1 THEN {right_value} ELSE {right_null} END AS b FROM range({count})) t ORDER BY id"
                    for function in ["COALESCE", "IFNULL", "NVL", "IF", "CASE"]:
                        expression = conditional_sql(function, condition, left, right)
                        name = f"mixed_{scenario}_{left_type.lower()}_{right_type.lower()}_{selection}_{function.lower()}"
                        yield name, f"SELECT {expression} AS r{suffix}"
    if timezone in {"UTC", "America/Phoenix"}:
        for left_type in ["DATE", "TIMESTAMP_NTZ"]:
            left_value = f"CAST('2026-01-02 03:04:05.123456' AS {left_type})"
            other_value = f"CAST('2026-01-03 04:05:06.234567' AS {left_type})"
            for selection in ["unused", "required", "column_unused"]:
                left = f"CAST(NULL AS {left_type})" if selection == "required" else left_value
                condition = "false" if selection == "required" else "true"
                suffix = ""
                if selection == "column_unused":
                    left = "a"
                    condition = "id>=0"
                    suffix = f" FROM (SELECT id, CASE WHEN id=0 THEN {left_value} ELSE {other_value} END AS a FROM range(2)) t ORDER BY id"
                right = "CAST('bad' AS TIMESTAMP)"
                for function in ["COALESCE", "IFNULL", "NVL", "IF", "CASE"]:
                    expression = conditional_sql(function, condition, left, right)
                    name = f"mixed_lazy_{left_type.lower()}_timestamp_{selection}_{function.lower()}"
                    yield name, f"SELECT {expression} AS r{suffix}"


def capture_conditional_result(spark, sql):
    try:
        frame = spark.sql(sql)
        schema = frame.schema.jsonValue()
        rows = [[encode_reference_value(row[0], schema["fields"][0]["type"])] for row in frame.collect()]
        return dict(status="ok", schema=schema, rows=rows)
    except Exception as error:
        condition = error_condition(error)
        if condition not in {
            "CAST_INVALID_INPUT",
            "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
            "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
            "DIVIDE_BY_ZERO",
            "SCALAR_SUBQUERY_TOO_MANY_ROWS",
            "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
            "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION",
        }:
            raise
        return dict(status="error", condition=condition)


spark = (
    SparkSession.builder.master("local[1]")
    .appName("dar-conditional-reference")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)
records = []
try:
    assert spark.version == "4.2.0"
    spark.sparkContext.setLogLevel("ERROR")
    for timezone in ["UTC", "America/Phoenix"]:
        spark.conf.set("spark.sql.session.timeZone", timezone)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            queries = []
            for function in ["IFNULL", "NVL", "COALESCE"]:
                arguments_by_case = list(fallback_cases())
                if function != "COALESCE":
                    arguments_by_case += [(f"arity_{n}", ",".join(["NULL"] * n), "") for n in [0, 1, 3]]
                    if not ansi:
                        arguments_by_case += [
                            ("int_string", "CAST(NULL AS INT), '9'", ""),
                            ("int_bad_string", "CAST(NULL AS INT), 'bad'", ""),
                        ]
                else:
                    arguments_by_case += [
                        ("one_argument", "DATE '2026-01-02'", ""),
                        ("three_arguments", "CAST(NULL AS DATE), CAST(NULL AS STRING), '2026-01-02'", ""),
                        ("same_decimal", "CAST(NULL AS DECIMAL(10,2)), CAST(3.25 AS DECIMAL(10,2))", ""),
                    ]
                queries.extend(
                    (f"{function.lower()}_{name}", f"SELECT {function}({arguments}) AS r{query_suffix}")
                    for name, arguments, query_suffix in arguments_by_case
                )
            if timezone == "UTC":
                queries.extend(case_condition_queries())
                queries.extend(fallback_evaluation_queries())
                queries.extend(nullif_evaluation_queries())
            for name, sql in queries:
                expected = capture_conditional_result(spark, sql)
                records.append(dict(id=name, sql=sql, ansi=ansi, timezone=timezone, expected=expected))
    for timezone in ["UTC", "America/Phoenix", "America/New_York", "+05:45"]:
        spark.conf.set("spark.sql.session.timeZone", timezone)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in mixed_temporal_queries(timezone):
                expected = capture_conditional_result(spark, sql)
                records.append(dict(id=name, sql=sql, ansi=ansi, timezone=timezone, expected=expected))
    output = Path(__file__).with_name("conditional-spark.jsonl")
    metadata = dict(spark_version=spark.version, timestamp_encoding="microseconds since 1970-01-01; TIMESTAMP in UTC, TIMESTAMP_NTZ in wall time")
    output.write_text("\n".join(json.dumps(row, separators=(",", ":")) for row in [metadata, *records]) + "\n")
    print(json.dumps(dict(observations=len(records), errors=sum(row["expected"]["status"] == "error" for row in records))))
finally:
    spark.stop()
