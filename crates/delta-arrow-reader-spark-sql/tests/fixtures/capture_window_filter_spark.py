"""Capture aggregate window FILTER results and admission with Spark 4.2.0."""
import itertools
import json
import os
from pathlib import Path
import sys

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types


ROWS = [[index, index // 3, predicate, keep, "1" if keep else "bad"]
        for index, (predicate, keep) in enumerate(itertools.product([True, False, None], repeat=2))]
ROWS.append([9, 0, True, True, "1"])
for row in ROWS:
    row.append(None if row[0] in [0, 9] else row[0])
FIELDS = [("id", "integer"), ("g", "integer"), ("predicate", "boolean"),
          ("keep", "boolean"), ("raw", "string"), ("value", "integer")]
FILTER_PRESERVING_RULE_EXCLUSIONS = {
    **{f"{name}_false": "org.apache.spark.sql.catalyst.optimizer.EliminateAggregateFilter"
       for name in ["count_if", "count", "sum"]},
    "first_value_prefix": "org.apache.spark.sql.catalyst.optimizer.OptimizeWindowFunctions",
}


def queries():
    windows = [
        ("all", ""), ("partition", "PARTITION BY g"),
        ("rows", "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"),
        ("range", "ORDER BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW"),
        ("following", "ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING"),
        ("cumulative", "PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
    ]
    for label, window in windows:
        for function, argument in [("COUNT_IF", "predicate"), ("COUNT", "*"),
                                   ("SUM", "value"), ("MIN", "value"), ("MAX", "value")]:
            yield f"{function.lower()}_{label}", f"SELECT id,{function}({argument}) FILTER (WHERE keep) OVER ({window}) AS n FROM inputs ORDER BY id"
    for name, expression in [
        ("avg", "AVG(value)"), ("first", "FIRST(value)"), ("first_value", "FIRST_VALUE(value)"),
        ("first_ignore_nulls", "FIRST(value,true)"), ("last", "LAST(value)"),
        ("last_value", "LAST_VALUE(value)"), ("last_ignore_nulls", "LAST(value,true)"),
        ("any_value", "ANY_VALUE(value)"), ("approx_count_distinct", "APPROX_COUNT_DISTINCT(value)"),
    ]:
        for label, window in [windows[2], windows[4]]:
            yield f"{name}_{label}", f"SELECT id,{expression} FILTER (WHERE keep) OVER ({window}) AS n FROM inputs ORDER BY id"
    for label, window in [windows[2], windows[4]]:
        yield f"hll_sketch_{label}", f"SELECT id,HLL_SKETCH_ESTIMATE(HLL_SKETCH_AGG(value) FILTER (WHERE keep) OVER ({window})) AS n FROM inputs ORDER BY id"
    for name in ["COUNT_IF", "COUNT", "SUM"]:
        argument = {"COUNT_IF":"predicate", "COUNT":"value", "SUM":"value"}[name]
        for label, filter_sql in [("true", "true"), ("false", "false"), ("null", "CAST(NULL AS BOOLEAN)")]:
            yield f"{name.lower()}_{label}", f"SELECT id,{name}({argument}) FILTER (WHERE {filter_sql}) OVER () AS n FROM inputs ORDER BY id"
    for name, expression in [("rank", "RANK()"), ("row_number", "ROW_NUMBER()"), ("lead", "LEAD(id)"),
                             ("nth_value", "NTH_VALUE(id,1)"), ("ntile", "NTILE(2)")]:
        yield f"invalid_function_{name}", f"SELECT {expression} FILTER (WHERE keep) OVER (ORDER BY id) AS n FROM inputs"
    for name, filter_sql in [("integer", "1"), ("string", "'true'"), ("null", "NULL"),
                             ("volatile", "rand()>0.5"), ("aggregate", "SUM(id)>0"),
                             ("window", "ROW_NUMBER() OVER (ORDER BY id)>0")]:
        yield f"invalid_filter_{name}", f"SELECT COUNT_IF(predicate) FILTER (WHERE {filter_sql}) OVER () AS n FROM inputs"
    for name, predicate in [("string", "'true'"), ("bad_string", "'bad'"), ("integer", "1"),
                            ("empty", ""), ("extra", "true,false"), ("true", "true"),
                            ("false", "false"), ("null", "CAST(NULL AS BOOLEAN)"),
                            ("volatile_true", "rand(10)>=0"), ("volatile_false", "rand(10)<0")]:
        yield f"argument_{name}", f"SELECT id,COUNT_IF({predicate}) FILTER (WHERE keep) OVER () AS n FROM inputs ORDER BY id"
    for label, predicate, filter_sql in [("cast_keep", "CAST(raw AS INT)>0", "keep"),
            ("cast_null", "CAST(raw AS INT)>0", "CAST(NULL AS BOOLEAN)"),
            ("filter_cast", "predicate", "CAST(raw AS INT)>0"),
            ("false_filter_cast", "false", "CAST(raw AS INT)>0"),
            ("literal_cast", "CAST('bad' AS INT)>0", "keep")]:
        yield label, f"SELECT id,COUNT_IF({predicate}) FILTER (WHERE {filter_sql}) OVER () AS n FROM inputs ORDER BY id"
    yield "distinct", "SELECT COUNT_IF(DISTINCT predicate) FILTER (WHERE keep) OVER () AS n FROM inputs"
    yield "empty_input", "SELECT id,COUNT_IF(predicate) FILTER (WHERE keep) OVER () AS n FROM empty_inputs ORDER BY id"
    yield "named_window", "SELECT id,COUNT_IF(predicate) FILTER (WHERE keep) OVER w AS n FROM inputs WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id"
    yield "over_grouped", "SELECT g,COUNT_IF(SUM(id)>0) FILTER (WHERE g>0) OVER () AS n FROM inputs GROUP BY g ORDER BY g"
    yield "mixed", "SELECT id,COUNT_IF(predicate) OVER () AS unfiltered,COUNT_IF(predicate) FILTER (WHERE keep) OVER () AS filtered FROM inputs ORDER BY id"
    for name in ["COLLECT_SET", "COLLECT_LIST", "ARRAY_AGG"]:
        yield name.lower(), f"SELECT id,ARRAY_JOIN(SORT_ARRAY({name}(value) FILTER (WHERE keep) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),',') AS n FROM inputs ORDER BY id"
        for label, window in [windows[2], windows[4]]:
            yield f"{name.lower()}_direct_{label}", f"SELECT id,{name}(value) FILTER (WHERE keep) OVER ({window}) AS n FROM inputs ORDER BY id"
        yield f"{name.lower()}_group", f"SELECT g,{name}(value) FILTER (WHERE keep) AS n FROM inputs GROUP BY g ORDER BY g"
    for table in ["inputs", "empty_inputs"]:
        yield f"approx_count_distinct_group_{table}", f"SELECT APPROX_COUNT_DISTINCT(value) FILTER (WHERE keep) AS n FROM {table}"
    yield "first_value_prefix", "SELECT id,FIRST_VALUE(value,true) FILTER (WHERE keep) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n FROM inputs ORDER BY id"
    frame = "ORDER BY id ROWS BETWEEN 8 PRECEDING AND CURRENT ROW"
    first_last = [f"{function}(value,{str(ignore_nulls).lower()}) FILTER (WHERE keep) OVER ({frame}) AS {function.lower()}_{'ignore' if ignore_nulls else 'respect'}"
                  for function in ["FIRST_VALUE", "LAST_VALUE"] for ignore_nulls in [False, True]]
    yield "first_last_multiple_values", f"SELECT id,{','.join(first_last)} FROM inputs ORDER BY id"
    for name, expression in [("avg", "AVG(NULL)"), ("listagg", "LISTAGG(NULL, ',')")]:
        yield f"null_argument_{name}_filter_cast", f"SELECT id,{expression} FILTER (WHERE CAST(raw AS INT)>0) OVER () AS n FROM inputs ORDER BY id"
        yield f"null_argument_{name}_unfiltered", f"SELECT id,{expression} OVER () AS n FROM inputs ORDER BY id"


def encode_reference_value(value):
    if value is None or isinstance(value, float):
        return value
    if isinstance(value, list):
        return [encode_reference_value(element) for element in value]
    return str(value)


def capture_query(spark, sql):
    try:
        frame = spark.sql(sql)
        return {"status": "ok", "schema": frame.schema.jsonValue(),
                "rows": [[encode_reference_value(v) for v in row] for row in frame.collect()]}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        assert condition is not None, (sql, str(error))
        return {"status": "error", "condition": condition}


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-window-filter")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType.fromJson({"type": "struct", "fields": [
            {"name": name, "type": kind, "nullable": True, "metadata": {}} for name, kind in FIELDS]})
        for name, rows in [("inputs", ROWS), ("empty_inputs", [])]:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
        records = [{"spark_version": spark.version, "timezone": "UTC", "fields": FIELDS, "rows": ROWS}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in queries():
                record = {"id": name, "ansi": ansi, "sql": sql, "expected": capture_query(spark, sql)}
                if name in FILTER_PRESERVING_RULE_EXCLUSIONS:
                    rule = FILTER_PRESERVING_RULE_EXCLUSIONS[name]
                    if record["expected"]["status"] == "ok":
                        record["optimized_plan"] = spark.sql(sql)._jdf.queryExecution().optimizedPlan().toString()
                    spark.conf.set("spark.sql.optimizer.excludedRules", rule)
                    try:
                        record["filter_preserving_reference"] = {
                            "excluded_rule": rule,
                            "expected": capture_query(spark, sql),
                            "optimized_plan": spark.sql(sql)._jdf.queryExecution().optimizedPlan().toString(),
                        }
                    finally:
                        spark.conf.set("spark.sql.optimizer.excludedRules", "")
                records.append(record)
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records) - 1
        Path(__file__).with_name("window-filter-spark.jsonl").write_text(
            "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
