"""Capture COUNT_IF predicate and SQL FILTER semantics with Spark 4.2.0."""
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
FIELDS = [("id", "integer"), ("g", "integer"), ("predicate", "boolean"),
          ("keep", "boolean"), ("raw", "string")]


def queries():
    for distinct in [False, True]:
        argument = "DISTINCT predicate" if distinct else "predicate"
        prefix = "distinct" if distinct else "ordinary"
        for label, filter_sql in [("none", ""), ("column", " FILTER (WHERE keep)"),
                                  ("true", " FILTER (WHERE true)"), ("false", " FILTER (WHERE false)"),
                                  ("null", " FILTER (WHERE CAST(NULL AS BOOLEAN))")]:
            expression = f"COUNT_IF({argument}){filter_sql}"
            yield f"{prefix}_{label}", f"SELECT {expression} AS n FROM inputs"
            yield f"{prefix}_{label}_groups", f"SELECT g,{expression} AS n FROM inputs GROUP BY g ORDER BY g"
        for label, source in [("empty", "empty_inputs"), ("all_null", "inputs WHERE predicate IS NULL"),
                              ("all_false", "inputs WHERE predicate=false")]:
            yield f"{prefix}_{label}", f"SELECT COUNT_IF({argument}) FILTER (WHERE keep) AS n FROM {source}"
        yield f"{prefix}_empty_groups", f"SELECT g,COUNT_IF({argument}) FILTER (WHERE keep) AS n FROM empty_inputs GROUP BY g ORDER BY g"
        yield f"{prefix}_having", (f"SELECT g,COUNT_IF({argument}) FILTER (WHERE keep) AS n FROM inputs "
                                   f"GROUP BY g HAVING COUNT_IF({argument}) FILTER (WHERE keep)>0 ORDER BY g")
        for label, filter_sql in [("masked", "keep"), ("required", "true"), ("null_filter", "NULL")]:
            argument = ("DISTINCT " if distinct else "") + "CAST(raw AS INT)>0"
            yield f"{prefix}_cast_{label}", f"SELECT COUNT_IF({argument}) FILTER (WHERE {filter_sql}) AS n FROM inputs"
        yield f"{prefix}_filter_error", (f"SELECT COUNT_IF({'DISTINCT ' if distinct else ''}false) "
                                         "FILTER (WHERE CAST(raw AS INT)>0) AS n FROM inputs")
        for group in [1, 2]:
            yield f"{prefix}_dynamic_filter_error_{group}", (f"SELECT COUNT_IF({'DISTINCT ' if distinct else ''}predicate) "
                f"FILTER (WHERE CAST(raw AS INT)>0) AS n FROM inputs WHERE g={group}")
        yield f"{prefix}_cast_masked_groups", (f"SELECT g,COUNT_IF({'DISTINCT ' if distinct else ''}CAST(raw AS INT)>0) "
                "FILTER (WHERE keep) AS n FROM inputs GROUP BY g ORDER BY g")
        for label, filter_sql in [("false", "false"), ("typed_null", "CAST(NULL AS BOOLEAN)")]:
            yield f"{prefix}_cast_filter_{label}", (f"SELECT COUNT_IF({'DISTINCT ' if distinct else ''}CAST(raw AS INT)>0) "
                f"FILTER (WHERE {filter_sql}) AS n FROM inputs")
    for label, filter_sql in [("false", "false"), ("folded_false", "1=2"), ("null", "CAST(NULL AS BOOLEAN)"), ("true", "true")]:
        yield f"literal_cast_filter_{label}", f"SELECT COUNT_IF(CAST('bad' AS INT)>0) FILTER (WHERE {filter_sql}) AS n FROM inputs"
    for label, predicate in [("false", "false"), ("null", "CAST(NULL AS BOOLEAN)")]:
        yield f"literal_filter_error_{label}", f"SELECT COUNT_IF({predicate}) FILTER (WHERE CAST('bad' AS INT)>0) AS n FROM inputs"
    for index, predicate in enumerate(["NOT true", "1=2", "CAST('false' AS BOOLEAN)", "NULL"]):
        yield f"folded_false_filter_{index}", f"SELECT COUNT_IF({predicate}) FILTER (WHERE CAST(raw AS INT)>0) AS n FROM inputs"
    for label, text in [("false", "false"), ("yes", " yes "), ("invalid", "bad")]:
        yield f"string_argument_{label}", f"SELECT COUNT_IF('{text}') FILTER (WHERE keep) AS n FROM inputs"
    for label, expression in [("integer", "1"), ("string", "'true'"), ("volatile", "rand()>0.5")]:
        yield f"invalid_filter_{label}", f"SELECT COUNT_IF(predicate) FILTER (WHERE {expression}) AS n FROM inputs"
    for label, argument, filter_sql in [("filter", "false", "SUM(id)>0"),
                                        ("predicate", "SUM(id)>0", "false"),
                                        ("window_filter", "false", "ROW_NUMBER() OVER (ORDER BY id)>0"),
                                        ("window_predicate", "ROW_NUMBER() OVER (ORDER BY id)>0", "false")]:
        yield f"nested_{label}", f"SELECT COUNT_IF({argument}) FILTER (WHERE {filter_sql}) AS n FROM inputs"
    yield "window_over_aggregate", "SELECT g,COUNT_IF(SUM(id)>0) OVER () AS n FROM inputs GROUP BY g ORDER BY g"
    yield "scalar_aggregate_subquery", "SELECT COUNT_IF((SELECT MAX(id) FROM inputs)>0) FILTER (WHERE false) AS n FROM inputs"
    yield "volatile_argument", "SELECT COUNT_IF(rand()>0.5) FILTER (WHERE keep) AS n FROM inputs"
    for expression in ["true", "false", "NULL", "CAST(NULL AS BOOLEAN)"]:
        label = expression.lower().replace(" ", "_").replace("(", "_").replace(")", "")
        yield f"constant_{label}", f"SELECT COUNT_IF({expression}) FILTER (WHERE keep) AS n FROM inputs"
    yield "mixed_aggregates", ("SELECT COUNT(*) AS rows,COUNT_IF(predicate) AS unfiltered,"
                               "COUNT_IF(predicate) FILTER (WHERE keep) AS filtered FROM inputs")
    yield "literal_rows", ("SELECT COUNT_IF(predicate) FILTER (WHERE keep) AS n FROM VALUES "
                           "(true,true),(true,false),(true,NULL),(false,true),(NULL,true) t(predicate,keep)")
    yield "window", ("SELECT id,COUNT_IF(predicate) OVER (PARTITION BY g ORDER BY id "
                     "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM inputs ORDER BY id")
    for label, expression in [("integer", "1"), ("string", "'true'"), ("no_arguments", ""),
                              ("extra_argument", "true,false")]:
        yield f"invalid_{label}", f"SELECT COUNT_IF({expression}) FILTER (WHERE keep) AS n FROM inputs"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-count-if")
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
                try:
                    frame = spark.sql(sql)
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(),
                                "rows": [[None if v is None else str(v) for v in row] for row in frame.collect()]}
                except Exception as error:
                    condition = getattr(error, "getCondition", lambda: None)()
                    cause = getattr(error, "java_exception", None)
                    while condition is None and cause is not None:
                        try:
                            condition = cause.getCondition()
                        except Py4JError:
                            pass
                        cause = cause.getCause()
                    assert condition is not None, (name, str(error))
                    expected = {"status": "error", "condition": condition}
                records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records) - 1
        Path(__file__).with_name("count-if-spark.jsonl").write_text(
            "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
