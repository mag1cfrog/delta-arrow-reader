"""Capture unknown-column scopes and successful query controls with Spark 4.2.0."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from capture_floating_group_keys_spark import encode


def quote_identifier(value):
    return "`" + value.replace("`", "``") + "`"


def encode_input_value(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.TimestampType):
        return str(int(value.replace(tzinfo=timezone.utc).timestamp()) * 1_000_000)
    return encode(value, data_type, input_value=True)


def scope_columns(spark, queries):
    names = set()
    for sql in queries:
        output = spark.sql(sql)._jdf.queryExecution().analyzed().output()
        for index in range(output.size()):
            field = output.apply(index)
            qualifier = field.qualifier()
            parts = [str(qualifier.apply(i)) for i in range(qualifier.size())]
            parts.append(str(field.name()))
            names.add(".".join(quote_identifier(part) for part in parts))
    return names


def main():
    output = Path(sys.argv[1])
    assert not output.exists()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-unknown-columns")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0"
        spark.sparkContext.setLogLevel("ERROR")
        timestamp = datetime(2026, 1, 1)
        schema = types.StructType([
            types.StructField("k", types.IntegerType(), False),
            types.StructField("hour", types.IntegerType()),
            *[types.StructField(name, types.TimestampType()) for name in ["s", "e", "n", "day"]],
        ])
        definitions = [
            ("inputs", schema, [(1, 2, timestamp, timestamp, None, timestamp),
                                (2, None, None, timestamp, timestamp, None),
                                (2, 5, timestamp, None, timestamp, timestamp)]),
            ("lookup", types.StructType([types.StructField("id", types.IntegerType(), False),
                                         types.StructField("value", types.IntegerType())]), [(1, 10), (2, 20)]),
            ("odd", types.StructType([types.StructField(name, types.IntegerType())
                                      for name in ["a.b", "sp ace", "back`tick"]]), [(1, 2, 3)]),
        ]
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": []}]
        for table, schema, rows in definitions:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                "rows": [[encode_input_value(value, field.dataType) for value, field in zip(row, schema)]
                         for row in rows]})
        local = ["SELECT * FROM inputs"]
        outer = ["SELECT * FROM inputs q"]
        errors = [
            ("projection", "SELECT FOO FROM inputs", local, []),
            ("datediff", "SELECT k, DATEDIFF(FOO, hour, s) AS r FROM inputs", local, []),
            ("filter", "SELECT k FROM inputs WHERE missing > 0", local, []),
            ("qualified", "SELECT q.missing FROM inputs q", outer, []),
            ("join", "SELECT missing FROM inputs l JOIN lookup r ON l.k=r.id",
             ["SELECT * FROM inputs l JOIN lookup r ON l.k=r.id"], []),
            ("aggregate", "SELECT SUM(missing) FROM inputs", local, []),
            ("group_alias", "SELECT k AS known,COUNT(*) AS cnt FROM inputs GROUP BY missing",
             ["SELECT inputs.*,k AS known FROM inputs"], []),
            ("having", "SELECT k,COUNT(*) AS cnt FROM inputs GROUP BY k HAVING missing > 0",
             ["SELECT k,COUNT(*) AS cnt FROM inputs GROUP BY k"], []),
            ("subquery", "SELECT missing FROM (SELECT k AS key FROM inputs) q",
             ["SELECT * FROM (SELECT k AS key FROM inputs) q"], []),
            ("outer", "SELECT (SELECT missing) FROM inputs q", outer, []),
            ("local_outer", "SELECT (SELECT missing FROM lookup r WHERE r.id=q.k) FROM inputs q",
             outer + ["SELECT * FROM lookup r"], []),
            ("sort", "SELECT k AS key FROM inputs ORDER BY missing",
             ["SELECT k AS key FROM inputs"], []),
            ("sort_input_expression", "SELECT k AS key FROM inputs ORDER BY hour + missing", local, []),
            ("sort_alias_expression", "SELECT k AS key FROM inputs ORDER BY key + missing",
             ["SELECT k AS key FROM inputs"], []),
            ("using_left", "SELECT * FROM inputs JOIN lookup USING(missing)",
             ["SELECT k AS k,hour AS hour,s AS s,e AS e,n AS n,day AS day FROM inputs"], []),
            ("using_right", "SELECT * FROM inputs JOIN lookup USING(k)",
             ["SELECT id AS id,value AS value FROM lookup"], []),
            ("using_outer", "SELECT (SELECT COUNT(*) FROM inputs l JOIN lookup r USING(missing)) FROM (SELECT k AS outer_only FROM inputs) q",
             ["SELECT k AS k,hour AS hour,s AS s,e AS e,n AS n,day AS day FROM inputs"], []),
            ("lambda", "SELECT TRANSFORM(ARRAY(k), x -> missing) FROM inputs", local, ["x"]),
            ("lambda_no_input", "SELECT TRANSFORM(ARRAY(1), x -> missing)", [], ["x"]),
            ("empty_scope", "SELECT missing", [], []),
            ("quoted", "SELECT `lost.name` FROM odd w", ["SELECT * FROM odd w"], []),
            ("quoted_backtick", "SELECT w.`missing``field` FROM odd w", ["SELECT * FROM odd w"], []),
            ("closed_cte", "WITH unused AS (SELECT hour AS private_column FROM inputs) SELECT missing FROM (SELECT k AS public_key FROM inputs) q",
             ["SELECT * FROM (SELECT k AS public_key FROM inputs) q"], []),
        ]
        controls = [
            ("projection", "SELECT k,hour,s FROM inputs"),
            ("sort", "SELECT k AS key FROM inputs ORDER BY hour"),
            ("filter", "SELECT k FROM inputs WHERE hour > 0"),
            ("join", "SELECT l.k,r.value FROM inputs l JOIN lookup r ON l.k=r.id"),
            ("using", "SELECT k,value FROM inputs JOIN (SELECT id AS k,value FROM lookup) r USING(k)"),
            ("natural", "SELECT k,value FROM inputs NATURAL JOIN (SELECT id AS k,value FROM lookup) r"),
            ("aggregate", "SELECT k,COUNT(*) AS cnt FROM inputs GROUP BY k"),
            ("having", "SELECT k,COUNT(*) AS cnt FROM inputs GROUP BY k HAVING cnt > 1"),
            ("subquery", "SELECT key FROM (SELECT k AS key FROM inputs) q"),
            ("outer", "SELECT k,(SELECT r.value FROM lookup r WHERE r.id=q.k) AS amount FROM inputs q"),
            ("lambda", "SELECT TRANSFORM(ARRAY(k), x -> x + 1) AS result FROM inputs"),
            ("quoted", "SELECT w.`a.b`,w.`back``tick` FROM odd w"),
            ("closed_cte", "WITH unused AS (SELECT hour AS private_column FROM inputs) SELECT public_key FROM (SELECT k AS public_key FROM inputs) q"),
        ]
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql, scopes, parameters in errors:
                columns = scope_columns(spark, scopes) | {quote_identifier(name) for name in parameters}
                try:
                    spark.sql(sql).collect()
                except Exception as error:
                    condition = error.getCondition()
                    details = dict(error.getMessageParameters())
                    assert condition.startswith(("UNRESOLVED_COLUMN", "UNRESOLVED_USING_COLUMN")), (name, condition)
                    records.append({"id": name, "ansi": ansi, "sql": sql,
                        "unresolved_name": details.get("objectName", details.get("colName")), "columns": sorted(columns),
                        "spark_condition": condition, "spark_parameters": details})
                else:
                    raise AssertionError((name, "unknown column resolved"))
            for name, sql in [
                ("sort_known_type", "SELECT k AS key FROM inputs ORDER BY DATEDIFF(hour,s)"),
                ("sort_merged_type", "SELECT k AS key FROM inputs GROUP BY k ORDER BY key + SUM(ARRAY(hour))"),
            ]:
                try:
                    spark.sql(sql).collect()
                except Exception as error:
                    records.append({"id": name, "ansi": ansi, "sql": sql,
                        "known_input_error": True, "spark_condition": error.getCondition(),
                        "spark_parameters": dict(error.getMessageParameters())})
                else:
                    raise AssertionError((name, "Spark accepted invalid argument types"))
            for name, sql in controls:
                frame = spark.sql(sql)
                rows = [encode(row, frame.schema) for row in frame.collect()]
                ordered = name == "sort"
                if not ordered:
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                records.append({"id": "valid_" + name, "ansi": ansi, "sql": sql, "ordered": ordered,
                    "rows": rows, "schema": json.loads(frame._jdf.queryExecution().executedPlan().schema().json())})
        assert len(records) == 77
        with output.open("x") as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
