"""Capture integral string CAST conditions, values and schemas with Spark 4.2.0."""

import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


os.environ["PYSPARK_PYTHON"] = sys.executable
inputs = []
for name, values, nullable in [
    ("valid", ["7", "-2"], False),
    ("nullable", ["7", None, "-2"], True),
    ("invalid", ["7", None, "bad"], True),
    ("nulls", [None], True),
    ("empty", [], True),
]:
    schema = types.StructType([
        types.StructField("s", types.StringType(), nullable, {"source": "input"})
    ])
    inputs.append({"name": name, "schema": schema.jsonValue(),
                   "rows": [[value] for value in values]})

queries = []
for kind in ["TINYINT", "SMALLINT", "INT", "BIGINT"]:
    for cast in ["CAST", "TRY_CAST"]:
        for name, value in [("valid", "'7'"), ("null", "CAST(NULL AS STRING)"),
                            ("invalid", "'bad'")]:
            queries.append((f"{kind.lower()}_{cast.lower()}_literal_{name}",
                            f"SELECT {cast}({value} AS {kind}) AS r"))
        for source in inputs:
            name = source["name"]
            queries.append((f"{kind.lower()}_{cast.lower()}_provider_{name}",
                            f"SELECT {cast}(s AS {kind}) AS r FROM {name}"))
        for name, sql in [
            ("literal_empty", f"SELECT {cast}('bad' AS {kind}) AS r WHERE false"),
            ("provider_empty_filter", f"SELECT {cast}(s AS {kind}) AS r FROM invalid WHERE false"),
            ("provider_limit_zero", f"SELECT {cast}(s AS {kind}) AS r FROM invalid LIMIT 0"),
            ("local_invalid", f"SELECT {cast}(s AS {kind}) AS r FROM VALUES ('bad') t(s)"),
        ]:
            queries.append((f"{kind.lower()}_{cast.lower()}_{name}", sql))
assert len(queries) == len({name for name, _ in queries})

spark = (SparkSession.builder.master("local[1]").appName("dar-integral-cast-conditions")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    for source in inputs:
        schema = types.StructType.fromJson(source["schema"])
        spark.createDataFrame(source["rows"], schema).createOrReplaceTempView(source["name"])
    records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            stage = "sql"
            try:
                frame = spark.sql(sql)
                stage = "schema"
                schema = frame.schema.jsonValue()
                stage = "collect"
                rows = [[None if value is None else str(value) for value in row]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                expected = {"schema": schema, "rows": rows}
                observed = {"stage": "complete"}
            except Exception as error:
                condition = error.getCondition()
                assert condition == "CAST_INVALID_INPUT", (name, condition, str(error))
                expected = {"condition": condition}
                observed = {"stage": stage, "message": str(error)}
            records.append({"id": name, "ansi": ansi, "sql": sql,
                            "expected": expected, "observed": observed})
    Path(__file__).with_name("integral-cast-conditions-spark.jsonl").write_text(
        "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n")
finally:
    spark.stop()
