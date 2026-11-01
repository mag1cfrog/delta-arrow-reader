"""Capture extrema as contributing values enter and leave Spark window frames."""

import json
import os
from decimal import Decimal
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


INPUTS = [
    [0, 0, True, 3], [1, 0, False, 7], [2, 0, None, -2],
    [3, 0, True, None], [4, 0, True, None], [5, 0, True, 8],
    [6, 1, True, None], [7, 1, True, -3], [8, 1, False, 2],
    [9, 1, None, 1], [10, 1, True, None], [11, 1, True, 6],
]
TYPES = ["integer", "double", "string", "decimal(8,2)"]


def queries():
    frames = [
        ("rows", "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"),
        ("range", "ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW"),
        ("following", "ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING"),
        ("partition", "PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"),
        ("cumulative", "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
        ("current", "ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW"),
    ]
    for data_type in TYPES:
        table = "input_" + data_type.split("(")[0]
        for filtered in [False, True]:
            clause = " FILTER (WHERE keep)" if filtered else ""
            for name, frame in frames:
                yield f"{table}_{filtered}_{name}", filtered, (
                    f"SELECT id,MIN(value){clause} OVER ({frame}) AS minimum,"
                    f"MAX(value){clause} OVER ({frame}) AS maximum FROM {table} ORDER BY id"
                )


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-window-extrema")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for data_type in TYPES:
            schema = types.StructType.fromJson({"type": "struct", "fields": [
                {"name": name, "type": kind, "nullable": True, "metadata": {}}
                for name, kind in [("id", "integer"), ("g", "integer"),
                                   ("keep", "boolean"), ("value", data_type)]
            ]})
            convert = {"integer": int, "double": float, "string": str,
                       "decimal(8,2)": Decimal}[data_type]
            rows = [[i, g, keep, None if v is None else convert(v)] for i, g, keep, v in INPUTS]
            spark.createDataFrame(rows, schema).createOrReplaceTempView("input_" + data_type.split("(")[0])
        records = [{"spark_version": spark.version, "timezone": "UTC", "types": TYPES, "inputs": INPUTS}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, filtered, sql in queries():
                frame = spark.sql(sql)
                records.append({"id": name, "ansi": ansi, "filtered": filtered, "sql": sql,
                                "schema": frame.schema.jsonValue(),
                                "rows": [[v if v is None or isinstance(v, float) else str(v)
                                          for v in row] for row in frame.collect()]})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records) - 1
        Path(__file__).with_name("window-extrema-spark.jsonl").write_text(
            "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
