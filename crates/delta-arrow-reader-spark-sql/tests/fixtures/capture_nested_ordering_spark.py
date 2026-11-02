"""Capture independent ROW_NUMBER ordering with Spark 4.2.0, without RANGE."""
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode
from capture_nested_window_frames_spark import window_inputs


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-nested-ordering")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        definitions = []
        for table, key_type, values in window_inputs():
            if not table.endswith("mixed") or "_scalar_" in table:
                continue
            schema = types.StructType([types.StructField("k", key_type, True),
                                       types.StructField("id", types.IntegerType(), False)])
            rows = [(value, i) for i, value in enumerate(values)]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                      "rows": [encode(row, schema) for row in rows]})
            for index, direction in enumerate(["ASC NULLS FIRST", "ASC NULLS LAST",
                                               "DESC NULLS FIRST", "DESC NULLS LAST"]):
                sql = f"SELECT id,k,ROW_NUMBER() OVER (ORDER BY k {direction},id) AS position FROM {table}"
                definitions.append((f"{table}_order_{index}", sql))
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                expected = capture_query(spark, sql)
                assert "condition" not in expected, (name, expected)
                records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
        assert len(records) - 1 == 64
        Path(__file__).with_name("nested-ordering-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1, "inputs": len(metadata["inputs"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
