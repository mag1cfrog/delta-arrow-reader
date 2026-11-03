"""Capture string field selectors on arrays and computed scalar expressions."""

import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import encode
from capture_struct_field_resolution_spark import capture


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=32f7299601108917fb01920a54e084595b7b3bf8" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-struct-field-strings")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        item = types.StructField("item", types.IntegerType(), True, {"source": "item"})
        schema = types.StructType([
            types.StructField("k", types.IntegerType(), True),
            types.StructField("items", types.ArrayType(types.StructType([item]), True), True),
            types.StructField("duplicates", types.ArrayType(types.StructType([item, item]), True), True),
            types.StructField("numbers", types.ArrayType(types.IntegerType(), True), True),
        ])
        rows = [(3, [(4,), None], [(5, 6)], [7, 8]), (None, None, None, None)]
        tables = [("field_strings_mixed", rows), ("field_strings_nulls", [rows[-1]]),
                  ("field_strings_empty", [])]
        records = [{"spark_version": spark.version, "spark_build": build,
                    "timezone": "UTC", "case_sensitive": False, "inputs": []}]
        for table, values in tables:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema, input_value=True)
                                                  for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table, _ in tables:
                for name, expression in [
                    ("array_bracket_valid", "items['item']"),
                    ("array_bracket_missing", "items['missing']"),
                    ("array_bracket_duplicate", "duplicates['item']"),
                    ("computed_scalar_tail", "(k+1).missing"),
                    ("numeric_string_index", "numbers['1']"),
                ]:
                    sql = f"SELECT {expression} AS result FROM {table}"
                    records.append({"id": name, "input": table, "ansi": ansi,
                                    "sql": sql, **capture(spark, sql)})
        assert len(records) == 31
        assert len({(case["id"], case["input"], case["ansi"])
                    for case in records[1:]}) == 30
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"observations": len(records) - 1, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
