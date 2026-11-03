"""Capture the reviewed NullType and Map extraction boundaries on pinned Spark."""

import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession

from capture_struct_field_resolution_spark import capture


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=32f7299601108917fb01920a54e084595b7b3bf8" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-review-struct-null-boundaries")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "spark_build": build,
                    "timezone": "UTC", "case_sensitive": False, "inputs": []}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in [
                ("null_local_base", "SELECT s.missing AS result FROM (SELECT NULL AS s) q"),
                ("null_computed_base", "SELECT (NULL).missing AS result"),
                ("null_lambda_base", "SELECT TRANSFORM(ARRAY(NULL),x -> x.missing) AS result"),
                ("map_lambda_base", "SELECT TRANSFORM(ARRAY(MAP('key',1)),x -> x.key) AS result"),
                ("map_nullable_lambda_base", "SELECT TRANSFORM(ARRAY(MAP('key',1),NULL),x -> x.key) AS result"),
                ("map_struct_case_lambda", "SELECT TRANSFORM(ARRAY(MAP('key',NAMED_STRUCT('Item',1)),NULL),x -> x.key.item) AS result"),
                ("map_struct_missing_lambda", "SELECT TRANSFORM(ARRAY(MAP('key',NAMED_STRUCT('other',1)),NULL),x -> x.key.item) AS result"),
                ("map_struct_duplicate_lambda", "SELECT TRANSFORM(ARRAY(MAP('key',NAMED_STRUCT('item',1,'ITEM',2)),NULL),x -> x.key.item) AS result"),
            ]:
                records.append({"id": name, "input": "constant", "ansi": ansi,
                                "sql": sql, **capture(spark, sql)})
        assert len(records) == 17
        assert len({(case["id"], case["input"], case["ansi"])
                    for case in records[1:]}) == 16
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"observations": 16, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
