"""Capture ordinary unnamed STRUCT children after wildcard expansion."""

import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query

REFERENCE = Path(__file__).with_name("struct-expression-names-spark.jsonl")


def main():
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name("review-757-ordinary-wildcard-spark-v1.jsonl")
    assert not output.exists(), output
    reference = json.loads(REFERENCE.read_text().splitlines()[0])
    provider = next(value for value in reference["inputs"] if value["name"] == "struct_empty")
    assert provider["rows"] == []
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-struct-ordinary-wildcard-review")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        spark.sparkContext.setLogLevel("ERROR")
        spark.createDataFrame([], types.StructType.fromJson(provider["schema"])).createOrReplaceTempView("struct_empty")
        records = [{"spark_version": spark.version, "spark_build": build, "timezone": "UTC", "inputs": [provider]}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, expression in [
                ("nested_star_literal", "STRUCT(nested.*, 1)"),
                ("qualified_star_abs", "STRUCT(q.*, ABS(v))"),
                ("nested_star_arithmetic", "STRUCT(nested.*, v + 1)"),
                ("nested_star_identifier_nested", "STRUCT(nested.*, IDENTIFIER('nested.item'))"),
            ]:
                sql = f"SELECT {expression} AS s FROM struct_empty q"
                records.append({"id": name, "input": "struct_empty", "ansi": ansi,
                                "sql": sql, "expected": capture_query(spark, sql)})
        assert len(records) == 9
        assert len({(case["id"], case["input"], case["ansi"]) for case in records[1:]}) == 8
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records) - 1, "spark_build": build, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
