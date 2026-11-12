"""Capture only empty STRUCT wildcard/IDENTIFIER positions in both ANSI modes."""
import json
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession, types
from capture_struct_expression_names_spark import capture_struct_query

REFERENCE = Path(__file__).with_name("struct-expression-names-spark.jsonl")

def main():
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name("struct-identifier-wildcard-spark.jsonl")
    assert not output.exists(), output
    header = json.loads(REFERENCE.read_text().splitlines()[0])
    provider = next(value for value in header["inputs"] if value["name"] == "struct_empty")
    assert provider["rows"] == []
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-struct-identifier-positions")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        spark.createDataFrame([], types.StructType.fromJson(provider["schema"])).createOrReplaceTempView("struct_empty")
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": [provider]}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, expression in [
                ("nested_star_identifier", "STRUCT(nested.*, IDENTIFIER('v'))"),
                ("qualified_star_identifier", "STRUCT(q.*, IDENTIFIER('v'))"),
            ]:
                sql = f"SELECT {expression} AS s FROM struct_empty q"
                records.append({"id": name, "input": "struct_empty", "ansi": ansi,
                                "sql": sql, "expected": capture_struct_query(spark, sql)})
        assert len(records) == 5
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records) - 1, "output": str(output)}))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
