"""Independently capture ordinary NULL attribute evaluation on pinned Spark."""
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

from capture_null_base_extraction_spark import capture


def cases():
    yield {"id": "projected_raise", "input": "constant", "sql": "SELECT s.missing AS result FROM (SELECT RAISE_ERROR('null-base-required') AS s) q"}
    for table in ["null_mixed", "null_nulls", "null_empty"]:
        yield {"id": "projected_case", "input": table, "sql": f"SELECT s.missing AS result FROM (SELECT CASE WHEN id=0 THEN RAISE_ERROR('null-base-required') ELSE NULL END AS s FROM {table}) q"}
    yield {"id": "projected_nested_raise", "input": "constant", "sql": "SELECT r.n.missing AS result FROM (SELECT NAMED_STRUCT('n',RAISE_ERROR('null-base-required')) AS r) q"}


def main():
    import pyspark
    from pyspark.sql import SparkSession, types
    from capture_floating_group_keys_spark import encode

    output = Path(sys.argv[1])
    assert not output.exists(), output
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=32f7299601108917fb01920a54e084595b7b3bf8" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-null-attribute-evaluation-815")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        null_child = types.StructType([
            types.StructField("n", types.NullType(), True, {"source": "null_child"})])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("s", types.NullType(), True, {"source": "null_column"}),
            types.StructField("r", null_child, True),
            types.StructField("numbers", types.ArrayType(types.IntegerType(), True), True),
            types.StructField("records", types.ArrayType(null_child, True), True),
            types.StructField("nulls", types.ArrayType(types.NullType(), True), True),
        ])
        mixed = [(0, None, (None,), [1, None], [(None,), None], [None, None]),
                 (1, None, None, None, None, None),
                 (2, None, (None,), [], [], [])]
        nulls = [(3, None, None, None, None, None)]
        metadata = {"spark_version": spark.version, "spark_build": build,
                    "timezone": "UTC", "case_sensitive": False, "inputs": []}
        for name, rows in [("null_mixed", mixed), ("null_nulls", nulls), ("null_empty", [])]:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            metadata["inputs"].append({"name": name, "schema": schema.jsonValue(),
                                       "rows": [encode(row, schema, input_value=True) for row in rows]})
        original = json.loads(Path(__file__).with_name("original-inputs.json").read_text())
        assert metadata == original, "provider declarations/rows differ from original independent capture"
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in cases():
                records.append({**case, "ansi": ansi, **capture(spark, case["sql"])})
        assert len(records) == 11
        assert len({(x["id"], x["input"], x["ansi"]) for x in records[1:]}) == 10
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": 10, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
