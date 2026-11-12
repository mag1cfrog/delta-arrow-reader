"""Capture MAP_VALUES over registered Map columns with Spark 4.2.0."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


def encode(value):
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        return [encode(v) for v in value]
    return str(value).lower() if isinstance(value, bool) else str(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.sql import SparkSession, types

    spark = (SparkSession.builder.master("local[1]").appName("dar-map-values-structs")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0"
        spark.sparkContext.setLogLevel("ERROR")
        struct = types.StructType([types.StructField("v", types.IntegerType())])
        definitions = [
            ("struct_required", struct, False,
             [(1, {"a": (1,), "b": (None,)}), (2, None), (3, {}), (4, {"a": (2,), "b": (2,)})]),
            ("struct_nullable", struct, True,
             [(1, {"a": (1,), "b": None}), (2, None), (3, {}), (4, {"a": (None,), "b": (2,)})]),
            ("primitive_required", types.IntegerType(), False,
             [(1, {"a": 1, "b": 2}), (2, None), (3, {}), (4, {"a": 2, "b": 2})]),
            ("primitive_nullable", types.IntegerType(), True,
             [(1, {"a": 1, "b": None}), (2, None), (3, {}), (4, {"a": 2})]),
            ("struct_empty", struct, False, []),
        ]
        inputs = []
        queries = []
        for name, value_type, nullable, rows in definitions:
            schema = types.StructType([
                types.StructField("id", types.IntegerType(), False),
                types.StructField("m", types.MapType(types.StringType(), value_type, nullable)),
            ])
            frame = spark.createDataFrame(rows, schema).cache()
            frame.createOrReplaceTempView(name)
            # Record each map's physical entry order, without a JVM Map-to-dict conversion.
            entries = frame.selectExpr("id", "map_entries(m) AS entries").collect()
            inputs.append({"name": name, "schema": json.loads(schema.json()),
                           "rows": [[encode(row.id), encode(row.entries)] for row in entries]})
            for suffix, expression in [
                ("direct", "map_values(m)"),
                ("scalar", "(SELECT map_values(m))"),
                ("coalesce", "coalesce(map_values(m), array())"),
                ("size", "size(map_values(m))"),
            ]:
                queries.append((f"{name}_{suffix}", f"SELECT id, {expression} AS r FROM {name}", False))
            queries.append((f"{name}_hidden_sort", f"SELECT map_values(m) AS r FROM {name} ORDER BY id DESC", True))
            queries.append((f"{name}_first", f"SELECT id, element_at(map_values(m), 1) AS r FROM {name} WHERE size(m) > 0", False))
        schema = types.StructType([types.StructField("id", types.IntegerType(), False),
                                   types.StructField("v", types.IntegerType())])
        rows = [(1, 1), (2, None), (3, 2), (4, 2)]
        frame = spark.createDataFrame(rows, schema).cache()
        frame.createOrReplaceTempView("values_input")
        inputs.append({"name": "values_input", "schema": json.loads(schema.json()),
                       "rows": [encode(row) for row in frame.collect()]})
        value = "map_values(map('k', struct(q.v AS renamed)))"
        for name, sql, ordered in [
            ("constructor_direct", f"SELECT id, {value} AS r FROM values_input q", False),
            ("constructor_scalar", f"SELECT id, (SELECT {value}) AS r FROM values_input q", False),
            ("constructor_hidden_sort", f"SELECT (SELECT {value}) AS r FROM values_input q ORDER BY id DESC", True),
            ("constructor_coalesce", f"SELECT id, coalesce({value}, array()) AS r FROM values_input q", False),
            ("constructor_size", f"SELECT id, size({value}) AS r FROM values_input q", False),
            ("typed_null_map", "SELECT map_values(CAST(NULL AS MAP<STRING, STRUCT<v: INT>>)) AS r", False),
        ]:
            queries.append((name, sql, ordered))
        jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
        with ZipFile(jar) as archive:
            build = archive.read("spark-version-info.properties").decode()
        records = [{"spark_version": spark.version, "spark_build": build, "inputs": inputs,
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}]
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql, ordered in queries:
                frame = spark.sql(sql)
                rows = [encode(row) for row in frame.collect()]
                if not ordered:
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                records.append({"id": name, "sql": sql, "ansi": ansi, "ordered": ordered,
                                "rows": rows, "schema": json.loads(frame._jdf.queryExecution().executedPlan().schema().json())})
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
