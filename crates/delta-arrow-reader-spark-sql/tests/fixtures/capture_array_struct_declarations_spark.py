"""Capture ARRAY/MAP STRUCT declarations on the pinned Spark 4.2.0 build."""

import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"


def constant_cases():
    return [
        ("array_nullable_child", "SELECT ARRAY(STRUCT(CAST(NULL AS INT) AS x),STRUCT(1 AS x)) AS r"),
        ("array_required_control", "SELECT ARRAY(STRUCT(1 AS x),STRUCT(2 AS x)) AS r"),
        ("array_nullable_parent", "SELECT ARRAY(CAST(NULL AS STRUCT<x:INT>),STRUCT(1 AS x)) AS r"),
        ("map_nullable_parent", "SELECT MAP('a',CAST(NULL AS STRUCT<x:INT>),'b',STRUCT(1 AS x)) AS r"),
        ("array_required_cast_error", "SELECT ARRAY(STRUCT(CAST('invalid' AS INT) AS x),STRUCT(1 AS x)) AS r"),
        ("map_required_cast_error", "SELECT MAP('a',STRUCT(CAST('invalid' AS INT) AS x),'b',STRUCT(1 AS x)) AS r"),
    ]


def expressions():
    return [
        ("array_runtime_declarations", "ARRAY(STRUCT(q.v AS x),STRUCT(q.id AS x))"),
        ("map_runtime_declarations", "MAP('a',STRUCT(q.v AS x),'b',STRUCT(q.id AS x))"),
        ("array_parent_metadata", "ARRAY(q.s,q.t)"),
        ("map_parent_metadata", "MAP('a',q.s,'b',q.t)"),
        ("array_nested_declarations", "ARRAY(STRUCT(q.s AS child),STRUCT(q.t AS child))"),
        ("map_nested_declarations", "MAP('a',STRUCT(q.s AS child),'b',STRUCT(q.t AS child))"),
        ("array_numeric_widening", "ARRAY(STRUCT(q.v AS x),STRUCT(CAST(q.id AS BIGINT) AS x))"),
    ]


def cases(table):
    for name, expression in expressions():
        yield name, f"SELECT {expression} AS r FROM {table} q"


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    import pyspark
    from pyspark.sql import SparkSession, types

    sys.path.insert(0, str(ROOT / "correlated-scalar-metadata-stages" / "map-interactions" / "sources"))
    from capture_correlated_scalar_metadata_stages import capture_case
    from capture_floating_group_keys_spark import encode as encode_leaf

    def encode(value, data_type, *, input_value=False):
        if value is None:
            return None
        if isinstance(data_type, types.MapType):
            return [[encode(key, data_type.keyType), encode(item, data_type.valueType)]
                    for key, item in value.items()]
        if isinstance(data_type, types.StructType):
            assert len(value) == len(data_type)
            return [encode(item, field.dataType, input_value=input_value)
                    for item, field in zip(value, data_type)]
        if isinstance(data_type, types.ArrayType):
            return [encode(item, data_type.elementType, input_value=input_value) for item in value]
        return encode_leaf(value, data_type, input_value=input_value)

    map_type = types.MapType(types.StringType(), types.ArrayType(types.IntegerType()), True)
    assert encode(None, map_type) is None
    assert encode({}, map_type) == []
    assert encode({"k": None}, map_type) == [["k", None]]
    assert encode({"k": [1, None]}, map_type) == [["k", ["1", None]]]
    duplicate_struct = types.StructType([types.StructField("item", types.IntegerType()),
                                        types.StructField("item", types.IntegerType())])
    assert encode((1, 2), duplicate_struct) == ["1", "2"]

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={REVISION}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-array-struct-declarations-827")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        spark.sparkContext.setLogLevel("ERROR")
        nullable_fields = types.StructType([
            types.StructField("x", types.IntegerType(), True, {"source": "nullable"}),
        ])
        required_fields = types.StructType([
            types.StructField("x", types.IntegerType(), False, {"source": "required"}),
        ])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False, {"source": "id"}),
            types.StructField("v", types.IntegerType(), True, {"source": "value"}),
            types.StructField("s", nullable_fields, True, {"source": "nullable_parent"}),
            types.StructField("t", required_fields, False, {"source": "required_parent"}),
        ])
        rows = [(0, 1, (11,), (21,)), (1, None, (None,), (22,)),
                (2, None, None, (23,))]
        tables = [("declarations_mixed", rows), ("declarations_nulls", [rows[2]]),
                  ("declarations_empty", [])]
        records = [{"spark_version": spark.version, "spark_revision": REVISION,
                    "spark_build": build, "timezone": "UTC", "map_encoding": "entry_pairs",
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "source_files": [{"path": str(path.relative_to(ROOT)),
                                      "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                                     for path in [
                                         ROOT / "correlated-scalar-metadata-stages/map-interactions/sources/capture_correlated_scalar_metadata_stages.py",
                                         ROOT / "correlated-scalar-metadata-stages/map-interactions/sources/capture_floating_group_keys_spark.py",
                                     ]],
                    "inputs": []}]
        for table, values in tables:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                        "rows": [encode(row, schema, input_value=True) for row in values]})
        calls = spark.sparkContext.accumulator(0)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in constant_cases():
                case = {"id": name, "input": "single_row", "ansi": ansi, "sql": sql}
                records.append(capture_case(spark, case, calls, encode))
            for table, _ in tables:
                for name, sql in cases(table):
                    case = {"id": name, "input": table, "ansi": ansi, "sql": sql}
                    records.append(capture_case(spark, case, calls, encode))
        count = 2 * (len(constant_cases()) + len(tables) * len(expressions()))
        assert len(records) - 1 == count == 54
        assert len({(case["id"], case["input"], case["ansi"]) for case in records[1:]}) == count
        assert all(("expected" in case) != ("error" in case) for case in records[1:])
        assert all(case["error"]["condition"] for case in records[1:] if "error" in case)
        with output.open("x") as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": count, "output": str(output),
                          "errors": sum("error" in case for case in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
