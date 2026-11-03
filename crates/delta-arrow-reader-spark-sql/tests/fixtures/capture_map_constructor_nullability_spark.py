"""Capture direct and scalar MAP declarations on the pinned Spark 4.2.0 build."""

import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"


def expressions():
    return [
        ("direct_nullable", "MAP('k',q.v)"),
        ("direct_required", "MAP('k',q.id)"),
        ("empty_map", "MAP()"),
        ("array_element", "ARRAY(MAP('k',q.v))"),
        ("struct_child", "NAMED_STRUCT('child',MAP('k',q.v))"),
        ("map_value", "MAP('outer',MAP('inner',q.v))"),
        ("lambda_result", "TRANSFORM(q.numbers,x -> MAP('k',x))"),
        ("struct_metadata", "MAP('k',STRUCT(q.v AS renamed))"),
        ("duplicate_struct_fields", "MAP('k',STRUCT(q.v AS item,q.id AS item))"),
        ("explicit_cast", "CAST(MAP('k',STRUCT(q.v AS renamed)) AS MAP<STRING,STRUCT<renamed:INT>>)"),
        ("scalar", "(SELECT MAP('k',q.v))"),
        ("scalar_struct_metadata", "(SELECT MAP('k',STRUCT(q.v AS renamed)))"),
        ("scalar_explicit_cast", "(SELECT CAST(MAP('k',STRUCT(q.v AS renamed)) AS MAP<STRING,STRUCT<renamed:INT>>))"),
        ("from_arrays_control", "MAP_FROM_ARRAYS(ARRAY('k'),ARRAY(q.v))"),
        ("from_entries_control", "MAP_FROM_ENTRIES(ARRAY(STRUCT('k',q.v)))"),
        ("duplicate_key_error", "MAP('k',q.v,'k',q.id)"),
    ]


def cases(table):
    for name, expression in expressions():
        yield name, f"SELECT {expression} AS r FROM {table} q"
    yield "scalar_hidden_order", (
        f"SELECT (SELECT MAP('k',STRUCT(q.v AS renamed))) AS r FROM {table} q ORDER BY q.id"
    )


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-map-constructor-nullability-824")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False, {"source": "id"}),
            types.StructField("v", types.IntegerType(), True, {"source": "plain"}),
            types.StructField("numbers", types.ArrayType(types.IntegerType(), True), True),
        ])
        rows = [(0, 2, [1, None]), (1, None, []), (2, -2, None),
                (3, None, [None]), (4, 2, [1, None])]
        tables = [("map_mixed", rows), ("map_nulls", [rows[1], rows[3]]), ("map_empty", [])]
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
            for table, _ in tables:
                for name, sql in cases(table):
                    case = {"id": name, "input": table, "ansi": ansi, "sql": sql}
                    records.append(capture_case(spark, case, calls, encode))
        count = 2 * len(tables) * len(list(cases(tables[0][0])))
        assert len(records) - 1 == count == 102
        assert len({(case["id"], case["input"], case["ansi"]) for case in records[1:]}) == count
        assert all(("expected" in case) != ("error" in case) for case in records[1:])
        with output.open("x") as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": count, "output": str(output),
                          "errors": sum("error" in case for case in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
