"""Capture recursive STRUCT admission and NULL-valued MAP boundaries on pinned Spark 4.2.0."""

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
        ("array_case_names", "SELECT ARRAY(STRUCT(1 AS X),STRUCT(2 AS x)) AS r"),
        ("array_list_case_names", "SELECT ARRAY(ARRAY(STRUCT(1 AS X)),ARRAY(STRUCT(2 AS x))) AS r"),
        ("map_null_first_case_names", "SELECT MAP('none',NULL,'a',STRUCT(1 AS X),'b',STRUCT(2 AS x)) AS r"),
        ("array_duplicate_case_positions", "SELECT ARRAY(STRUCT(11 AS item,12 AS ITEM),STRUCT(21 AS ITEM,22 AS item)) AS r"),
    ]


def expressions():
    return []


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-array-struct-constructor-case-827")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        spark.sparkContext.setLogLevel("ERROR")
        tables = []
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
        assert len(records) - 1 == count == 8
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
