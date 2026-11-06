"""Capture complete collection-size fields independently of lambda optimization."""

import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import encode
from capture_struct_field_resolution_spark import capture


def input_value(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.MapType):
        return [[key, encode(item, data_type.valueType, input_value=True)]
                for key, item in sorted(value.items())]
    if isinstance(data_type, types.StructType):
        return [input_value(item, field.dataType) for item, field in zip(value, data_type)]
    return encode(value, data_type, input_value=True)


def constants():
    for function in ["SIZE", "CARDINALITY", "ARRAY_SIZE"]:
        prefix = function.lower()
        arrays = [
            ("null_array", "CAST(NULL AS ARRAY<INT>)"),
            ("empty_array", "CAST(ARRAY() AS ARRAY<INT>)"),
            ("null_elements", "ARRAY(NULL,NULL)"),
            ("nested_nulls", "ARRAY(ARRAY(NULL),CAST(NULL AS ARRAY<INT>))"),
            ("untyped_null", "NULL"),
        ]
        maps = [
            ("null_map", "CAST(NULL AS MAP<STRING,INT>)"),
            ("empty_map", "CAST(MAP() AS MAP<STRING,INT>)"),
            ("null_values", "MAP('a',CAST(NULL AS INT),'b',2)"),
            ("nested_nulls", "MAP('a',CAST(NULL AS ARRAY<INT>),'b',ARRAY(1,NULL))"),
        ]
        for label, arguments in [("array_literals", arrays), ("map_literals", maps)]:
            yield f"{prefix}_{label}", "SELECT " + ",".join(
                f"{function}({argument}) AS {name}" for name, argument in arguments)
        yield f"{prefix}_invalid_scalar", f"SELECT {function}(1) AS result"
        yield f"{prefix}_invalid_typed_null", f"SELECT {function}(CAST(NULL AS INT)) AS result"
        yield f"{prefix}_live_cast", f"SELECT {function}(ARRAY(CAST('bad' AS INT))) AS result"


def providers(table):
    for function in ["SIZE", "CARDINALITY", "ARRAY_SIZE"]:
        columns = ["required_array", "nullable_array"]
        if function != "ARRAY_SIZE":
            columns += ["required_map", "nullable_map"]
        yield function.lower() + "_columns", "SELECT id," + ",".join(
            f"{function}({column}) AS {column}_size" for column in columns
        ) + f" FROM {table}"
    yield "array_size_invalid_map", f"SELECT ARRAY_SIZE(required_map) AS result FROM {table}"
    yield "null_and_fallback_consumer", (
        "SELECT id,CASE WHEN SIZE(required_array) IS NULL THEN 1 ELSE 0 END AS never_null,"
        "CASE WHEN SIZE(nullable_array) IS NULL THEN 1 ELSE 0 END AS maybe_null,"
        f"COALESCE(CARDINALITY(nullable_map),99) AS fallback FROM {table}"
    )
    yield "filter_consumer", (
        f"SELECT id FROM {table} WHERE SIZE(required_array) IS NOT NULL "
        "AND (SIZE(nullable_array) IS NULL OR CARDINALITY(required_map)>0)"
    )
    yield "aggregate_consumer", (
        "SELECT COUNT(SIZE(required_array)) AS n_required,"
        "COUNT(SIZE(nullable_array)) AS n_nullable,"
        f"SUM(CARDINALITY(required_map)) AS total FROM {table}"
    )


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=32f7299601108917fb01920a54e084595b7b3bf8" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-size-result-nullability-822")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        array = types.ArrayType(types.IntegerType(), True)
        map_type = types.MapType(types.StringType(), types.IntegerType(), True)
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("required_array", array, False, {"source": "required_array"}),
            types.StructField("nullable_array", array, True, {"source": "nullable_array"}),
            types.StructField("required_map", map_type, False, {"source": "required_map"}),
            types.StructField("nullable_map", map_type, True, {"source": "nullable_map"}),
        ])
        rows = [
            (0, [1, None], [1, None], {"a": 1, "b": None}, {"a": 1, "b": None}),
            (1, [], None, {}, None),
            (2, [None], [], {"a": None}, {}),
            (3, [2, 3], [None], {"z": 9}, {"z": None}),
        ]
        tables = [("size_fields_mixed", rows), ("size_fields_nulls", [rows[1]]),
                  ("size_fields_empty", [])]
        records = [{"spark_version": spark.version, "spark_build": build,
                    "timezone": "UTC", "case_sensitive": False,
                    "default_legacy_size_of_null": spark.conf.get("spark.sql.legacy.sizeOfNull"),
                    "inputs": []}]
        for table, values in tables:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [input_value(row, schema) for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for legacy in [True, False]:
                spark.conf.set("spark.sql.legacy.sizeOfNull", str(legacy).lower())
                for name, sql in constants():
                    records.append({"id": name, "input": "constant", "ansi": ansi,
                                    "legacy_size_of_null": legacy, "sql": sql,
                                    **capture(spark, sql)})
                for table, _ in tables:
                    for name, sql in providers(table):
                        records.append({"id": name, "input": table, "ansi": ansi,
                                        "legacy_size_of_null": legacy, "sql": sql,
                                        **capture(spark, sql)})
        count = 4 * (len(list(constants())) + len(tables) * len(list(providers(tables[0][0]))))
        assert count == len(records) - 1 == 144
        assert len({(case["id"], case["input"], case["ansi"], case["legacy_size_of_null"])
                    for case in records[1:]}) == count
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"observations": count, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
