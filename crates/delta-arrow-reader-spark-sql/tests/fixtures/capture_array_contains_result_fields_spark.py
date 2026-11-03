"""Reproduce the isolated Spark 4.2 ARRAY_CONTAINS declaration reference (#837)."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"


def array(element, contains_null=False):
    return {"type": "array", "elementType": element, "containsNull": contains_null}


def field(name, data_type, nullable=False, metadata=None):
    return {"name": name, "type": data_type, "nullable": nullable,
            "metadata": metadata or {}}


SCHEMA = {"type": "struct", "fields": [
    field("required", array("integer"), metadata={"source": "required"}),
    field("nullable", array("integer"), True),
    field("elements", array("integer", True)),
    field("nested_required", array(array("integer"))),
    field("nested_nullable", array(array("integer")), True),
    field("nested_elements", array(array("integer", True))),
    field("nested_null_inner", array(array("integer"), True)),
    field("needle", "integer"), field("needle_nullable", "integer", True),
    field("nested_structs", array(array({"type": "struct", "fields": [
        field("x", "integer", True, {"origin": "flatten-child"})]}))),
]}
ROWS = [
    [[1], None, [1, None], [[1], []], None, [[1, None]], [[1], None], 1, 1, [[[1]]]],
    [[], [], [], [], [], [], [], 1, None, []],
    [[2, 3], [1], [None], [[2]], [[1]], [[None]], [[]], 2, 2, [[[None]]]],
    [[-2147483648, 2147483647], [2], [2], [[-2147483648, 2147483647]], [[2]],
     [[2]], [[2]], 2147483647, None, [[[2147483647]]]],
]


def queries():
    expressions = [(name, f"ARRAY_CONTAINS({values}, {needle})") for name, values, needle in [
        ("hit", "ARRAY(1)", "1"), ("miss", "ARRAY(1)", "2"),
        ("null_hit", "ARRAY(1, NULL)", "1"), ("null_miss", "ARRAY(1, NULL)", "2"),
        ("empty", "CAST(ARRAY() AS ARRAY<INT>)", "1"),
        ("empty_null_lookup", "CAST(ARRAY() AS ARRAY<INT>)", "CAST(NULL AS INT)"),
        ("null_outer", "CAST(NULL AS ARRAY<INT>)", "1"),
        ("null_lookup", "ARRAY(1)", "CAST(NULL AS INT)"),
        ("boundary", "ARRAY(-2147483648, 2147483647)", "2147483647"),
        ("signed_zero", "ARRAY(CAST(-0.0 AS DOUBLE))", "CAST(0.0 AS DOUBLE)"),
        ("nan", "ARRAY(CAST('NaN' AS DOUBLE))", "CAST('NaN' AS DOUBLE)"),
    ]]
    columns = [f"ARRAY_CONTAINS({values}, {needle}) AS {name}" for name, values, needle in [
        ("required", "required", "needle"), ("nullable", "nullable", "needle"),
        ("elements", "elements", "needle"), ("lookup", "required", "needle_nullable")]]
    invalid = [("invalid_scalar", "ARRAY_CONTAINS(1, 1)"),
               ("invalid_arity", "ARRAY_CONTAINS(ARRAY(1))")]
    live_error = "ARRAY_CONTAINS(ARRAY(CAST('bad' AS INT)), 1)"
    result = [{"id": name, "sql": f"SELECT {expression} AS r, STRUCT({expression} AS x) AS nested"}
              for name, expression in expressions]
    result += [{"id": "columns", "sql": "SELECT " + ", ".join(columns) + " FROM array_fields"},
               {"id": "empty_columns", "sql": "SELECT " + ", ".join(columns) + " FROM array_fields WHERE false"}]
    result += [{"id": name, "sql": f"SELECT {expression} AS r"} for name, expression in invalid]
    result += [{"id": "required_cast_error", "sql": f"SELECT {live_error} AS r"}]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output_path = args.output
    assert output_path.parent.is_dir()
    assert not output_path.exists()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.sql import SparkSession, types
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={REVISION}" in build, build
    spark = (SparkSession.builder.master("local[1]").appName("dar-array-contains-result-fields-837")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType.fromJson(SCHEMA)

        def decode(value, data_type):
            if value is None:
                return None
            if isinstance(data_type, types.StructType):
                return tuple(decode(item, field.dataType) for item, field in zip(value, data_type))
            if isinstance(data_type, types.ArrayType):
                return [decode(item, data_type.elementType) for item in value]
            return value

        def encode(value):
            if value is None:
                return None
            if isinstance(value, (list, tuple)):
                return [encode(item) for item in value]
            if isinstance(value, bool):
                return str(value).lower()
            return str(value)

        spark.createDataFrame([decode(row, schema) for row in ROWS], schema).createOrReplaceTempView("array_fields")
        group = "array-contains"
        definitions = queries()
        records = [{"spark_version": spark.version, "spark_revision": REVISION,
                    "spark_build": build, "timezone": "UTC", "group": group,
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "observations": len(definitions) * 2,
                    "inputs": [{"name": "array_fields", "schema": SCHEMA, "rows": [encode(row) for row in ROWS]}]}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for definition in definitions:
                record = dict(definition, ansi=ansi)
                stages = {}
                phase = "sql"
                try:
                    frame = spark.sql(record["sql"])
                    execution = frame._jdf.queryExecution()
                    public = frame.schema.jsonValue()
                    phase = "analyzed_schema"
                    stages["analyzed"] = json.loads(execution.analyzed().schema().json())
                    phase = "optimized_schema"
                    stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
                    phase = "executed_schema"
                    stages["executed_before_collect"] = json.loads(execution.executedPlan().schema().json())
                    phase = "collect"
                    rows = [encode(row) for row in frame.collect()]
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    stages["executed"] = json.loads(execution.executedPlan().schema().json())
                    assert public == stages["analyzed"]
                    record["expected"] = {"schema": public, "rows": rows}
                except Exception as error:
                    assert definition["id"].startswith("invalid_") or definition["id"] == "required_cast_error", (record, error)
                    condition = error.getCondition() if hasattr(error, "getCondition") else None
                    assert condition, (record, error)
                    record.update(expected={"condition": condition}, spark_error=str(error),
                                  spark_error_phase=phase, spark_error_class=type(error).__name__)
                record["schema_stages"] = stages
                records.append(record)
        assert len(records) - 1 == records[0]["observations"] == 32
        assert len({(record["id"], record["ansi"]) for record in records[1:]}) == records[0]["observations"]
        with output_path.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"group": group, "observations": len(records) - 1,
                          "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
