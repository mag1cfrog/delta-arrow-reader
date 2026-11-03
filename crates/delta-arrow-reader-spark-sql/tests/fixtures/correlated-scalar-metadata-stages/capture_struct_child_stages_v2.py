"""Append independent inner-alias schema-stage controls without rewriting earlier observations."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from zipfile import ZipFile

import pyspark
from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import encode

ROOT = Path(__file__).resolve().parent
REFERENCE = ROOT / "struct-expression-names-spark.jsonl"
SEED = ROOT / "original-scope-spark.jsonl"
SEED_SHA256 = "332f7962d37366265ff8b23c0b170c40e1b451a4fff04ef67933a8ad0c3b3d5c"


def decode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.StructType):
        return tuple(decode(item, field.dataType) for item, field in zip(value, data_type))
    if isinstance(data_type, types.ArrayType):
        return [decode(item, data_type.elementType) for item in value]
    if isinstance(data_type, types.IntegerType):
        return int(value)
    raise AssertionError((value, data_type))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    seed_bytes = SEED.read_bytes()
    assert hashlib.sha256(seed_bytes).hexdigest() == SEED_SHA256
    assert seed_bytes.endswith(b"\n")
    seed_records = [json.loads(line) for line in seed_bytes.splitlines()]
    assert len(seed_records) == 25
    source = json.loads(REFERENCE.read_text().splitlines()[0])
    assert source["inputs"] == seed_records[0]["inputs"]
    capture_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    assert {provider["name"] for provider in source["inputs"]} == {
        "struct_mixed", "struct_nulls", "struct_empty"}
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-review-818-correlated-inner-alias-stages-v2")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        spark.sparkContext.setLogLevel("ERROR")
        assert seed_records[0]["spark_version"] == spark.version
        assert seed_records[0]["timezone"] == "UTC"
        assert seed_records[0]["spark_build"] == build
        records = list(seed_records)
        for provider in source["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            values = [decode(row, schema) for row in provider["rows"]]
            assert [encode(row, schema, input_value=True) for row in values] == provider["rows"]
            spark.createDataFrame(values, schema).createOrReplaceTempView(provider["name"])
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for provider in source["inputs"]:
                table = provider["name"]
                for label, sql in [
                    ("correlated_struct_inner_alias", f"SELECT id, (SELECT STRUCT(q.v AS renamed) AS inner_s) AS r FROM {table} q"),
                    ("correlated_direct_inner_alias", f"SELECT id, (SELECT q.v AS inner_v) AS r FROM {table} q"),
                ]:
                    record = {"id": label, "input": table, "ansi": ansi, "sql": sql,
                              "capture_script_sha256": capture_sha256}
                    try:
                        frame = spark.sql(sql)
                        record["public_schema"] = frame.schema.jsonValue()
                        execution = frame._jdf.queryExecution()
                        record["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
                        record["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
                        record["executed_schema_before_collect"] = json.loads(execution.executedPlan().schema().json())
                        record["analyzed_plan"] = execution.analyzed().toString()
                        record["optimized_plan"] = execution.optimizedPlan().toString()
                        rows = [encode(row, frame.schema) for row in frame.collect()]
                        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                        record["executed_schema_after_collect"] = json.loads(execution.executedPlan().schema().json())
                        record["physical_plan"] = execution.executedPlan().toString()
                        record["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
                    except Exception as error:
                        condition = getattr(error, "getCondition", lambda: None)()
                        cause = getattr(error, "java_exception", None)
                        while condition is None and cause is not None:
                            try:
                                condition = cause.getCondition()
                            except Py4JError:
                                pass
                            cause = cause.getCause()
                        record["error"] = {"condition": condition, "message": str(error)}
                    records.append(record)
        assert len(records) == 37
        assert len({(r["id"], r["input"], r["ansi"]) for r in records[1:]}) == 36
        assert all(("expected" in r) != ("error" in r) for r in records[1:])
        assert records[:len(seed_records)] == seed_records
        with args.output.open("xb") as output:
            output.write(seed_bytes)
            for record in records[len(seed_records):]:
                output.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
        assert args.output.read_bytes().startswith(seed_bytes)
        print(json.dumps({"observations": len(records)-1, "appended_observations": 12,
                          "preserved_seed_sha256": SEED_SHA256,
                          "capture_script_sha256": capture_sha256,
                          "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
