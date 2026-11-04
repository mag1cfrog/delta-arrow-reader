"""Capture ARRAY_COMPACT NULL-inspection boundaries from pinned Spark, never from DAR.

The manifest supplies inputs and SQL only. --self-check uses the standard
library and does not import Spark. Provider and parser failures abort capture.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
TIMEZONE = "UTC"
GROUP = "array-compact-890-subquery-preparation-v1"
ANSI_MODES = [True, False]


def decode_input(value, data_type, nullable=False):
    if value is None:
        assert nullable, data_type
        return None
    if isinstance(data_type, dict) and data_type["type"] == "struct":
        fields = data_type["fields"]
        assert isinstance(value, list) and len(value) == len(fields)
        return tuple(decode_input(item, field["type"], field["nullable"])
                     for item, field in zip(value, fields))
    if isinstance(data_type, dict) and data_type["type"] == "array":
        assert isinstance(value, list)
        return [decode_input(item, data_type["elementType"], data_type["containsNull"])
                for item in value]
    if data_type == "integer":
        assert isinstance(value, str) and str(int(value)) == value
        assert -(2 ** 31) <= int(value) < 2 ** 31
        return int(value)
    if data_type == "string":
        assert isinstance(value, str)
        return value
    raise ValueError(f"Unsupported capture input type: {data_type!r}")


def encode_result(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [encode_result(item) for item in value]
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return value
    raise ValueError(f"Unexpected query result type: {type(value).__name__}")


def self_check(manifest):
    assert manifest["spark_version"] == VERSION
    assert manifest["spark_revision"] == REVISION
    assert manifest["timezone"] == TIMEZONE
    assert manifest["group"] == GROUP
    assert manifest["ansi_modes"] == ANSI_MODES
    scenarios = manifest["scenarios"]
    assert len(scenarios) == manifest["scenario_count"] == 1
    assert len(scenarios) * len(ANSI_MODES) == manifest["observation_count"] == 2
    assert len({case["id"] for case in scenarios}) == len(scenarios)
    assert len({case["sql"] for case in scenarios}) == len(scenarios)
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                     for ansi in ANSI_MODES for case in scenarios]
    assert all("expected" not in case for case in scenarios)
    assert len(manifest["inputs"]) == 1
    for table in manifest["inputs"]:
        for row in table["rows"]:
            assert encode_result(decode_input(row, table["schema"])) == row
    for case in scenarios:
        assert case["sql"].startswith("SELECT ")
        assert case["allowed_error_conditions"] == ["CAST_INVALID_INPUT"]
    assert "pyspark" not in sys.modules


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    package = Path(__file__).resolve().parent
    source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    manifest_path = package / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    assert manifest["capture_script_sha256"] == source_sha256
    self_check(manifest)
    if args.self_check:
        assert args.output is None, "--self-check does not write raw output"
        print(json.dumps({"self_check": "passed", "scenarios": len(manifest["scenarios"]),
                          "observations": manifest["observation_count"],
                          "capture_sha256": source_sha256,
                          "manifest_sha256": manifest_sha256}))
        return
    assert args.output is not None, "an output path is required"
    assert args.output.parent.is_dir() and not args.output.exists(), args.output

    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.errors import PySparkException
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={REVISION}\n" in build, build
    assert f"version={VERSION}\n" in build, build
    spark = (SparkSession.builder.master("local[1]").appName(GROUP)
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", TIMEZONE).getOrCreate())
    try:
        assert spark.version == VERSION, spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == TIMEZONE
        spark.sparkContext.setLogLevel("ERROR")
        observed_inputs = []
        for table in manifest["inputs"]:
            rows = [decode_input(row, table["schema"]) for row in table["rows"]]
            frame = spark.createDataFrame(rows, types.StructType.fromJson(table["schema"]))
            frame.createOrReplaceTempView(table["name"])
            actual_schema = frame.schema.jsonValue()
            assert actual_schema == table["schema"], table["name"]
            actual_rows = [encode_result(row) for row in frame.orderBy("k").collect()]
            assert actual_rows == table["rows"], (table["name"], actual_rows)
            observed_inputs.append({"name": table["name"], "schema": actual_schema,
                                    "rows": actual_rows})
        # Parser and provider failures never become expected query errors.
        for case in manifest["scenarios"]:
            spark._jsparkSession.sessionState().sqlParser().parsePlan(case["sql"])
        records = [{"spark_version": spark.version, "spark_revision": REVISION,
                    "spark_build": build, "timezone": TIMEZONE, "group": GROUP,
                    "capture_sha256": source_sha256, "manifest_sha256": manifest_sha256,
                    "observations": manifest["observation_count"], "inputs": observed_inputs,
                    "input_encoding": manifest["input_encoding"],
                    "provider_input_roundtrip_checked": True}]
        for ansi in ANSI_MODES:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
            for case in manifest["scenarios"]:
                record = dict(case, ansi=ansi, timezone=TIMEZONE)
                stages, plans = {}, {}
                phase = "sql"
                try:
                    frame = spark.sql(case["sql"])
                    execution = frame._jdf.queryExecution()
                    phase = "public_schema"
                    public = frame.schema.jsonValue()
                    stages["public"] = public
                    for name, getter, phase in [
                        ("analyzed", execution.analyzed, "analyzed_schema"),
                        ("optimized", execution.optimizedPlan, "optimized_schema"),
                        ("executed_before_collect", execution.executedPlan, "executed_schema"),
                    ]:
                        plan = getter()
                        stages[name] = json.loads(plan.schema().json())
                        plans[name] = plan.toString()
                    phase = "collect"
                    rows = [encode_result(row) for row in frame.collect()]
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    phase = "executed_schema_after_collect"
                    plan = execution.executedPlan()
                    stages["executed"] = json.loads(plan.schema().json())
                    plans["executed"] = plan.toString()
                    assert public == stages["analyzed"]
                    record["expected"] = {"schema": public, "rows": rows}
                except Exception as error:
                    condition = error.getCondition() if isinstance(error, PySparkException) else None
                    assert condition in case["allowed_error_conditions"], (
                        case["id"], phase, type(error).__name__, str(error))
                    record.update(expected={"condition": condition}, spark_error=str(error),
                                  spark_error_phase=phase, spark_error_class=type(error).__name__,
                                  spark_error_details={"message_parameters": error.getMessageParameters()})
                record.update(schema_stages=stages, spark_plans=plans)
                records.append(record)
        assert len(records) - 1 == manifest["observation_count"]
        assert [{"id": record["id"], "ansi": record["ansi"]} for record in records[1:]] == (
            manifest["identities"])
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": manifest["observation_count"],
                          "capture_sha256": source_sha256,
                          "manifest_sha256": manifest_sha256,
                          "raw_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
