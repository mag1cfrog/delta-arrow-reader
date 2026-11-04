"""Capture the independent Spark case-conversion interaction hypothesis."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import traceback
from zipfile import ZipFile

sys.dont_write_bytecode = True
PACKAGE = Path(__file__).resolve().parent
MANIFEST_PATH = PACKAGE / "case-conversion-slice-manifest.json"
HELPER_PACKAGE = PACKAGE / "slice-coordinates"
spec = importlib.util.spec_from_file_location(
    "coordinate_reference", HELPER_PACKAGE / "capture_spark.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def self_check():
    manifest = json.loads(MANIFEST_PATH.read_text())
    assert manifest["capture_script_sha256"] == sha256(Path(__file__))
    assert manifest["source_package"] == "slice-coordinates"
    for relative, expected_hash in manifest["references"].items():
        assert sha256(HELPER_PACKAGE / relative) == expected_hash, relative
    assert manifest["group"] == "slice-894-case-conversion-interaction-v1"
    assert manifest["spark_version"] == helper.VERSION
    assert manifest["spark_revision"] == helper.REVISION
    assert manifest["timezone"] == "UTC"
    assert manifest["ansi_modes"] == [True, False]
    assert len(manifest["scenarios"]) == manifest["scenario_count"] == 3
    assert manifest["observation_count"] == 6
    assert [case["id"] for case in manifest["scenarios"]] == [
        "slice_provider_context", "array_provider_context", "slice_scalar_context"]
    assert all(set(case) == {"id", "sql", "ordered"} and case["ordered"] is True
               for case in manifest["scenarios"])
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                      for ansi in [True, False] for case in manifest["scenarios"]]
    source_manifest = json.loads((HELPER_PACKAGE / "manifest.json").read_text())
    helper.self_check(source_manifest)
    assert manifest["input_encoding"] == source_manifest["input_encoding"]
    assert manifest["inputs"] == [item for item in source_manifest["inputs"]
                                  if item["name"] == "slice_fields"]
    assert len(manifest["inputs"]) == 1 and len(manifest["inputs"][0]["rows"]) == 4
    for item in manifest["inputs"]:
        for row in item["rows"]:
            assert helper.encode_result(helper.decode_input(row, item["schema"])) == row
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest = self_check()
    if args.self_check:
        assert args.output is None
        assert "pyspark" not in sys.modules
        print(json.dumps({"self_check": "passed", "scenarios": 3, "observations": 6,
                          "capture_sha256": sha256(Path(__file__)),
                          "manifest_sha256": sha256(MANIFEST_PATH)}))
        return
    assert args.output is not None and args.output.parent.is_dir()
    assert not args.output.exists(), args.output
    assert os.environ.get("TZ") == "UTC"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.errors import PySparkException
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={helper.REVISION}\n" in build
    assert f"version={helper.VERSION}\n" in build
    spark = (SparkSession.builder.master("local[1]").appName(manifest["group"])
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == helper.VERSION
        spark.sparkContext.setLogLevel("ERROR")
        for item in manifest["inputs"]:
            schema = types.StructType.fromJson(item["schema"])
            frame = spark.createDataFrame(
                [helper.decode_input(row, item["schema"]) for row in item["rows"]], schema)
            frame.createOrReplaceTempView(item["name"])
            assert frame.schema.jsonValue() == item["schema"]
            assert [helper.encode_result(row) for row in frame.orderBy("k").collect()] == item["rows"]
        for item in manifest["scenarios"]:
            spark._jsparkSession.sessionState().sqlParser().parsePlan(item["sql"])
        records = [dict(manifest, manifest_sha256=sha256(MANIFEST_PATH),
                        capture_sha256=sha256(Path(__file__)), spark_build=build,
                        process_timezone=os.environ["TZ"], provider_input_roundtrip_checked=True)]
        for ansi in manifest["ansi_modes"]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
            for item in manifest["scenarios"]:
                record = dict(item, ansi=ansi, timezone="UTC")
                stages, plans, phase = {}, {}, "sql"
                try:
                    frame = spark.sql(item["sql"])
                    execution = frame._jdf.queryExecution()
                    phase = "public_schema"
                    stages["public"] = frame.schema.jsonValue()
                    stages["resolved"] = stages["public"]
                    for name, getter, phase in [
                        ("analyzed", execution.analyzed, "analyzed_schema"),
                        ("optimized", execution.optimizedPlan, "optimized_schema"),
                        ("physical", execution.sparkPlan, "physical_schema"),
                        ("executed_before_collect", execution.executedPlan, "executed_schema"),
                    ]:
                        plan = getter()
                        stages[name] = json.loads(plan.schema().json())
                        plans[name] = plan.toString()
                    phase = "collect"
                    rows = [helper.encode_result(row) for row in frame.collect()]
                    record["spark_collected_rows"] = rows
                    phase = "executed_schema_after_collect"
                    plan = execution.executedPlan()
                    stages["executed"] = json.loads(plan.schema().json())
                    plans["executed"] = plan.toString()
                    record["expected"] = {"schema": stages["public"], "rows": rows}
                except Exception as error:
                    if not isinstance(error, PySparkException) and getattr(error, "java_exception", None) is None:
                        raise
                    details = helper.error_details(error, PySparkException)
                    record.update(expected={"condition": details["condition"]}, spark_error=str(error),
                                  spark_error_phase=phase, spark_error_class=type(error).__name__,
                                  spark_error_details=details, spark_error_traceback=traceback.format_exc())
                record.update(schema_stages=stages, spark_plans=plans)
                records.append(record)
        assert len(records) == 7
        assert [{"id": r["id"], "ansi": r["ansi"]} for r in records[1:]] == manifest["identities"]
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": 6, "raw_sha256": sha256(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
