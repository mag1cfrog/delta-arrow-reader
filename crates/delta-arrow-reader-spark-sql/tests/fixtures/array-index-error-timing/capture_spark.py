"""Capture #971 ARRAY bounds preparation and reachability from pinned Spark.

The manifest contains inputs and SQL only. --self-check never imports Spark.
"""

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
spec = importlib.util.spec_from_file_location(
    "array_bounds_capture_helpers", PACKAGE.parent / "slice-coordinates/capture_spark.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def self_check():
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    assert manifest["spark_version"] == helper.VERSION == "4.2.0"
    assert manifest["spark_revision"] == helper.REVISION
    assert manifest["timezone"] == helper.TIMEZONE == "UTC"
    assert manifest["ansi_modes"] == [True, False]
    assert manifest["native_batch_sizes"] == [1, 32]
    assert manifest["case_count"] == len(manifest["cases"]) == 54
    assert manifest["observations"] == 2 * manifest["case_count"] == 108
    assert len({case["id"] for case in manifest["cases"]}) == 54
    assert len({case["sql"] for case in manifest["cases"]}) == 54
    assert {case["policy"] for case in manifest["cases"]} == {
        "literal", "alias", "provider", "conditional", "provider_condition",
        "bounded_cycles", "empty_and_pruning", "first_error"}
    assert {(case["depth"], case["id"].endswith("_selected"))
            for case in manifest["cases"] if case["policy"] == "bounded_cycles"} == {
                (depth, selected) for depth in [3, 4, 8] for selected in [True, False]}
    for case in manifest["cases"]:
        assert case["sql"].startswith("SELECT ")
        assert isinstance(case["ordered"], bool)
        assert not {"expected", "schema_stages", "spark_error"}.intersection(case)
    for relative, digest in manifest["helper_sources"].items():
        path = Path(relative)
        assert not path.is_absolute() and ".." not in path.parts
        assert sha256(PACKAGE.parent / path) == digest, relative
    for provider in manifest["inputs"]:
        assert provider["schema"]["type"] == "struct"
        for row in provider["rows"]:
            assert helper.encode_result(helper.decode_input(row, provider["schema"])) == row
    assert [provider["name"] for provider in manifest["inputs"]] == [
        "bounds_rows", "bounds_empty"]
    assert manifest["inputs"][1]["rows"] == []
    provenance = json.loads((PACKAGE / "original-provenance.json").read_text())
    assert provenance["source_sha256"] == (
        "bca7979a35a8eca98ae3f5340c87a8c364c184bc7698388357f8958ebe922c2d")
    assert sha256(PACKAGE / "original-spark.jsonl") == provenance["selected_sha256"]
    lines = (PACKAGE / "original-spark.jsonl").read_bytes().splitlines(keepends=True)
    assert len(lines) == len(provenance["source_lines"]) == 17
    for line, original in zip(lines, provenance["source_lines"]):
        assert hashlib.sha256(line).hexdigest() == original["sha256"]
    assert {(json.loads(line)["id"], json.loads(line)["ansi"]) for line in lines[1:]} == {
        (case_id, ansi) for case_id in provenance["selected_ids"] for ansi in [True, False]}
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
        print(json.dumps({"self_check": "passed", "cases": 54, "observations": 108,
                          "capture_sha256": sha256(Path(__file__)),
                          "manifest_sha256": sha256(PACKAGE / "manifest.json")}))
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
    assert f"revision={manifest['spark_revision']}\n" in build, build
    assert f"version={manifest['spark_version']}\n" in build, build
    spark = (SparkSession.builder.master("local[1]").appName(manifest["group"])
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"]
        spark.sparkContext.setLogLevel("ERROR")
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            frame = spark.createDataFrame(
                [helper.decode_input(row, provider["schema"]) for row in provider["rows"]], schema)
            assert frame.schema.jsonValue() == provider["schema"]
            assert [helper.encode_result(row) for row in frame.orderBy("id").collect()] == provider["rows"]
            frame.createOrReplaceTempView(provider["name"])
        header = {key: value for key, value in manifest.items() if key != "cases"}
        header.update(spark_build=build, capture_sha256=sha256(Path(__file__)),
                      manifest_sha256=sha256(PACKAGE / "manifest.json"),
                      process_timezone=os.environ["TZ"], provider_input_roundtrip_checked=True,
                      settings={key: spark.conf.get(key) for key in [
                          "spark.sql.shuffle.partitions", "spark.sql.session.timeZone",
                          "spark.sql.adaptive.enabled"]})
        count = 0
        with args.output.open("x") as output:
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            for ansi in manifest["ansi_modes"]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
                for case in manifest["cases"]:
                    record = dict(case, ansi=ansi, timezone="UTC")
                    stages, plans, phase = {}, {}, "sql"
                    try:
                        frame = spark.sql(case["sql"])
                        execution = frame._jdf.queryExecution()
                        phase = "public_schema"
                        record["public_schema"] = frame.schema.jsonValue()
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
                        record["rows_raw"] = rows
                        phase = "executed_schema_after_collect"
                        plan = execution.executedPlan()
                        stages["executed"] = json.loads(plan.schema().json())
                        plans["executed"] = plan.toString()
                        record["expected"] = {"schema": record["public_schema"], "rows": rows if case["ordered"]
                                              else sorted(rows, key=lambda row: json.dumps(row, separators=(",", ":")))}
                    except Exception as error:
                        if not isinstance(error, PySparkException) and getattr(error, "java_exception", None) is None:
                            raise
                        details = helper.error_details(error, PySparkException)
                        assert details["condition"] is not None, (case, phase, str(error))
                        record.update(expected={"condition": details["condition"]}, spark_error=str(error),
                                      spark_error_phase=phase, spark_error_class=type(error).__name__,
                                      spark_error_details=details, spark_error_traceback=traceback.format_exc())
                        if phase == "collect":
                            plan = execution.executedPlan()
                            stages["executed"] = json.loads(plan.schema().json())
                            plans["executed"] = plan.toString()
                    record.update(schema_stages=stages, spark_plans=plans)
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
                    count += 1
        assert count == manifest["observations"] == 108
        print(json.dumps({"observations": count, "output_sha256": sha256(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
