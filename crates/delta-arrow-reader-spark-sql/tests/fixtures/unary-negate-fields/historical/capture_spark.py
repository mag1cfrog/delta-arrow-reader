"""Capture five historical unary field declarations from pinned Spark.

The manifest contains SQL inputs only. --self-check never imports Spark.
All SQL is expected to execute successfully; any setup/query error aborts.
"""

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
TIMEZONE = "UTC"
GROUP = "unary-historical-fields-current-v1"
OBSERVATION_COUNT = 5


def encode_result(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [encode_result(item) for item in value]
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    assert isinstance(value, str), type(value)
    return value


def self_check(manifest):
    assert manifest["spark_version"] == VERSION
    assert manifest["spark_revision"] == REVISION
    assert manifest["timezone"] == TIMEZONE
    assert manifest["group"] == GROUP
    assert manifest["observation_count"] == OBSERVATION_COUNT
    scenarios = manifest["scenarios"]
    assert len(scenarios) == OBSERVATION_COUNT
    assert all(set(case) == {"id", "sql", "ansi", "ordered"} for case in scenarios)
    assert all(case["sql"].startswith("SELECT ") and case["sql"].isascii() for case in scenarios)
    assert all(type(case["ansi"]) is bool and type(case["ordered"]) is bool for case in scenarios)
    assert len({(case["id"], case["ansi"]) for case in scenarios}) == OBSERVATION_COUNT
    assert len({case["sql"] for case in scenarios}) == 3
    assert manifest["identities"] == [{"id": case["id"], "ansi": case["ansi"]} for case in scenarios]
    reference = Path(manifest["reused_encoder"])
    assert not reference.is_absolute() and ".." not in reference.parts
    reference = Path(__file__).resolve().parent / reference
    assert hashlib.sha256(reference.read_bytes()).hexdigest() == manifest["reused_encoder_sha256"]
    trees = [ast.parse(path.read_text()) for path in [Path(__file__), reference]]
    encoders = [next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "encode_result") for tree in trees]
    assert ast.dump(encoders[0]) == ast.dump(encoders[1])
    assert encode_result((None, True, -9223372036854775808)) == [None, "true", "-9223372036854775808"]


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
        assert args.output is None, "--self-check does not write capture output"
        assert "pyspark" not in sys.modules
        print(json.dumps({"self_check": "passed", "observations": OBSERVATION_COUNT,
                          "capture_sha256": source_sha256, "manifest_sha256": manifest_sha256}))
        return
    assert args.output is not None and args.output.parent.is_dir()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.sql import SparkSession

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
        for definition in manifest["scenarios"]:
            spark._jsparkSession.sessionState().sqlParser().parsePlan(definition["sql"])
        records = [{"spark_version": spark.version, "spark_revision": REVISION,
                    "spark_build": build, "timezone": TIMEZONE, "group": GROUP,
                    "capture_sha256": source_sha256, "manifest_sha256": manifest_sha256,
                    "observations": OBSERVATION_COUNT, "input_encoding": manifest["input_encoding"]}]
        for definition in manifest["scenarios"]:
            spark.conf.set("spark.sql.ansi.enabled", str(definition["ansi"]).lower())
            assert spark.conf.get("spark.sql.ansi.enabled") == str(definition["ansi"]).lower()
            frame = spark.sql(definition["sql"])
            execution = frame._jdf.queryExecution()
            public = frame.schema.jsonValue()
            stages, plans = {"public": public}, {}
            for name, getter in [("analyzed", execution.analyzed),
                                 ("optimized", execution.optimizedPlan),
                                 ("executed_before_collect", execution.executedPlan)]:
                plan = getter()
                stages[name] = json.loads(plan.schema().json())
                plans[name] = plan.toString()
            rows = [encode_result(row) for row in frame.collect()]
            if not definition["ordered"]:
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
            executed = execution.executedPlan()
            stages["executed"] = json.loads(executed.schema().json())
            plans["executed"] = executed.toString()
            assert public == stages["analyzed"]
            records.append(dict(definition, timezone=TIMEZONE, schema_stages=stages,
                                spark_plans=plans, expected={"schema": public, "rows": rows}))
        assert [{"id": r["id"], "ansi": r["ansi"]} for r in records[1:]] == manifest["identities"]
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": OBSERVATION_COUNT, "capture_sha256": source_sha256,
                          "manifest_sha256": manifest_sha256,
                          "raw_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
