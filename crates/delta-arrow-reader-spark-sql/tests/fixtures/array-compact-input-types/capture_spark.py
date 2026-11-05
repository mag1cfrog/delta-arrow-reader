"""Capture ARRAY_COMPACT input admission and successful controls from pinned Spark.

The manifest supplies SQL and ANSI modes only. --self-check does not import Spark.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

sys.dont_write_bytecode = True
PACKAGE = Path(__file__).resolve().parent
VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
GROUP = "array-compact-input-types-891-v1"
BASELINE = "02f1ca8f9572ac39c9e5ef1a6fd18e94185a6132"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def encode_result(value):
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (tuple, list)):
        return [encode_result(item) for item in value]
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    raise TypeError(type(value).__name__)


def self_check(manifest):
    assert manifest["spark_version"] == VERSION
    assert manifest["spark_revision"] == REVISION
    assert manifest["source_commit"] == BASELINE
    assert manifest["group"] == GROUP
    assert manifest["timezone"] == "UTC"
    assert manifest["ansi_modes"] == [True, False]
    assert manifest["capture_script_sha256"] == sha256(Path(__file__))
    assert manifest["inputs"] == []
    scenarios = manifest["scenarios"]
    assert [case["id"] for case in scenarios] == [
        "integer", "string", "struct", "map", "typed_null_integer", "array", "untyped_null"]
    assert len({case["sql"] for case in scenarios}) == len(scenarios) == 7
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                     for ansi in manifest["ansi_modes"] for case in scenarios]
    assert manifest["observation_count"] == 14
    assert all(set(case) == {"id", "sql"} for case in scenarios)
    assert encode_result((None, [1, True, "x"])) == [None, ["1", "true", "x"]]
    assert "pyspark" not in sys.modules


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest_path = PACKAGE / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    self_check(manifest)
    source_hash, manifest_hash = sha256(Path(__file__)), sha256(manifest_path)
    if args.self_check:
        assert args.output is None
        print(json.dumps({"self_check": "passed", "observations": 14,
                          "capture_sha256": source_hash, "manifest_sha256": manifest_hash}))
        return
    assert args.output is not None and args.output.parent.is_dir()
    assert os.environ.get("TZ") == "UTC", "run with TZ=UTC"
    with args.output.open("x", encoding="utf-8") as output:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        import pyspark
        from pyspark.errors import PySparkException
        from pyspark.sql import SparkSession

        jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
        with ZipFile(jar) as archive:
            build = archive.read("spark-version-info.properties").decode()
        assert f"revision={REVISION}\n" in build, build
        assert f"version={VERSION}\n" in build, build
        builder = SparkSession.builder.master("local[1]").appName(GROUP)
        for key, value in manifest["spark_config"].items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        try:
            assert spark.version == VERSION
            assert spark.sparkContext.master == "local[1]"
            spark.sparkContext.setLogLevel("ERROR")
            config = {key: spark.conf.get(key) for key in manifest["spark_config"]}
            assert config == manifest["spark_config"]
            for case in manifest["scenarios"]:
                spark._jsparkSession.sessionState().sqlParser().parsePlan(case["sql"])
            header = {"spark_version": spark.version, "spark_revision": REVISION,
                      "spark_build": build, "group": GROUP, "source_commit": BASELINE,
                      "timezone": "UTC", "process_timezone": os.environ["TZ"],
                      "spark_master": spark.sparkContext.master, "spark_config": config,
                      "capture_sha256": source_hash, "manifest_sha256": manifest_hash,
                      "observations": 14, "inputs": []}
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            identities = []
            for ansi in manifest["ansi_modes"]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
                for case in manifest["scenarios"]:
                    record = dict(case, ansi=ansi, timezone="UTC")
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
                        phase = "executed_schema_after_collect"
                        plan = execution.executedPlan()
                        stages["executed"] = json.loads(plan.schema().json())
                        plans["executed"] = plan.toString()
                        phase = "schema_consistency"
                        assert public == stages["analyzed"]
                        record["expected"] = {"schema": public, "rows": rows}
                    except PySparkException as error:
                        assert phase not in {"schema_consistency", "executed_schema_after_collect"}
                        condition = error.getCondition()
                        assert condition is not None, str(error)
                        record.update(expected={"condition": condition}, spark_error=str(error),
                                      spark_error_phase=phase, spark_error_class=type(error).__name__,
                                      spark_error_details={"condition": condition,
                                          "sql_state": error.getSqlState(),
                                          "message_parameters": error.getMessageParameters()})
                    record.update(schema_stages=stages, spark_plans=plans)
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
                    identities.append({"id": case["id"], "ansi": ansi})
            assert identities == manifest["identities"]
            assert sha256(Path(__file__)) == source_hash
            assert sha256(manifest_path) == manifest_hash
        finally:
            spark.stop()
    print(json.dumps({"observations": len(identities), "capture_sha256": source_hash,
                      "manifest_sha256": manifest_hash, "raw_sha256": sha256(args.output)}))


if __name__ == "__main__":
    main()
