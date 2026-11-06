"""Capture #722 inline VALUES stages and causes from pinned Spark.

The manifest contains SQL and provider inputs only. --self-check imports no Spark.
"""

import argparse
from decimal import Decimal
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import struct
import sys
import traceback
from zipfile import ZipFile

sys.dont_write_bytecode = True
PACKAGE = Path(__file__).resolve().parent
HELPER = PACKAGE.parent / "recapture_string_binary_spark.py"
spec = importlib.util.spec_from_file_location("inline_values_error_capture", HELPER)
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def encode(value):
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        return [encode(item) for item in value]
    if isinstance(value, float):
        return struct.pack("!d", value).hex()
    assert isinstance(value, (int, str, Decimal)), type(value)
    return str(value)


def self_check():
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    assert manifest["spark_version"] == "4.2.0"
    assert manifest["spark_revision"] == "32f7299601108917fb01920a54e084595b7b3bf8"
    assert manifest["source_commit"] == "b0b98f64981a5fde769e92ba87a3cf9257dd6c24"
    assert manifest["timezone"] == "UTC"
    assert manifest["ansi_modes"] == [True, False]
    cases = manifest["cases"]
    assert len(cases) == manifest["scenario_count"] == 36
    assert len({case["id"] for case in cases}) == len({case["sql"] for case in cases}) == 36
    assert manifest["observations"] == 72
    assert manifest["identities"] == [dict(id=case["id"], ansi=ansi)
                                      for ansi in manifest["ansi_modes"] for case in cases]
    assert all(set(case) == {"id", "sql", "ordered", "group"} for case in cases)
    assert all(isinstance(case["sql"], str) and case["sql"].isascii() for case in cases)
    provider, = manifest["inputs"]
    assert provider["name"] == "inline_runtime"
    assert provider["rows"] == [["0", "bad", "0"], ["1", "7", "1"]]
    assert encode((None, 7, Decimal("1.25"))) == [None, "7", "1.25"]
    return manifest


def capture_case(spark, case):
    phase, stages, plans = "sql", {}, {}
    record = dict(case, schema_stages=stages, spark_plans=plans)
    try:
        frame = spark.sql(case["sql"])
        execution = frame._jdf.queryExecution()
        phase = "public_schema"
        stages["public"] = frame.schema.jsonValue()
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
        rows = [encode(row) for row in frame.collect()]
        record["spark_collected_rows"] = rows
        if not case["ordered"]:
            rows = sorted(rows, key=lambda row: json.dumps(row, separators=(",", ":")))
        plan = execution.executedPlan()
        stages["executed"] = json.loads(plan.schema().json())
        plans["executed"] = plan.toString()
        record.update(expected={"schema": stages["analyzed"], "rows": rows},
                      row_count=len(rows))
    except Exception as error:
        details = helper.describe_error(error, phase)
        assert details["condition"] is not None, (case, details)
        record.update(expected={"condition": details["condition"]},
                      spark_error_phase=phase, spark_error=str(error),
                      spark_error_details=details, spark_error_traceback=traceback.format_exc())
        if phase == "collect":
            plan = execution.executedPlan()
            stages["executed"] = json.loads(plan.schema().json())
            plans["executed"] = plan.toString()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest = self_check()
    if args.self_check:
        assert args.output is None and "pyspark" not in sys.modules
        print(json.dumps({"source_check": "passed", "scenarios": 36, "observations": 72}))
        return
    assert args.output is not None and args.output.parent.is_dir()
    assert not args.output.exists(), args.output
    assert os.environ.get("TZ") == "UTC", "run with TZ=UTC"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] + "\n" in build, build
    assert "version=" + manifest["spark_version"] + "\n" in build, build
    spark = (SparkSession.builder.master("local[1]").appName(manifest["group"])
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        registered = []
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [tuple(int(value) if field.dataType.typeName() == "integer" else value
                          for value, field in zip(row, schema)) for row in provider["rows"]]
            assert [encode(row) for row in rows] == provider["rows"]
            frame = spark.createDataFrame(rows, schema)
            frame.createOrReplaceTempView(provider["name"])
            observed = [encode(row) for row in frame.orderBy("k").collect()]
            assert frame.schema.jsonValue() == provider["schema"]
            assert observed == provider["rows"]
            registered.append(dict(name=provider["name"], schema=frame.schema.jsonValue(), rows=observed))
        header = dict(manifest, spark_build=build, process_timezone=os.environ["TZ"],
                      manifest_sha256=sha256(PACKAGE / "manifest.json"),
                      capture_sha256=sha256(Path(__file__)), helper_sha256=sha256(HELPER),
                      registered_inputs=registered,
                      settings={row.key: row.value for row in spark.sql("SET -v").collect()},
                      spark_context_settings=dict(spark.sparkContext.getConf().getAll()))
        records = [header]
        for ansi in manifest["ansi_modes"]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in manifest["cases"]:
                record = capture_case(spark, dict(case, ansi=ansi, timezone="UTC"))
                records.append(record)
                print(f"{case['id']} ansi={ansi}: {record['expected']}", file=sys.stderr, flush=True)
        assert len(records) == 73
        assert [dict(id=record["id"], ansi=record["ansi"])
                for record in records[1:]] == manifest["identities"]
        with args.output.open("x") as output:
            output.writelines(json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n"
                              for record in records)
        print(json.dumps({"observations": 72, "sha256": sha256(args.output),
                          "errors": sum("condition" in record["expected"] for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
