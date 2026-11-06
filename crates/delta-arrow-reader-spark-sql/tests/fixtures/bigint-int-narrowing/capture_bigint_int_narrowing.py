"""Capture #813 numeric CAST stages independently; --check needs no Spark."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
from recapture_string_binary_spark import (  # noqa: E402
    describe_error, encode, file_sha256, register_inputs, runtime_identity,
)


def load_manifest():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    original = manifest["original_reference"]
    data = (ROOT / original["path"]).read_bytes()
    assert hashlib.sha256(data).hexdigest() == original["sha256"]
    assert len(data.splitlines()) - 1 == original["records"] == 10
    cases = [{**{k: v for k, v in case.items() if k != "ansi_modes"}, "ansi": ansi}
             for case in manifest["cases"] for ansi in case["ansi_modes"]]
    assert len({(case["id"], case["ansi"]) for case in cases}) == len(cases)
    assert len(cases) == manifest["observations"] == 182
    assert dict(Counter(case["domain"] for case in cases)) == manifest["counts"]
    assert manifest["counts"] == {"direct": 162, "adapter": 20}
    assert len({table["name"] for table in manifest["inputs"]}) == len(manifest["inputs"])
    for table in manifest["inputs"]:
        fields = table["schema"]["fields"]
        assert [field["type"] for field in fields] == ["integer", "long"]
        for row in table["rows"]:
            assert len(row) == len(fields)
            for value, field in zip(row, fields):
                if value is None:
                    assert field["nullable"]
                else:
                    assert isinstance(value, str) and str(int(value)) == value
                    bits = 32 if field["type"] == "integer" else 64
                    assert -(1 << (bits - 1)) <= int(value) < 1 << (bits - 1)
    required = next(table for table in manifest["inputs"] if table["name"] == "narrow_required")
    assert required["rows"][0][1] == "-9223372036854775808"
    assert required["rows"][-1][1] == "9223372036854775807"
    assert manifest["native"]["batch_sizes"] == [1, 32]
    assert manifest["native"]["target_partitions"] == 2
    return manifest, cases


def observe_stages(spark, case):
    phase = "sql"
    stages = {}
    plans = {}
    record = {**case, "schema_stages": stages, "plans": plans}
    execution = None
    try:
        frame = spark.sql(case["sql"])
        execution = frame._jdf.queryExecution()
        phase = "analyzed"
        record["dataframe_schema"] = frame.schema.jsonValue()
        stages["analyzed"] = json.loads(execution.analyzed().schema().json())
        assert record["dataframe_schema"] == stages["analyzed"]
        plans["analyzed"] = execution.analyzed().toString()
        phase = "optimized"
        stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
        plans["optimized"] = execution.optimizedPlan().toString()
        phase = "executed_before_collect"
        stages["executed_before_collect"] = json.loads(execution.executedPlan().schema().json())
        plans["executed_before_collect"] = execution.executedPlan().toString()
        phase = "collect"
        rows = [[encode(value) for value in row] for row in frame.collect()]
        stages["executed"] = json.loads(execution.executedPlan().schema().json())
        plans["executed"] = execution.executedPlan().toString()
        record["expected"] = {"schema": record["dataframe_schema"], "rows": rows}
    except Exception as error:
        details = describe_error(error, phase)
        if details["condition"] is None:
            raise
        record.update(expected={"condition": details["condition"]},
                      spark_error_phase=phase, spark_error_details=details)
        if phase == "collect" and execution is not None:
            stages["executed"] = json.loads(execution.executedPlan().schema().json())
            plans["executed"] = execution.executedPlan().toString()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    manifest, cases = load_manifest()
    if args.check:
        print(json.dumps({"observations": len(cases), "counts": manifest["counts"],
                          "original_bytes_verified": True, "engines_run": False}))
        return
    if args.output is None or args.output.exists() or args.output.is_symlink():
        parser.error("output must be a fresh path")
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = SparkSession.builder.master("local[1]").appName("dar-bigint-int-narrowing-813")
    for key, value in manifest["settings"].items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        assert spark.version == manifest["spark_version"]
        spark.sparkContext.setLogLevel("ERROR")
        identity = runtime_identity(spark)
        assert identity["spark_build"]["SPARK_REVISION"]["value"] == manifest["spark_revision"]
        registered = register_inputs(spark, manifest)
        assert registered == [{k: table[k] for k in ["name", "schema", "rows"]}
                              for table in manifest["inputs"]]
        header = {**{k: v for k, v in manifest.items() if k != "cases"}, **identity,
                  "registered_inputs": registered,
                  "settings": {row.key: row.value for row in spark.sql("SET -v").collect()},
                  "spark_context_settings": dict(spark.sparkContext.getConf().getAll()),
                  "capture_files": [{"path": str(path), "sha256": file_sha256(path),
                                     "bytes": path.stat().st_size} for path in
                                    [Path(__file__).resolve(), ROOT / "manifest.json",
                                     ROOT.parent / "recapture_string_binary_spark.py"]]}
        # Flush each raw observation so a failed capture leaves its completed evidence.
        with args.output.open("x", encoding="utf-8") as output:
            output.write(json.dumps(header, separators=(",", ":"), ensure_ascii=True) + "\n")
            output.flush()
            for case in cases:
                spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
                record = observe_stages(spark, case)
                record["settings"] = {key: spark.conf.get(key) for key in
                                      [*manifest["settings"], "spark.sql.ansi.enabled"]}
                output.write(json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n")
                output.flush()
                print(f"{case['id']} ansi={case['ansi']}", file=sys.stderr, flush=True)
        assert len(args.output.read_bytes().splitlines()) == len(cases) + 1
        print(json.dumps({"output": str(args.output), "observations": len(cases),
                          "sha256": file_sha256(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
