"""Capture scalar CASE declarations and evaluation from pinned Spark."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types


def capture_case(spark, case):
    phase = "sql"
    stages = {}
    record = dict(case, schema_stages=stages)
    try:
        frame = spark.sql(case["sql"])
        execution = frame._jdf.queryExecution()
        stages["analyzed"] = json.loads(execution.analyzed().schema().json())
        assert frame.schema.jsonValue() == stages["analyzed"], case
        phase = "optimized"
        stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
        phase = "executed_before_collect"
        stages["executed_before_collect"] = json.loads(execution.executedPlan().schema().json())
        phase = "collect"
        rows = [[None if value is None else str(value) for value in row]
                for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        stages["executed"] = json.loads(execution.executedPlan().schema().json())
        record["expected"] = {"schema": stages["analyzed"], "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        assert condition is not None, (case, phase, str(error))
        record.update(expected={"condition": condition}, spark_error_phase=phase,
                      spark_error=str(error))
        if phase == "collect":
            stages["executed"] = json.loads(execution.executedPlan().schema().json())
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest_path = Path(__file__).with_name("manifest.json")
    manifest = json.loads(manifest_path.read_text())
    assert len({case["id"] for case in manifest["cases"]}) == len(manifest["cases"])
    assert 2 * len(manifest["cases"]) == manifest["observations"] == 58
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-case-explicit-else")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", manifest["timezone"])
             .getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        header = {key: value for key, value in manifest.items() if key != "cases"}
        header.update(spark_build=build,
                      manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                      capture_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        records = [header]
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [tuple(None if value is None else int(value) for value in row)
                    for row in provider["rows"]]
            assert [[None if value is None else str(value) for value in row]
                    for row in rows] == provider["rows"]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            records.extend(capture_case(spark, dict(case, ansi=ansi))
                           for case in manifest["cases"])
        assert len(records) - 1 == manifest["observations"]
        assert len({(record["id"], record["ansi"])
                    for record in records[1:]}) == manifest["observations"]
        with args.output.open("x") as output:
            output.writelines(json.dumps(record, separators=(",", ":")) + "\n"
                              for record in records)
        print(json.dumps({"observations": len(records) - 1,
                          "errors": sum("condition" in r["expected"] for r in records[1:]),
                          "output": str(args.output),
                          "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
