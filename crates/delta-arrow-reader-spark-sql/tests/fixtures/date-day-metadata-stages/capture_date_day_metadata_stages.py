"""Capture unchanged date-day cases before and after scalar-subquery inlining."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent


def cases_from_manifest(manifest):
    cases = []
    for source in manifest["sources"]:
        data = (ROOT / source["path"]).read_bytes()
        assert len(data) == source["bytes"], source["path"]
        assert hashlib.sha256(data).hexdigest() == source["sha256"], source["path"]
        records = [json.loads(line) for line in data.decode().splitlines()]
        assert len(records) - 1 == source["observations"]
        assert records[0]["spark_version"] == manifest["spark_version"]
        assert records[0]["timezone"] == manifest["timezone"]
        for case in records[1:]:
            cases.append((source["path"], records[0], case))
    assert len(cases) == manifest["observations"] == 68
    assert len({(case["id"], case["ansi"]) for _, _, case in cases}) == 68
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    parser.add_argument("--check", action="store_true", help="validate sources without Spark")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest_path = ROOT / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    cases = cases_from_manifest(manifest)
    if args.check:
        print(json.dumps({"observations": len(cases), "source_hashes_match": True}))
        return

    import pyspark
    from py4j.protocol import Py4JError
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={manifest['spark_revision']}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-date-day-metadata-stages")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"]
        assert spark.conf.get("spark.sql.session.timeZone") == manifest["timezone"]
        spark.sparkContext.setLogLevel("ERROR")
        registered = set()
        records = [{**manifest, "spark_build": build,
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "capture_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}]
        for source, header, case in cases:
            for provider in header.get("inputs", []):
                if provider["name"] in registered:
                    continue
                schema = types.StructType.fromJson(provider["schema"])
                rows = [tuple(None if value is None else
                              int(value) if isinstance(field.dataType, types.IntegerType()) else value
                              for value, field in zip(row, schema)) for row in provider["rows"]]
                spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
                registered.add(provider["name"])
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            record = {"source": source, **case}
            phase = "resolve"
            try:
                frame = spark.sql(case["sql"])
                assert frame.columns == ["r"], frame.columns
                record["public_schema"] = frame.schema.jsonValue()
                execution = frame._jdf.queryExecution()
                record["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
                record["analyzed_plan"] = execution.analyzed().toString()
                phase = "optimize"
                record["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
                record["optimized_plan"] = execution.optimizedPlan().toString()
                phase = "physical"
                record["executed_schema_before_collect"] = json.loads(execution.executedPlan().schema().json())
                phase = "execute"
                # Internal rows retain every Date32 day, including dates outside Python's range.
                rows = []
                for row in execution.executedPlan().executeCollect():
                    if row.isNullAt(0):
                        value = None
                    elif isinstance(frame.schema[0].dataType, types.DateType):
                        value = str(row.getInt(0))
                    else:
                        assert isinstance(frame.schema[0].dataType, types.ArrayType)
                        assert isinstance(frame.schema[0].dataType.elementType, types.DateType)
                        values = row.getArray(0)
                        value = [None if values.isNullAt(i) else str(values.getInt(i))
                                 for i in range(values.numElements())]
                    rows.append([value])
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                record["executed_schema_after_collect"] = json.loads(execution.executedPlan().schema().json())
                record["physical_plan"] = execution.executedPlan().toString()
                record["observed"] = {"schema": record["public_schema"], "rows": rows}
                record["original_matches"] = record["observed"] == case["expected"]
            except Exception as error:
                condition = getattr(error, "getCondition", lambda: None)()
                cause = getattr(error, "java_exception", None)
                while condition is None and cause is not None:
                    try:
                        condition = cause.getCondition()
                    except Py4JError:
                        pass
                    cause = cause.getCause()
                record["error"] = {"condition": condition, "phase": phase, "message": str(error)}
                record["original_matches"] = condition is not None and case["expected"] == {"condition": condition}
            records.append(record)
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(cases), "output": str(args.output),
                          "original_matches": sum(record["original_matches"] for record in records[1:])}))
        assert all(record["original_matches"] for record in records[1:])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
