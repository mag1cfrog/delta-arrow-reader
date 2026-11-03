"""Capture unchanged date-day cases before and after scalar-subquery inlining."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent


def decode_input_rows(provider):
    fields = provider["schema"]["fields"]
    assert all(field["type"] in ["integer", "string"] for field in fields)
    return [tuple(None if value is None else
                  int(value) if field["type"] == "integer" else value
                  for value, field in zip(row, fields)) for row in provider["rows"]]


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
        for provider in records[0].get("inputs", []):
            decoded = decode_input_rows(provider)
            assert len(decoded) == len(provider["rows"])
            assert all(len(row) == len(provider["schema"]["fields"]) for row in decoded)
        selected = records[1:]
        if "selected_ids" in source:
            selected = [case for case in selected if case["id"] in source["selected_ids"]]
            assert len(selected) == source["selected_observations"]
            assert {case["id"] for case in selected} == set(source["selected_ids"])
        for case in selected:
            cases.append((source["path"], records[0], case))
    assert len(cases) == manifest["observations"] == 92
    assert len({(source, json.dumps(case, sort_keys=True))
                for source, _, case in cases}) == 92
    from collections import Counter
    assert Counter(source.removeprefix("sources/").removesuffix("-spark.jsonl")
                   for source, _, _ in cases) == manifest["counts"]
    for source in manifest["files"]:
        data = (ROOT / source["path"]).read_bytes()
        assert len(data) == source["bytes"], source["path"]
        assert hashlib.sha256(data).hexdigest() == source["sha256"], source["path"]
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
        _, header, _ = cases[0]
        providers = {provider["name"]: provider for provider in header["inputs"]}
        assert decode_input_rows(providers["date_day_outer_text"]) == [
            (None, "bad"), (0, None), (0, " 1 "), (1, "-1")]
        assert decode_input_rows(providers["date_day_outer_int"]) == [
            (None, 1), (0, None), (0, 2147483647), (1, -1)]
        assert any("2147483647" in json.dumps(case["expected"]) for _, _, case in cases)
        assert any("-2147483648" in json.dumps(case["expected"]) for _, _, case in cases)
        assert {case["ansi"] for _, _, case in cases} == {True, False}
        print(json.dumps({"observations": len(cases), "source_hashes_match": True,
                          "original_contracts_preserved": True,
                          "date32_extremes_preserved": True}))
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
        registered = {}
        records = [{**manifest, "spark_build": build,
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "capture_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}]
        for source, header, case in cases:
            for provider in header.get("inputs", []):
                if provider["name"] in registered:
                    assert registered[provider["name"]] == provider
                    continue
                schema = types.StructType.fromJson(provider["schema"])
                rows = decode_input_rows(provider)
                spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
                registered[provider["name"]] = provider
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            record = {"source": source, "original_observation": case, **case}
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
                internal_rows = execution.executedPlan().executeCollect()
                phase = "encode"
                rows = []
                for row in internal_rows:
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
                          "original_matches": sum(record["original_matches"] for record in records[1:]),
                          "errors": sum("error" in record for record in records[1:])}))
        assert all(record["original_matches"] for record in records[1:])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
