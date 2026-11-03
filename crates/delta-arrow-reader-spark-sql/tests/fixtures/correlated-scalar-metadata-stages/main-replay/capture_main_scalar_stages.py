"""Capture all 152 unchanged main scalar-replay observations at every Spark stage."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent
MANIFEST = ROOT / "capture-inputs-and-cases.json"


def validate_manifest(manifest):
    for source in manifest["source_files"]:
        data = (ROOT / source["path"]).read_bytes()
        assert len(data) == source["bytes"], source["path"]
        assert hashlib.sha256(data).hexdigest() == source["sha256"], source["path"]
    original = [json.loads(line) for line in
                (ROOT / "sources/single-row-correlated-spark.jsonl").read_text().splitlines()]
    assert len(original) - 1 == manifest["original_observations"] == 202
    assert original[0]["inputs"] == manifest["inputs"]
    selected = []
    excluded = []
    for index, case in enumerate(original[1:], 1):
        case_id = case["id"]
        if ("_correlated_" in case_id or "_projected_in" in case_id
                or "_volatile_direct_" in case_id or "_volatile_nested_" in case_id
                or case_id == "volatile_rand_raw"):
            excluded.append({"source_index": index, "id": case_id, "ansi": case["ansi"]})
        else:
            selected.append((index, case))
    assert len(selected) == manifest["observations"] == len(manifest["cases"]) == 152
    assert excluded == manifest["excluded_original_identities"]
    assert len(excluded) == 50
    assert len({(case["id"], case["ansi"]) for _, case in selected}) == 152
    for planned, (index, case) in zip(manifest["cases"], selected):
        assert planned["source_index"] == index
        assert planned["original_observation"] == case
        assert all(planned[key] == case[key] for key in ["id", "ansi", "sql"])
    assert Counter(case["expected"]["status"] for _, case in selected) == {
        "ok": 130, "error": 22}
    assert sum("evaluated_rows" in case for _, case in selected) == 8


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest = json.loads(MANIFEST.read_text())
    validate_manifest(manifest)

    import pyspark
    from pyspark.sql import SparkSession, functions, types

    sys.path.insert(0, str(ROOT / "sources"))
    from capture_correlated_scalar_metadata_stages import capture_case, decode
    from capture_single_row_correlated_spark import encode

    def encode_row(row, schema, *, input_value=False):
        assert len(row) == len(schema)
        return [encode(value, field.dataType) for value, field in zip(row, schema)]

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={manifest['spark_revision']}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-818-complete-main-scalar-schema-stages")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"]
        assert spark.conf.get("spark.sql.session.timeZone") == manifest["timezone"]
        spark.sparkContext.setLogLevel("ERROR")
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [decode(row, schema, types) for row in provider["rows"]]
            assert [encode_row(row, schema, input_value=True) for row in rows] == provider["rows"]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
        calls = spark.sparkContext.accumulator(0)

        def count_row(value):
            calls.add(1)
            return value

        spark.udf.register("count_row", functions.udf(
            count_row, types.IntegerType()).asNondeterministic())
        records = [{key: value for key, value in manifest.items() if key != "cases"}]
        records[0].update({"spark_build": build,
                           "capture_script_sha256": hashlib.sha256(
                               Path(__file__).read_bytes()).hexdigest(),
                           "capture_manifest_sha256": hashlib.sha256(
                               MANIFEST.read_bytes()).hexdigest()})
        for case in manifest["cases"]:
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            query_case = {key: value for key, value in case.items()
                          if key != "original_observation"}
            record = capture_case(spark, query_case, calls, encode_row)
            record["evaluated_rows"] = calls.value
            record["original_observation"] = original = case["original_observation"]
            original_expected = original["expected"]
            succeeded = "expected" in record
            record["original_status_matches"] = (
                succeeded == (original_expected["status"] == "ok"))
            if original_expected["status"] == "ok":
                record["original_public_matches"] = (
                    record.get("public_schema") == original_expected["schema"])
                record["original_rows_match"] = (
                    succeeded and record["expected"]["rows"] == original_expected["rows"])
            else:
                record["original_condition_matches"] = (
                    record.get("error", {}).get("condition") == original_expected["condition"])
            if "evaluated_rows" in original:
                record["original_evaluations_match"] = calls.value == original["evaluated_rows"]
            records.append(record)
        assert len(records) - 1 == 152
        assert len({(record["id"], record["ansi"]) for record in records[1:]}) == 152
        assert all(("expected" in record) != ("error" in record) for record in records[1:])
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": 152, "output": str(args.output),
                          "errors": sum("error" in record for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
