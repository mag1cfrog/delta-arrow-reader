"""Capture CASE and explicit/internal error boundaries from pinned Spark 4.2.0."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

sys.dont_write_bytecode = True
helper = Path(__file__).resolve().parents[1] / "case-explicit-else" / "capture_case_explicit_else.py"
sys.path.insert(0, str(helper.parent))
from capture_case_explicit_else import capture_case


def main():
    import pyspark
    from pyspark.sql import SparkSession, types

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest_path = Path(__file__).with_name("manifest.json")
    manifest = json.loads(manifest_path.read_text())
    assert len({(case["id"], case["input"]) for case in manifest["cases"]}) == len(manifest["cases"])
    assert 2 * len(manifest["cases"]) == manifest["observations"] == 92
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-860-conditional-errors")
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
                      capture_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      helper_sha256=hashlib.sha256(helper.read_bytes()).hexdigest())
        records = [header]
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [tuple(None if value is None else
                          int(value) if isinstance(field.dataType, types.IntegerType) else value
                          for value, field in zip(row, schema)) for row in provider["rows"]]
            assert [[None if value is None else str(value) for value in row]
                    for row in rows] == provider["rows"]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            records.extend(capture_case(spark, dict(case, ansi=ansi))
                           for case in manifest["cases"])
        assert len(records) - 1 == manifest["observations"]
        assert len({(record["id"], record["input"], record["ansi"])
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
