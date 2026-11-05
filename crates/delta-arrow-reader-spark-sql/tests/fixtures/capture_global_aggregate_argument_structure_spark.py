"""Capture ordinary aggregate structure and separate scalar-subquery scopes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

from capture_global_aggregate_admission_spark import capture


HERE = Path(__file__).resolve().parent


def inventory():
    path = HERE / "global-aggregate-argument-structure-cases.json"
    manifest = json.loads(path.read_text())
    assert manifest["input_version"] == 1
    assert manifest["capture_kind"] == "aggregate_argument_structure"
    assert manifest["spark_revision"] == "32f7299601108917fb01920a54e084595b7b3bf8"
    assert manifest["ansi_modes"] == [True, False]
    assert len(manifest["cases"]) == len({case["id"] for case in manifest["cases"]}) == 5
    assert all("expected" not in case for case in manifest["cases"])
    return manifest, hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest, digest = inventory()
    if args.self_check:
        print(json.dumps({"observations": 10, "manifest_sha256": digest}))
        return
    assert args.output is not None and not args.output.exists(), args.output

    import pyspark
    from pyspark.sql import SparkSession

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-aggregate-subquery-scope-733")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        header = {key: manifest[key] for key in ["input_version", "capture_kind",
            "spark_version", "spark_revision", "timezone"]}
        header.update({"manifest_sha256": digest, "spark_build": build,
            "settings": {key: spark.conf.get(key) for key in [
                "spark.sql.session.timeZone", "spark.sql.shuffle.partitions",
                "spark.sql.optimizer.excludedRules", "spark.sql.caseSensitive"]}})
        failures = []
        with args.output.open("x") as output:
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            for ansi in manifest["ansi_modes"]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for case in manifest["cases"]:
                    record = {**case, "ansi": ansi, **capture(spark, case["sql"])}
                    if "capture_failure" in record:
                        failures.append((case["id"], ansi))
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
        assert not failures, ("capture failures retained in output", failures)
        print(json.dumps({"observations": 10, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
