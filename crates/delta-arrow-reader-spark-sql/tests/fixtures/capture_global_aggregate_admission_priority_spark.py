"""Capture independent aggregate nested/type priority and scalar-subquery admission."""

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
    manifest_path = HERE / "global-aggregate-admission-priority-cases.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["input_version"] == 1
    assert manifest["capture_kind"] == "global_aggregate_admission_priority"
    assert manifest["spark_revision"] == "32f7299601108917fb01920a54e084595b7b3bf8"
    assert manifest["ansi_modes"] == [True, False]
    assert hashlib.sha256((HERE / "global-aggregate-admission-cases.json").read_bytes()).hexdigest() == manifest["main_manifest_sha256"]
    cases = manifest["cases"]
    assert len(cases) == len({case["id"] for case in cases}) == 17
    assert sum("udf_sql" in case for case in cases) == 9
    assert all("expected" not in case for case in cases)
    sqls = {sql for case in cases for sql in [case["sql"]] + ([case["udf_sql"]] if "udf_sql" in case else [])}
    assert len(sqls) == 26
    assert {
        "SELECT COUNT_IF(SUM(RAND())>0) AS result FROM VALUES(1),(2)t(v)",
        "SELECT SUM(SUM(RAND())) AS result FROM VALUES(1),(2)t(v)",
        "SELECT COUNT(RAND()) AS a,SUM(ARRAY(v)) AS b FROM VALUES(1),(2)t(v)",
        "SELECT SUM(ARRAY(v)) AS b,COUNT(RAND()) AS a FROM VALUES(1),(2)t(v)",
        "SELECT SUM((SELECT RAND())) AS result FROM VALUES(1),(2)t(v)",
        "SELECT SUM((SELECT observed_arg(1))) AS result FROM VALUES(1),(2)t(v)",
    }.issubset(sqls)
    return manifest, hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest, manifest_sha256 = inventory()
    if args.self_check:
        print(json.dumps({"main_observations": 34, "udf_observations": 18,
                          "manifest_sha256": manifest_sha256}))
        return
    assert args.output is not None and not args.output.exists(), args.output

    import pyspark
    from pyspark.sql import SparkSession, functions, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-aggregate-priority-733")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        spark.udf.register("observed_arg", functions.udf(
            lambda _: 0.5, types.DoubleType()).asNondeterministic())
        header = {key: manifest[key] for key in ["input_version", "capture_kind",
            "spark_version", "spark_revision", "timezone", "main_manifest_sha256", "registered_udf"]}
        header.update({"manifest_sha256": manifest_sha256, "spark_build": build,
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
                    if "udf_sql" in case:
                        record["udf_reference"] = capture(spark, case["udf_sql"])
                    if "capture_failure" in record or "capture_failure" in record.get("udf_reference", {}):
                        failures.append((case["id"], ansi))
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
        assert not failures, ("capture failures preserved in output", failures)
        print(json.dumps({"main_observations": 34, "udf_observations": 18,
                          "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
