"""Capture the 14 exact historical Decimal-to-integer replay corrections."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession


DIRECTORY = Path(__file__).parent
sys.path.insert(0, str(DIRECTORY / "decimal-integer-cast" / "interactions"))
from capture_interactions import capture_case


IDS = {
    "replay/existing-div-null-numeric-cases/36/tinyint_decimal_below_true",
    "replay/existing-div-null-numeric-cases/37/tinyint_decimal_above_true",
    "replay/existing-div-null-numeric-cases/110/smallint_decimal_below_true",
    "replay/existing-div-null-numeric-cases/111/smallint_decimal_above_true",
    "replay/existing-div-null-numeric-cases/184/int_decimal_below_true",
    "replay/existing-div-null-numeric-cases/185/int_decimal_above_true",
    "replay/existing-div-null-numeric-cases/258/bigint_decimal_below_true",
    "replay/existing-div-null-numeric-cases/259/bigint_decimal_above_true",
    "replay/focused-arithmetic-cases/150/unchanged_int_add_true",
    "replay/focused-arithmetic-cases/150/unchanged_int_add_false",
    "replay/focused-arithmetic-cases/169/unchanged_int_sub_true",
    "replay/focused-arithmetic-cases/169/unchanged_int_sub_false",
    "replay/focused-arithmetic-cases/188/unchanged_int_mul_true",
    "replay/focused-arithmetic-cases/188/unchanged_int_mul_false",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    corpus = DIRECTORY / "accepted-runtime.jsonl.gz"
    with gzip.open(corpus, "rt") as source:
        cases = [{key: case[key] for key in ("id", "sql", "ansi")}
                 for case in map(json.loads, source) if case["id"] in IDS]
    assert len(cases) == len(IDS) == 14
    assert {case["id"] for case in cases} == IDS
    revision = "32f7299601108917fb01920a54e084595b7b3bf8"
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + revision in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-decimal-integer-replay")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "spark_revision": revision,
                    "spark_build": build, "timezone": "UTC", "observations": 14,
                    "corpus_sha256": hashlib.sha256(corpus.read_bytes()).hexdigest(),
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "shared_capture_sha256": hashlib.sha256(
                        (DIRECTORY / "decimal-integer-cast" / "interactions"
                         / "capture_interactions.py").read_bytes()).hexdigest()}]
        for case in cases:
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            records.append(capture_case(spark, case))
        assert len(records) - 1 == 14
        with args.output.open("x") as output:
            output.writelines(json.dumps(record, separators=(",", ":")) + "\n"
                              for record in records)
        print(json.dumps({"observations": 14,
                          "errors": sum("condition" in r["expected"] for r in records[1:]),
                          "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
