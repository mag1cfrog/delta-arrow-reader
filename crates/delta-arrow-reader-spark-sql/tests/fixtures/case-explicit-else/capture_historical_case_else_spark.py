"""Capture exact historical CASE selectors using independent pinned Spark."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


SPARK_VERSION = "4.2.0"
SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CORPUS_SHA256 = "4a6de91a60bd9d6db19e8c373e33e45bb393f9f767316d88c41547edd612a271"


def load_cases(fixtures, selectors_path):
    selectors = json.loads(selectors_path.read_text())
    assert isinstance(selectors, list) and selectors, "nonempty selector list required"
    selected = {}
    for case in selectors:
        assert set(case) == {"id", "sql", "ansi"}, case
        assert isinstance(case["id"], str) and case["id"], case
        assert isinstance(case["sql"], str) and case["sql"], case
        assert type(case["ansi"]) is bool, case
        assert case["id"] not in selected, case["id"]
        selected[case["id"]] = case
    corpus = fixtures / "accepted-runtime.jsonl.gz"
    assert hashlib.sha256(corpus.read_bytes()).hexdigest() == CORPUS_SHA256, corpus
    manifest = json.loads((fixtures / "accepted-runtime.json").read_text())
    assert manifest["fixture_sha256"] == CORPUS_SHA256, manifest
    assert manifest["unique_observations"] == 25180, manifest
    cases = []
    seen = set()
    with gzip.open(corpus, "rt") as source:
        for record in map(json.loads, source):
            assert record["id"] not in seen, record["id"]
            seen.add(record["id"])
            if record["id"] in selected:
                case = {key: record[key] for key in ("id", "sql", "ansi")}
                assert case == selected[case["id"]], case
                cases.append(case)
    assert len(seen) == 25180, len(seen)
    assert len(cases) == len(selectors), "selectors must identify original observations"
    assert {case["id"] for case in cases} == set(selected)
    return corpus, cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixtures", type=Path)
    parser.add_argument("selectors", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    corpus, cases = load_cases(args.fixtures, args.selectors)
    helper = args.fixtures / "decimal-integer-cast" / "interactions" / "capture_interactions.py"
    sys.path.insert(0, str(helper.parent))
    from capture_interactions import capture_case
    import pyspark
    from pyspark.sql import SparkSession

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + SPARK_REVISION in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-case-else-historical")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == SPARK_VERSION, spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "spark_revision": SPARK_REVISION,
                    "spark_build": build, "timezone": "UTC", "observations": len(cases),
                    "corpus_sha256": hashlib.sha256(corpus.read_bytes()).hexdigest(),
                    "selectors_sha256": hashlib.sha256(args.selectors.read_bytes()).hexdigest(),
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "shared_capture_sha256": hashlib.sha256(helper.read_bytes()).hexdigest()}]
        for case in cases:
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            records.append(capture_case(spark, case))
        assert len(records) - 1 == len(cases)
        assert {record["id"] for record in records[1:]} == {case["id"] for case in cases}
        with args.output.open("x") as output:
            output.writelines(json.dumps(record, separators=(",", ":")) + "\n"
                              for record in records)
        print(json.dumps({"observations": len(cases),
                          "errors": sum("condition" in r["expected"] for r in records[1:]),
                          "output": str(args.output),
                          "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
