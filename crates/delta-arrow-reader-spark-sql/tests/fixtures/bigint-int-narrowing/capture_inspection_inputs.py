"""Capture four #813 inspection probes with the frozen full-stage observer."""

import argparse
import json
import math
import os
from pathlib import Path
import struct
import sys

import capture_bigint_int_narrowing as capture


ROOT = Path(__file__).resolve().parent
MANIFEST = ROOT / "inspection-manifest.json"
SCALAR_ENCODE = capture.encode


def encode_inspection_value(value):
    if isinstance(value, tuple):
        return [encode_inspection_value(item) for item in value]
    if isinstance(value, float):
        return "nan" if math.isnan(value) else struct.pack("!d", value).hex()
    return SCALAR_ENCODE(value)


def load_manifest():
    supplement = json.loads(MANIFEST.read_text())
    for source in supplement["preserved_sources"]:
        assert capture.file_sha256(ROOT / source["path"]) == source["sha256"]
    base, _ = capture.load_manifest()
    for key in ["spark_version", "spark_revision", "timezone", "settings"]:
        assert supplement[key] == base[key], key
    cases = [{**{key: value for key, value in case.items() if key != "ansi_modes"},
              "ansi": ansi}
             for case in supplement["cases"] for ansi in case["ansi_modes"]]
    assert len(supplement["cases"]) == 4
    assert len(cases) == supplement["observations"] == 8
    assert len({(case["id"], case["ansi"]) for case in cases}) == len(cases)
    names = {table["name"] for table in base["inputs"]}
    for case in supplement["cases"]:
        assert set(case) == {"id", "sql", "purpose", "domain", "ordered",
                             "input_tables", "ansi_modes"}
        assert case["ansi_modes"] == [True, False]
        assert case["domain"] == "inspection"
        assert isinstance(case["sql"], str) and case["sql"].startswith("SELECT ")
        assert type(case["ordered"]) is bool and case["purpose"]
        assert set(case["input_tables"]) <= names
    return {**supplement, "inputs": base["inputs"], "native": base["native"]}, cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    manifest, cases = load_manifest()
    assert encode_inspection_value((1, (None, -2))) == ["1", [None, "-2"]]
    assert encode_inspection_value(1.0) == "3ff0000000000000"
    if args.check:
        print(json.dumps({"queries": 4, "observations": len(cases),
                          "preserved_sources_verified": True, "engines_run": False}))
        return
    if args.output is None or args.output.exists() or args.output.is_symlink():
        parser.error("output must be a fresh path")
    from pyspark.sql import SparkSession

    # Only this process's serializer changes; all frozen observer bytes stay intact.
    capture.encode = encode_inspection_value
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = SparkSession.builder.master("local[1]").appName("dar-813-inspection-inputs")
    for key, value in manifest["settings"].items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        assert spark.version == manifest["spark_version"]
        spark.sparkContext.setLogLevel("ERROR")
        identity = capture.runtime_identity(spark)
        assert identity["spark_build"]["SPARK_REVISION"]["value"] == manifest["spark_revision"]
        registered = capture.register_inputs(spark, manifest)
        assert registered == [{key: table[key] for key in ["name", "schema", "rows"]}
                              for table in manifest["inputs"]]
        sources = [Path(__file__).resolve(), MANIFEST, Path(capture.__file__).resolve(),
                   ROOT.parent / "recapture_string_binary_spark.py",
                   *[ROOT / source["path"] for source in manifest["preserved_sources"]]]
        sources = list(dict.fromkeys(path.resolve() for path in sources))
        header = {**{key: value for key, value in manifest.items() if key != "cases"},
                  **identity, "registered_inputs": registered,
                  "settings": {row.key: row.value for row in spark.sql("SET -v").collect()},
                  "spark_context_settings": dict(spark.sparkContext.getConf().getAll()),
                  "capture_files": [{"path": str(path), "sha256": capture.file_sha256(path),
                                     "bytes": path.stat().st_size} for path in sources]}
        with args.output.open("x", encoding="utf-8") as output:
            output.write(json.dumps(header, separators=(",", ":"), ensure_ascii=True) + "\n")
            output.flush()
            for case in cases:
                spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
                record = capture.observe_stages(spark, case)
                record["settings"] = {key: spark.conf.get(key) for key in
                                      [*manifest["settings"], "spark.sql.ansi.enabled"]}
                output.write(json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n")
                output.flush()
                print(f"{case['id']} ansi={case['ansi']}", file=sys.stderr, flush=True)
        assert len(args.output.read_bytes().splitlines()) == len(cases) + 1
        print(json.dumps({"output": str(args.output), "observations": len(cases),
                          "sha256": capture.file_sha256(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
