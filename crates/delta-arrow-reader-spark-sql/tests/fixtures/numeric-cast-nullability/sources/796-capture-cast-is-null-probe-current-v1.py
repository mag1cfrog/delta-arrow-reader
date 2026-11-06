"""Capture the six #796 CAST IS NULL preservation witnesses; --check imports no Spark."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys


sys.dont_write_bytecode = True
MANIFEST = Path(__file__).with_name("796-cast-is-null-probe-current-v1-manifest.json")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_manifest():
    manifest = json.loads(MANIFEST.read_text())
    fixtures = Path(manifest["fixtures_root"])
    original = manifest["source_manifest"]
    assert digest(Path(original["path"])) == original["sha256"]
    previous = json.loads(Path(original["path"]).read_text())
    assert manifest["inputs"] == previous["inputs"]
    assert manifest["native"] == previous["native"]
    assert len(manifest["inputs"]) == 5
    for item in manifest["immutable_existing_evidence"]:
        assert digest(Path(item["path"])) == item["sha256"]
    for path, expected in manifest["helper_sources"].items():
        assert digest(fixtures / path) == expected
    assert manifest["spark_version"] == "4.2.0"
    assert manifest["spark_revision"] == "32f7299601108917fb01920a54e084595b7b3bf8"
    assert manifest["timezone"] == "UTC"
    assert len(manifest["cases"]) == manifest["case_count"] == 6
    assert manifest["observations"] == 12 and manifest["native_observations"] == 24
    assert len({case["id"] for case in manifest["cases"]}) == 6
    assert all(case["ansi_modes"] == [True, False] for case in manifest["cases"])
    assert all(not {"expected", "schema_stages"}.intersection(case) for case in manifest["cases"])
    return manifest, fixtures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    manifest, fixtures = load_manifest()
    cases = [{**{key: value for key, value in case.items() if key != "ansi_modes"}, "ansi": ansi}
             for case in manifest["cases"] for ansi in case["ansi_modes"]]
    if args.check:
        assert args.output is None and "pyspark" not in sys.modules
        print(json.dumps({"SQL": 6, "observations": 12, "native_observations": 24,
                          "original_inputs_and_evidence_verified": True, "engines_run": False}))
        return
    if args.output is None or args.output.exists() or args.output.is_symlink():
        parser.error("output must be a fresh path")
    assert args.output.parent.is_dir()
    assert os.environ.get("TZ") == "UTC"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sys.path.insert(0, str(fixtures / "bigint-int-narrowing"))
    from capture_bigint_int_narrowing import observe_stages
    from recapture_string_binary_spark import runtime_identity
    from pyspark.sql import SparkSession, types
    sys.path.insert(0, str(fixtures / "decimal-integer-cast"))
    from capture_decimal_to_integer_declarations import decode, encode

    builder = SparkSession.builder.master("local[1]").appName("dar-cast-is-null-preservation-796")
    for key, value in manifest["settings"].items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        assert spark.version == manifest["spark_version"]
        spark.sparkContext.setLogLevel("ERROR")
        identity = runtime_identity(spark)
        assert identity["spark_build"]["SPARK_REVISION"]["value"] == manifest["spark_revision"]
        registered = []
        for table in manifest["inputs"]:
            schema = types.StructType.fromJson(table["schema"])
            rows = [decode(row, schema) for row in table["rows"]]
            assert [encode(row, schema, input_value=True) for row in rows] == table["rows"]
            frame = spark.createDataFrame(rows, schema)
            assert frame.schema.jsonValue() == table["schema"]
            observed = frame.orderBy(schema.fields[0].name).collect()
            assert [encode(row, schema, input_value=True) for row in observed] == table["rows"]
            frame.createOrReplaceTempView(table["name"])
            registered.append({key: table[key] for key in ["name", "schema", "rows"]})
        files = [Path(__file__).resolve(), MANIFEST, *(fixtures / path for path in manifest["helper_sources"])]
        header = {key: value for key, value in manifest.items() if key != "cases"}
        header.update(identity, registered_inputs=registered,
                      settings={row.key: row.value for row in spark.sql("SET -v").collect()},
                      spark_context_settings=dict(spark.sparkContext.getConf().getAll()),
                      manifest_sha256=digest(MANIFEST),
                      capture_files=[dict(path=str(path), sha256=digest(path), bytes=path.stat().st_size) for path in files])
        with args.output.open("x", encoding="utf-8") as output:
            output.write(json.dumps(header, ensure_ascii=True, separators=(",", ":")) + "\n")
            output.flush()
            for case in cases:
                mode = str(case["ansi"]).lower()
                spark.conf.set("spark.sql.ansi.enabled", mode)
                assert spark.conf.get("spark.sql.ansi.enabled") == mode
                record = observe_stages(spark, case)
                assert spark.conf.get("spark.sql.ansi.enabled") == mode
                record["settings"] = {key: spark.conf.get(key) for key in [*manifest["settings"], "spark.sql.ansi.enabled"]}
                output.write(json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n")
                output.flush()
                print(f"{case['id']} ansi={case['ansi']}", file=sys.stderr, flush=True)
        assert len(args.output.read_bytes().splitlines()) == 13
        print(json.dumps({"output": str(args.output), "observations": 12, "sha256": digest(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
