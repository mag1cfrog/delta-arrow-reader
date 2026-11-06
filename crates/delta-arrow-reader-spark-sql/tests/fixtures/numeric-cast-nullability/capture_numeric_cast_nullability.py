"""Capture #796 integer CAST stages independently; --check imports no Spark."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import struct
import sys


sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT.parent
sys.path.insert(0, str(FIXTURES / "bigint-int-narrowing"))
from capture_bigint_int_narrowing import observe_stages  # noqa: E402
from recapture_string_binary_spark import file_sha256, runtime_identity  # noqa: E402


def load_manifest():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    assert manifest["spark_version"] == "4.2.0"
    assert manifest["spark_revision"] == "32f7299601108917fb01920a54e084595b7b3bf8"
    assert manifest["timezone"] == "UTC"
    cases = manifest["cases"]
    assert len(cases) == manifest["case_count"] == 132
    assert len({case["id"] for case in cases}) == len(cases)
    assert dict(Counter(case["domain"] for case in cases)) == manifest["case_counts"]
    assert sum(len(case["ansi_modes"]) for case in cases) == manifest["observations"] == 264
    assert all(case["ansi_modes"] == [True, False] for case in cases)
    assert all(not {"expected", "schema_stages"}.intersection(case) for case in cases)
    for relative, digest in manifest["helper_sources"].items():
        assert file_sha256(FIXTURES / relative) == digest, relative
    for source in manifest["original_references"]:
        data = (FIXTURES / source["path"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == source["sha256"]
        lines = data.splitlines(keepends=True)
        header = json.loads(lines[0])
        for original_input in header.get("registered_inputs", header.get("inputs", [])):
            for current_input in manifest["inputs"]:
                if current_input["name"] == original_input["name"]:
                    assert current_input == original_input
        for selected in source["selected_records"]:
            line = lines[selected["line"] - 1]
            assert hashlib.sha256(line).hexdigest() == selected["sha256"]
            recorded = json.loads(line)
            case, = [case for case in cases if case["id"] == recorded["id"]]
            assert case["domain"] == "original_c06"
            assert case["sql"] == recorded["sql"]
            assert selected["ansi"] == recorded["ansi"]
    assert sum(len(source["selected_records"])
               for source in manifest["original_references"]) == 18
    assert len({table["name"] for table in manifest["inputs"]}) == 7
    assert len(manifest["width_probe_input_names"]) == 6
    assert len([case for case in cases if case["domain"] == "integer_target_controls"]) == 54
    fitting, = [table for table in manifest["inputs"]
                if table["name"] == "numeric_cast_fitting"]
    required, = [table for table in manifest["inputs"]
                 if table["name"] == "numeric_cast_required"]
    assert fitting["rows"] == required["rows"][:6]
    assert fitting["schema"] == required["schema"]
    assert all(not field["nullable"] for field in fitting["schema"]["fields"])
    assert all(value is not None for row in fitting["rows"] for value in row)
    for row in fitting["rows"]:
        for value, encoding in zip(row[1:], ["!f", "!d"]):
            assert -128 <= struct.unpack(encoding, bytes.fromhex(value))[0] <= 127
    nullable, = [table for table in manifest["inputs"]
                 if table["name"] == "numeric_cast_nullable"]
    assert all(field["nullable"] for field in nullable["schema"]["fields"][1:])
    assert any(row[1:] == [None, None] for row in nullable["rows"])
    empty, = [table for table in manifest["inputs"]
              if table["name"] == "numeric_cast_empty"]
    assert empty["rows"] == []
    assert len([case for case in cases if case["domain"] == "generated_names"]) == 2
    assert manifest["native"]["batch_sizes"] == [1, 32]
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--width-probe-only", action="store_true")
    args = parser.parse_args()
    manifest = load_manifest()
    cases = [case for case in manifest["cases"]
             if not args.width_probe_only or case["domain"] == "integer_width_probe"]
    expanded = [{**{key: value for key, value in case.items() if key != "ansi_modes"},
                 "ansi": ansi} for case in cases for ansi in case["ansi_modes"]]
    assert len(expanded) == (32 if args.width_probe_only else 264)
    if args.check:
        assert args.output is None
        assert "pyspark" not in sys.modules
        print(json.dumps({"cases": len(cases), "observations": len(expanded),
                          "original_bytes_verified": True, "engines_run": False}))
        return
    if args.output is None or args.output.exists() or args.output.is_symlink():
        parser.error("output must be a fresh path")
    assert args.output.parent.is_dir()
    assert os.environ.get("TZ") == "UTC"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession, types
    sys.path.insert(0, str(FIXTURES / "decimal-integer-cast"))
    from capture_decimal_to_integer_declarations import decode, encode

    builder = SparkSession.builder.master("local[1]").appName("dar-numeric-cast-nullability-796")
    for key, value in manifest["settings"].items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        assert spark.version == manifest["spark_version"]
        spark.sparkContext.setLogLevel("ERROR")
        identity = runtime_identity(spark)
        assert identity["spark_build"]["SPARK_REVISION"]["value"] == manifest["spark_revision"]
        registered = []
        tables = [table for table in manifest["inputs"] if not args.width_probe_only
                  or table["name"] in manifest["width_probe_input_names"]]
        for table in tables:
            schema = types.StructType.fromJson(table["schema"])
            rows = [decode(row, schema) for row in table["rows"]]
            assert [encode(row, schema, input_value=True) for row in rows] == table["rows"]
            frame = spark.createDataFrame(rows, schema)
            assert frame.schema.jsonValue() == table["schema"]
            actual_rows = frame.orderBy(schema.fields[0].name).collect()
            assert [encode(row, schema, input_value=True) for row in actual_rows] == table["rows"]
            frame.createOrReplaceTempView(table["name"])
            registered.append({key: table[key] for key in ["name", "schema", "rows"]})
        header = {key: value for key, value in manifest.items() if key != "cases"}
        header.update(identity, registered_inputs=registered,
                      inputs=tables,
                      case_count=len(cases), observations=len(expanded),
                      case_counts=dict(Counter(case["domain"] for case in cases)),
                      selection="integer_width_probe" if args.width_probe_only else "all",
                      settings={row.key: row.value for row in spark.sql("SET -v").collect()},
                      spark_context_settings=dict(spark.sparkContext.getConf().getAll()),
                      manifest_sha256=file_sha256(ROOT / "manifest.json"),
                      capture_files=[{"path": str(path), "sha256": file_sha256(path)} for path in
                                     [Path(__file__).resolve(), ROOT / "manifest.json",
                                      *(FIXTURES / path for path in manifest["helper_sources"])]])
        with args.output.open("x", encoding="utf-8") as output:
            output.write(json.dumps(header, ensure_ascii=True, separators=(",", ":")) + "\n")
            output.flush()
            for case in expanded:
                mode = str(case["ansi"]).lower()
                spark.conf.set("spark.sql.ansi.enabled", mode)
                assert spark.conf.get("spark.sql.ansi.enabled") == mode
                record = observe_stages(spark, case)
                assert spark.conf.get("spark.sql.ansi.enabled") == mode
                record["settings"] = {key: spark.conf.get(key) for key in
                                      [*manifest["settings"], "spark.sql.ansi.enabled"]}
                output.write(json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n")
                output.flush()
                print(f"{case['id']} ansi={case['ansi']}", file=sys.stderr, flush=True)
        assert len(args.output.read_bytes().splitlines()) == len(expanded) + 1
        print(json.dumps({"output": str(args.output), "observations": len(expanded),
                          "sha256": file_sha256(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
