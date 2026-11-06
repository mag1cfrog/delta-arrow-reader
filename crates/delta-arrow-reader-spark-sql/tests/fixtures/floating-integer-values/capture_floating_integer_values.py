"""Capture #897 floating-to-signed-integer stages; --check imports no Spark."""

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import sys


sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
MANIFEST = ROOT / "manifest.json"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_manifest():
    manifest = json.loads(MANIFEST.read_text())
    fixtures = ROOT.parent
    provenance = manifest["capture_provenance"]
    for key in ["original_capture", "original_manifest", "original_reference"]:
        original = provenance[key]
        assert digest(ROOT / original["path"]) == original["sha256"], original["path"]
    original_manifest = json.loads((ROOT / provenance["original_manifest"]["path"]).read_text())
    for key in ["cases", "inputs", "native", "settings", "original_references", "helper_sources"]:
        assert manifest[key] == original_manifest[key], key
    assert "fixtures_root" not in manifest
    assert manifest["issue"] == 897
    assert manifest["spark_version"] == "4.2.0"
    assert manifest["spark_revision"] == "32f7299601108917fb01920a54e084595b7b3bf8"
    assert manifest["timezone"] == "UTC"
    assert manifest["source_types"] == ["FLOAT", "DOUBLE"]
    assert manifest["target_types"] == ["TINYINT", "SMALLINT", "INT", "BIGINT"]
    cases = manifest["cases"]
    assert len(cases) == manifest["case_count"] == 176
    assert len({case["id"] for case in cases}) == len(cases)
    assert dict(Counter(case["domain"] for case in cases)) == manifest["case_counts"]
    assert all(case["ansi_modes"] == [True, False] for case in cases)
    assert all(not {"expected", "schema_stages"}.intersection(case) for case in cases)
    assert sum(len(case["ansi_modes"]) for case in cases) == manifest["observations"] == 352
    assert manifest["native_comparisons"] == 704
    for path, expected in manifest["helper_sources"].items():
        assert digest(fixtures / path) == expected, path
    for source in manifest["preserved_reference_files"]:
        assert digest(fixtures / source["path"]) == source["sha256"], source["path"]
    originals = 0
    for source in manifest["original_references"]:
        path = fixtures / source["path"]
        assert digest(path) == source["sha256"]
        lines = path.read_bytes().splitlines(keepends=True)
        assert hashlib.sha256(lines[0]).hexdigest() == source["header_sha256"]
        for selected in source["selected_records"]:
            line = lines[selected["line"] - 1]
            assert hashlib.sha256(line).hexdigest() == selected["sha256"]
            recorded = json.loads(line)
            assert (recorded["id"], recorded["ansi"]) == (selected["id"], selected["ansi"])
            case, = [case for case in cases if case["id"] == recorded["id"]]
            assert case["original_fixture"] == source["path"]
            assert case["sql"] == recorded["sql"]
            originals += 1
    assert originals == 14
    lines = (fixtures / "regexp-replace-position-int-spark.jsonl").read_bytes().splitlines(keepends=True)
    assert len(manifest["diagnostic922_preserved_records"]) == 10
    for selected in manifest["diagnostic922_preserved_records"]:
        line = lines[selected["line"] - 1]
        assert hashlib.sha256(line).hexdigest() == selected["sha256"]
        recorded = json.loads(line)
        assert recorded["ansi"] is True and recorded["expected"]["condition"] == "CAST_OVERFLOW"
        assert recorded["expected"]["parameters"]["sourceType"] == selected["source_type"]
    roles = manifest["value_roles"]
    assert len(roles) == 41 and len({role["name"] for role in roles}) == 41
    for role in roles:
        for column, encoding in [("f", "!f"), ("d", "!d")]:
            raw = bytes.fromhex(role[column])
            assert len(raw) == struct.calcsize(encoding)
            assert struct.pack(encoding, struct.unpack(encoding, raw)[0]) == raw
    for target in manifest["target_types"]:
        for source, column in [("FLOAT", "f"), ("DOUBLE", "d")]:
            group = [case for case in cases if case["domain"] == "literal_boundary"
                     and case["source_type"] == source and case["target_type"] == target]
            assert len(group) == 11
            assert {case["discriminator"] for case in group} == {
                "nan", "positive_infinity", "negative_infinity", "positive_max_finite", "negative_max_finite",
                *(target.lower() + "_" + name for name in ["upper_predecessor", "upper_cutoff", "upper_successor", "lower_toward_zero", "lower_cutoff", "lower_away_zero"])}
            for case in group:
                role, = [role for role in roles if role["name"] == case["discriminator"]]
                assert case["source_bits"] == role[column]
    assert len({table["name"] for table in manifest["inputs"]}) == 5
    for table in manifest["inputs"]:
        fields = table["schema"]["fields"]
        assert [field["type"] for field in fields] == ["integer", "float", "double"]
        assert [int(row[0]) for row in table["rows"]] == list(range(len(table["rows"])))
        for row in table["rows"]:
            assert len(row) == len(fields)
            for item, field in zip(row, fields):
                assert item is not None or field["nullable"]
        assert table["native_slice"] == table["name"].endswith("_sliced")
    required, = [table for table in manifest["inputs"] if table["name"] == "floating_integer_required"]
    assert required["rows"] == [[str(i), role["f"], role["d"]] for i, role in enumerate(roles)]
    fitting, = [table for table in manifest["inputs"] if table["name"] == "floating_integer_fitting_sliced"]
    assert fitting["rows"] == required["rows"][:6]
    assert manifest["native"]["batch_sizes"] == [1, 32]
    assert manifest["native"]["target_partitions"] == 2
    for target, width in [("TINYINT", 8), ("SMALLINT", 16), ("INT", 32), ("BIGINT", 64)]:
        for column, encoding in [("f", "!f"), ("d", "!d")]:
            def number(name):
                role, = [role for role in roles if role["name"] == target.lower() + "_" + name]
                return struct.unpack(encoding, bytes.fromhex(role[column]))[0]
            cutoff = float(1 << (width - 1))
            assert number("upper_predecessor") < number("upper_cutoff") == cutoff < number("upper_successor")
            assert number("lower_away_zero") < number("lower_cutoff") == -cutoff < number("lower_toward_zero")
    assert all(math.isfinite(struct.unpack(encoding, bytes.fromhex(role[column]))[0])
               for role in roles[:6] for column, encoding in [("f", "!f"), ("d", "!d")])
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
        assert args.output is None
        assert "pyspark" not in sys.modules
        print(json.dumps({"cases": len(manifest["cases"]), "observations": len(cases),
                          "original_bytes_verified": True, "engines_run": False}))
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

    builder = SparkSession.builder.master("local[1]").appName("dar-floating-integer-values-897")
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
        files = [Path(__file__).resolve(), MANIFEST,
                 *(ROOT / manifest["capture_provenance"][key]["path"] for key in ["original_capture", "original_manifest"]),
                 *(fixtures / name for name in manifest["helper_sources"])]
        header = {key: value for key, value in manifest.items() if key != "cases"}
        header.update(identity, status="Independent Spark capture; completion requires the final process receipt and record count.",
                      registered_inputs=registered,
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
        assert len(args.output.read_bytes().splitlines()) == len(cases) + 1
        print(json.dumps({"output": str(args.output), "observations": len(cases), "sha256": digest(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
