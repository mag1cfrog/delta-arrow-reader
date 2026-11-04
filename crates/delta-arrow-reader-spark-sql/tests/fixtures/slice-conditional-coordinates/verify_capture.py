"""Verify conditional SLICE records and optionally compare the frozen v2 oracle."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from capture_spark import PACKAGE, self_check, sha256

STAGES = ["public", "resolved", "analyzed", "optimized", "physical",
          "executed_before_collect", "executed"]


def read_records(path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(records) == 13
    return records[0], records[1:]


def verify(raw_path, frozen_path=None):
    manifest = self_check()
    header, records = read_records(raw_path)
    for key, value in manifest.items():
        assert header[key] == value, key
    assert header["capture_sha256"] == manifest["capture_script_sha256"]
    assert header["manifest_sha256"] == sha256(PACKAGE / "manifest.json")
    assert header["process_timezone"] == "UTC"
    assert header["provider_input_roundtrip_checked"] is True
    assert f"revision={manifest['spark_revision']}\n" in header["spark_build"]
    assert f"version={manifest['spark_version']}\n" in header["spark_build"]
    assert [{"id": r["id"], "ansi": r["ansi"]} for r in records] == manifest["identities"]
    assert len({(r["id"], r["ansi"]) for r in records}) == 12
    definitions = {item["id"]: item for item in manifest["scenarios"]}
    for record in records:
        for key, value in definitions[record["id"]].items():
            assert record[key] == value
        assert record["timezone"] == "UTC"
        stages, plans = record["schema_stages"], record["spark_plans"]
        assert list(plans) == [stage for stage in stages if stage not in ["public", "resolved"]]
        assert all(isinstance(plan, str) and plan for plan in plans.values())
        assert all(schema == stages["public"] for schema in stages.values())
        if record["id"] in ["positive_overflow_invalid_start_else", "nonoverflow_invalid_start_then"]:
            assert "spark_error" not in record
            assert list(stages) == STAGES
            assert record["expected"] == {"schema": stages["public"], "rows": [[["9"]]]}
            assert record["spark_collected_rows"] == [[["9"]]]
        else:
            parameter = "length" if record["id"] == "positive_overflow_invalid_length_then" else "start"
            condition = "INVALID_PARAMETER_VALUE." + parameter.upper()
            provider = record["id"] == "provider_overflow_invalid_start_then"
            assert list(stages) == STAGES[:6 if provider else 3]
            assert record["expected"] == {"condition": condition}
            assert record["spark_error_phase"] == ("collect" if provider else "optimized_schema")
            assert record["spark_error_class"] == "SparkRuntimeException"
            parameters = {"parameter": f"`{parameter}`", "functionName": "`slice`"}
            if parameter == "length":
                parameters["length"] = "-1"
            assert record["spark_error_details"] == {
                "condition": condition, "sql_state": "22023", "message_parameters": parameters,
                "java_causes": [], "metadata_errors": []}
            assert record["spark_error"] and record["spark_error_traceback"]
    if frozen_path is not None:
        assert sha256(frozen_path) == manifest["lineage"]["raw_sha256"]
        frozen_header, frozen_records = read_records(frozen_path)
        for key in ["spark_version", "spark_revision", "timezone", "inputs", "scenarios"]:
            assert header[key] == frozen_header[key], key
        for actual, frozen in zip(records, frozen_records):
            # Plans contain generated IDs; tracebacks contain the capture source path.
            # Both remain complete in each raw file; every recorded outcome stays exact.
            for key in ["id", "sql", "ordered", "ansi", "timezone", "expected", "schema_stages",
                        "spark_collected_rows", "spark_error_phase", "spark_error_class", "spark_error_details"]:
                assert actual.get(key) == frozen.get(key), (actual["id"], actual["ansi"], key)
    return {"verification": "passed", "scenarios": 6, "observations": 12,
            "raw_sha256": sha256(raw_path), "capture_sha256": header["capture_sha256"],
            "manifest_sha256": header["manifest_sha256"], "frozen_v2_compared": frozen_path is not None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("--frozen-reference", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(args.raw, args.frozen_reference)
    if args.output:
        with args.output.open("x") as output:
            output.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
