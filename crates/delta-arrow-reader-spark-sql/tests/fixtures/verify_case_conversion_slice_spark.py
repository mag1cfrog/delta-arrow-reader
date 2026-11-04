"""Verify the frozen Spark interaction capture without executing DAR or Spark."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import capture_case_conversion_slice_spark as capture
STAGES = ["public", "resolved", "analyzed", "optimized", "physical",
          "executed_before_collect", "executed"]
PHASE_STAGES = {"sql": [], "public_schema": [], "analyzed_schema": STAGES[:2],
                "optimized_schema": STAGES[:3], "physical_schema": STAGES[:4],
                "executed_schema": STAGES[:5], "collect": STAGES[:6],
                "executed_schema_after_collect": STAGES[:6]}


def verify(raw_path, frozen_path=None):
    manifest = capture.self_check()
    records = [json.loads(line) for line in raw_path.read_text().splitlines()]
    assert len(records) == 7
    header, observations = records[0], records[1:]
    for key, value in manifest.items():
        assert header[key] == value, key
    assert header["capture_sha256"] == manifest["capture_script_sha256"]
    assert header["manifest_sha256"] == capture.sha256(capture.MANIFEST_PATH)
    assert header["process_timezone"] == "UTC"
    assert header["provider_input_roundtrip_checked"] is True
    assert f"revision={manifest['spark_revision']}\n" in header["spark_build"]
    assert f"version={manifest['spark_version']}\n" in header["spark_build"]
    assert [{"id": r["id"], "ansi": r["ansi"]} for r in observations] == manifest["identities"]
    assert len({(r["id"], r["ansi"]) for r in observations}) == 6
    definitions = {case["id"]: case for case in manifest["scenarios"]}
    summary = []
    for record in observations:
        for key, value in definitions[record["id"]].items():
            assert record[key] == value
        assert record["timezone"] == "UTC"
        stages, plans = record["schema_stages"], record["spark_plans"]
        assert list(stages) == STAGES[:len(stages)]
        assert list(plans) == [stage for stage in stages if stage not in ["public", "resolved"]]
        assert all(isinstance(plan, str) and plan for plan in plans.values())
        assert all(schema["type"] == "struct" and isinstance(schema["fields"], list)
                   for schema in stages.values())
        if "spark_error" in record:
            assert list(stages) == PHASE_STAGES[record["spark_error_phase"]]
            assert set(record["expected"]) == {"condition"}
            details = record["spark_error_details"]
            assert set(details) == {"condition", "sql_state", "message_parameters",
                                    "java_causes", "metadata_errors"}
            assert details["condition"] == record["expected"]["condition"]
            assert details["metadata_errors"] == []
            assert record["spark_error"] and record["spark_error_traceback"] and record["spark_error_class"]
            outcome = {"error_phase": record["spark_error_phase"], "error_details": details}
        else:
            assert list(stages) == STAGES
            assert set(record["expected"]) == {"schema", "rows"}
            assert stages["public"] == stages["resolved"] == stages["analyzed"] == record["expected"]["schema"]
            assert record["spark_collected_rows"] == record["expected"]["rows"]
            outcome = {"schema": record["expected"]["schema"], "rows": record["spark_collected_rows"]}
        summary.append({"id": record["id"], "ansi": record["ansi"], **outcome})
    if frozen_path is not None:
        assert capture.sha256(frozen_path) == manifest["lineage"]["raw_sha256"]
        frozen = [json.loads(line) for line in frozen_path.read_text().splitlines()]
        assert len(frozen) == len(records)
        for key in ["spark_version", "spark_revision", "timezone", "inputs", "scenarios"]:
            assert header[key] == frozen[0][key], key
        for actual, original in zip(observations, frozen[1:]):
            # Keep generated plan IDs and source paths in both complete raw files.
            for key in ["id", "sql", "ordered", "ansi", "timezone", "expected", "schema_stages",
                        "spark_collected_rows", "spark_error_phase", "spark_error_class", "spark_error_details"]:
                assert actual.get(key) == original.get(key), (actual["id"], actual["ansi"], key)
    return {"verification": "passed", "observations": 6,
            "capture_sha256": header["capture_sha256"], "manifest_sha256": header["manifest_sha256"],
            "raw_sha256": capture.sha256(raw_path), "frozen_v1_compared": frozen_path is not None,
            "outcomes": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--frozen-reference", type=Path)
    args = parser.parse_args()
    result = verify(args.raw, args.frozen_reference)
    if args.output:
        with args.output.open("x") as output:
            output.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
