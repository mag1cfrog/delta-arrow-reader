"""Verify the independently captured #894 observations without Spark or DAR."""

import argparse
import hashlib
import json
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent
STAGES = ["public", "resolved", "analyzed", "optimized", "physical",
          "executed_before_collect", "executed"]
PHASE_STAGES = {"sql": [], "public_schema": [], "analyzed_schema": STAGES[:2],
                "optimized_schema": STAGES[:3], "physical_schema": STAGES[:4],
                "executed_schema": STAGES[:5], "collect": STAGES[:6],
                "executed_schema_after_collect": STAGES[:6]}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(raw_path):
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    records = [json.loads(line) for line in raw_path.read_text().splitlines()]
    assert len(records) == manifest["observation_count"] + 1 == 11
    header, observations = records[0], records[1:]
    assert header["spark_version"] == manifest["spark_version"] == "4.2.0"
    assert header["spark_revision"] == manifest["spark_revision"] == (
        "32f7299601108917fb01920a54e084595b7b3bf8")
    assert f"revision={header['spark_revision']}\n" in header["spark_build"]
    assert "version=4.2.0\n" in header["spark_build"]
    assert header["timezone"] == manifest["timezone"] == header["process_timezone"] == "UTC"
    assert header["group"] == manifest["group"] == "slice-894-coordinates-current-v1"
    assert header["source_commit"] == manifest["source_commit"] == (
        "085986ca2a53d8e249222ecd4052ea884386a203")
    assert header["source_reference_sha256"] == manifest["source_reference_sha256"] == (
        "d3d5001f84003b5756483f8f2699100746b1fd105ffcf92d8a974c219abafeb0")
    assert header["capture_sha256"] == manifest["capture_script_sha256"] == sha256(PACKAGE / "capture_spark.py")
    assert header["manifest_sha256"] == sha256(PACKAGE / "manifest.json")
    assert header["references"] == manifest["references"]
    for relative, expected_hash in manifest["references"].items():
        assert sha256(PACKAGE / relative) == expected_hash
    assert header["inputs"] == manifest["inputs"]
    assert header["input_encoding"] == manifest["input_encoding"]
    assert header["provider_input_roundtrip_checked"] is True
    definitions = {case["id"]: case for case in manifest["scenarios"]}
    assert len(definitions) == manifest["scenario_count"] == 5
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                      for ansi in [True, False] for case in manifest["scenarios"]]
    assert [{"id": case["id"], "ansi": case["ansi"]} for case in observations] == manifest["identities"]
    summary = []
    for case in observations:
        for key, value in definitions[case["id"]].items():
            assert case[key] == value, (case["id"], key)
        assert case["timezone"] == "UTC"
        stages, plans = case["schema_stages"], case["spark_plans"]
        assert list(stages) == STAGES[:len(stages)]
        assert list(plans) == [name for name in stages if name not in ("public", "resolved")]
        assert all(isinstance(plan, str) and plan for plan in plans.values())
        for schema in stages.values():
            assert schema["type"] == "struct" and isinstance(schema["fields"], list)
        if "spark_error" in case:
            assert list(stages) == PHASE_STAGES[case["spark_error_phase"]]
            assert set(case["expected"]) == {"condition"}
            details = case["spark_error_details"]
            assert set(details) == {"condition", "sql_state", "message_parameters", "java_causes", "metadata_errors"}
            assert details["metadata_errors"] == []
            assert details["condition"] == case["expected"]["condition"]
            assert case["spark_error"] and case["spark_error_traceback"] and case["spark_error_class"]
            outcome = {"error_phase": case["spark_error_phase"], "error_details": details}
        else:
            assert list(stages) == STAGES
            assert set(case["expected"]) == {"schema", "rows"}
            assert stages["public"] == stages["resolved"] == stages["analyzed"] == case["expected"]["schema"]
            rows = case["spark_collected_rows"]
            expected = rows if case["ordered"] else sorted(
                rows, key=lambda row: json.dumps(row, separators=(",", ":")))
            assert expected == case["expected"]["rows"]
            outcome = {"rows": rows, "schema": case["expected"]["schema"]}
        summary.append({"id": case["id"], "ansi": case["ansi"], **outcome})
    assert len({(case["id"], case["ansi"]) for case in observations}) == 10
    return {"verification": "passed", "scenarios": 5, "observations": 10,
            "raw_sha256": sha256(raw_path), "capture_sha256": header["capture_sha256"],
            "manifest_sha256": header["manifest_sha256"], "observations_summary": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(args.raw)
    if args.output:
        with args.output.open("x") as output:
            output.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items()
                      if key != "observations_summary"}, indent=2))


if __name__ == "__main__":
    main()
