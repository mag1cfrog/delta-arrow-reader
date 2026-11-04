"""Independently verify the bounded Spark capture using only saved inputs and SQL."""

import hashlib
import json
from pathlib import Path
import re

PACKAGE = Path(__file__).resolve().parent
VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
COMMIT = "85e36cf0d0e887cb186e36d862bd8cbf782fce00"
STAGES = ["public", "resolved", "analyzed", "optimized", "physical",
          "executed_before_collect", "executed"]
PHASE_STAGES = {"sql": [], "public_schema": [], "analyzed_schema": STAGES[:2],
                "optimized_schema": STAGES[:3], "physical_schema": STAGES[:4],
                "executed_schema": STAGES[:5], "collect": STAGES[:6],
                "executed_schema_after_collect": STAGES[:6]}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    records = [json.loads(line) for line in (PACKAGE / "spark-reference.jsonl").read_text().splitlines()]
    assert len(records) == 25
    header, observations = records[0], records[1:]
    assert header["spark_version"] == manifest["spark_version"] == VERSION
    assert header["spark_revision"] == manifest["spark_revision"] == REVISION
    assert f"revision={REVISION}\n" in header["spark_build"]
    assert f"version={VERSION}\n" in header["spark_build"]
    assert header["source_commit"] == manifest["source_commit"] == COMMIT
    assert header["timezone"] == manifest["timezone"] == header["process_timezone"] == "UTC"
    assert header["group"] == manifest["group"] == "slice-880-reachability-current-v1"
    assert header["capture_sha256"] == manifest["capture_script_sha256"] == sha256(PACKAGE / "capture_spark.py")
    assert header["manifest_sha256"] == sha256(PACKAGE / "manifest.json")
    assert header["references"] == manifest["references"]
    for relative, expected_hash in header["references"].items():
        assert sha256(PACKAGE / relative) == expected_hash
    input_header = json.loads((PACKAGE / "references" / "provider-header.json").read_text())
    assert header["inputs"] == manifest["inputs"] == input_header["inputs"]
    assert header["input_encoding"] == manifest["input_encoding"] == input_header["input_encoding"]
    assert header["provider_input_roundtrip_checked"] is True
    source_cases = re.findall(r'\(\s*"([a-z_]+)",\s*"(SELECT [^"\n]+)",\s*\)',
                              (PACKAGE / "references" / "scenarios.rs").read_text())
    assert len(source_cases) == manifest["scenario_count"] == 12
    assert [(case["id"], case["sql"]) for case in manifest["scenarios"]] == source_cases
    identities = [{"id": name, "ansi": ansi} for ansi in [True, False] for name, _ in source_cases]
    assert identities == manifest["identities"]
    assert [{"id": record["id"], "ansi": record["ansi"]} for record in observations] == identities
    assert len({(record["id"], record["ansi"]) for record in observations}) == 24
    assert header["observations"] == manifest["observation_count"] == 24
    sql_by_id = dict(source_cases)
    summary = []
    for record in observations:
        assert record["timezone"] == "UTC"
        assert record["sql"] == sql_by_id[record["id"]]
        assert record["ordered"] is False
        stages, plans = record["schema_stages"], record["spark_plans"]
        assert list(stages) == STAGES[:len(stages)], (record["id"], record["ansi"], list(stages))
        for name, schema in stages.items():
            assert schema["type"] == "struct" and isinstance(schema["fields"], list), name
        assert list(plans) == [name for name in stages if name not in ("public", "resolved")]
        assert all(isinstance(plan, str) and plan for plan in plans.values())
        if "spark_error" in record:
            assert set(record["expected"]) == {"condition"}
            details = record["spark_error_details"]
            assert record["expected"]["condition"] == details["condition"]
            assert set(details) == {"condition", "sql_state", "message_parameters", "java_causes", "metadata_errors"}
            assert details["metadata_errors"] == [], details["metadata_errors"]
            assert record["spark_error"] and record["spark_error_traceback"] and record["spark_error_class"]
            assert list(stages) == PHASE_STAGES[record["spark_error_phase"]], record["id"]
            outcome = {"condition": details["condition"], "phase": record["spark_error_phase"],
                       "error_class": record["spark_error_class"], "sql_state": details["sql_state"],
                       "message_parameters": details["message_parameters"]}
        else:
            assert list(stages) == STAGES
            assert stages["public"] == stages["resolved"] == stages["analyzed"] == record["expected"]["schema"]
            rows = record["spark_collected_rows"]
            assert sorted(rows, key=lambda row: json.dumps(row, separators=(",", ":"))) == record["expected"]["rows"]
            outcome = {"schema": record["expected"]["schema"], "rows": rows}
        summary.append({"id": record["id"], "ansi": record["ansi"], "stages": list(stages), **outcome})
    result = {"verification": "passed", "observations": 24, "scenarios": 12,
              "spark_version": VERSION, "spark_revision": REVISION,
              "capture_sha256": header["capture_sha256"], "manifest_sha256": header["manifest_sha256"],
              "raw_sha256": sha256(PACKAGE / "spark-reference.jsonl"),
              "provider_roundtrip": [{"name": provider["name"], "rows": len(provider["rows"])}
                                     for provider in header["inputs"]],
              "observed_successes": sum("rows" in record for record in summary),
              "observed_errors": sum("condition" in record for record in summary),
              "observations_summary": summary}
    output = PACKAGE / "verification.json"
    assert not output.exists(), output
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "observations_summary"}, indent=2))


if __name__ == "__main__":
    main()
