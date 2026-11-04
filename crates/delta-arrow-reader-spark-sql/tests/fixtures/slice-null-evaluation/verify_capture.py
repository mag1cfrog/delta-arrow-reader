"""Verify saved #881 Spark evidence with the Python standard library only."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
PACKAGE = Path(__file__).resolve().parent
VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
BASELINE = "085986ca2a53d8e249222ecd4052ea884386a203"
GROUP = "slice-881-null-evaluation-current-v1"
STAGES = ["public", "resolved", "analyzed", "optimized", "physical",
          "executed_before_collect", "executed"]
PHASE_STAGES = {"sql": [], "public_schema": [], "analyzed_schema": STAGES[:2],
                "optimized_schema": STAGES[:3], "physical_schema": STAGES[:4],
                "executed_schema": STAGES[:5], "collect": STAGES[:6],
                "encode_rows": STAGES[:6], "executed_schema_after_collect": STAGES[:6],
                "schema_consistency": STAGES}
SQL_CONDITIONS = {"CAST_INVALID_INPUT", "INVALID_PARAMETER_VALUE.START",
                  "INVALID_PARAMETER_VALUE.LENGTH"}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_schema(data_type):
    if isinstance(data_type, str):
        assert data_type in ("integer", "string", "boolean"), data_type
    elif data_type["type"] == "array":
        assert set(data_type) == {"type", "elementType", "containsNull"}
        assert isinstance(data_type["containsNull"], bool)
        check_schema(data_type["elementType"])
    else:
        assert set(data_type) == {"type", "fields"} and data_type["type"] == "struct"
        assert isinstance(data_type["fields"], list)
        for field in data_type["fields"]:
            assert set(field) == {"name", "type", "nullable", "metadata"}
            assert isinstance(field["name"], str)
            assert isinstance(field["nullable"], bool)
            assert isinstance(field["metadata"], dict)
            check_schema(field["type"])


def check_value(value, data_type, nullable=False):
    if value is None:
        assert nullable
    elif isinstance(data_type, dict) and data_type["type"] == "struct":
        assert isinstance(value, list) and len(value) == len(data_type["fields"])
        for item, field in zip(value, data_type["fields"]):
            check_value(item, field["type"], field["nullable"])
    elif isinstance(data_type, dict) and data_type["type"] == "array":
        assert isinstance(value, list)
        for item in value:
            check_value(item, data_type["elementType"], data_type["containsNull"])
    elif data_type == "integer":
        assert isinstance(value, str)
        assert str(int(value)) == value and -(2**31) <= int(value) < 2**31
    elif data_type == "boolean":
        assert value in ("true", "false") and isinstance(value, str)
    else:
        assert data_type == "string" and isinstance(value, str)


def verify_records(manifest, records):
    assert len(records) == manifest["observation_count"] + 1
    header, observations = records[0], records[1:]
    assert header["spark_version"] == manifest["spark_version"] == VERSION
    assert header["spark_revision"] == manifest["spark_revision"] == REVISION
    assert f"revision={REVISION}\n" in header["spark_build"]
    assert f"version={VERSION}\n" in header["spark_build"]
    assert header["source_commit"] == manifest["source_commit"] == BASELINE
    assert header["timezone"] == manifest["timezone"] == header["process_timezone"] == "UTC"
    assert header["group"] == manifest["group"] == GROUP
    assert header["observations"] == manifest["observation_count"]
    assert header["inputs"] == manifest["inputs"]
    assert header["input_encoding"] == manifest["input_encoding"]
    assert header["provider_input_roundtrip_checked"] is True
    assert manifest["ansi_modes"] == [True, False]
    scenarios = manifest["scenarios"]
    assert len(scenarios) == manifest["scenario_count"]
    assert len({case["id"] for case in scenarios}) == len(scenarios)
    assert len({case["sql"] for case in scenarios}) == len(scenarios)
    identities = [{"id": case["id"], "ansi": ansi} for ansi in [True, False] for case in scenarios]
    assert manifest["identities"] == identities
    assert [{"id": record["id"], "ansi": record["ansi"]} for record in observations] == identities
    assert len({(record["id"], record["ansi"]) for record in observations}) == len(identities)
    by_id = {case["id"]: case for case in scenarios}
    for provider in header["inputs"]:
        check_schema(provider["schema"])
        keys = [int(row[0]) for row in provider["rows"]]
        assert keys == sorted(set(keys))
        for row in provider["rows"]:
            check_value(row, provider["schema"])
    summary, unknown = [], []
    for record in observations:
        assert {key: record[key] for key in by_id[record["id"]]} == by_id[record["id"]]
        assert isinstance(record["ansi"], bool) and record["timezone"] == "UTC"
        stages, plans = record["schema_stages"], record["spark_plans"]
        assert list(stages) == STAGES[:len(stages)], (record["id"], list(stages))
        for schema in stages.values():
            check_schema(schema)
            assert schema["type"] == "struct"
        assert list(plans) == [name for name in stages if name not in ("public", "resolved")]
        assert all(isinstance(plan, str) and plan for plan in plans.values())
        item = {"id": record["id"], "ansi": record["ansi"], "stages": list(stages)}
        if "spark_error" in record:
            assert set(record["expected"]) == {"condition"}
            assert record["spark_error_phase"] in PHASE_STAGES
            assert list(stages) == PHASE_STAGES[record["spark_error_phase"]]
            details = record["spark_error_details"]
            assert set(details) == {"condition", "sql_state", "message_parameters", "condition_source",
                                    "java_causes", "metadata_errors"}
            assert details["condition"] is None or isinstance(details["condition"], str)
            assert details["sql_state"] is None or isinstance(details["sql_state"], str)
            parameters = details["message_parameters"]
            assert parameters is None or (isinstance(parameters, dict) and all(
                isinstance(key, str) and isinstance(value, str) for key, value in parameters.items()))
            assert isinstance(details["java_causes"], list)
            assert isinstance(details["metadata_errors"], list)
            assert record["expected"]["condition"] == details["condition"]
            assert record["expected_error"] == {"phase": record["spark_error_phase"], **{
                key: details[key] for key in ("condition", "sql_state", "message_parameters")}}
            assert isinstance(record["spark_error"], str)
            assert record["spark_error_class"] and record["spark_error_traceback"]
            recognized = details["condition"] in SQL_CONDITIONS and record["spark_error_phase"] not in (
                "encode_rows", "schema_consistency")
            assert record["query_failure_kind"] == ("sql_error" if recognized else "unknown_failure")
            if not recognized or details["metadata_errors"]:
                unknown.append({**item, "failure_kind": record["query_failure_kind"],
                                "error": record["spark_error"], "details": details})
            else:
                assert details["sql_state"] and isinstance(parameters, dict)
                assert details["condition_source"] is not None
            item.update(outcome="error", **record["expected_error"])
        else:
            assert list(stages) == STAGES
            assert stages["public"] == stages["resolved"] == stages["analyzed"] == record["expected"]["schema"]
            assert set(record["expected"]) == {"schema", "rows"}
            rows = record["spark_collected_rows"]
            assert isinstance(rows, list)
            for row in rows:
                check_value(row, stages["public"])
            expected_rows = list(rows)
            if not record["ordered"]:
                expected_rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
            assert record["expected"]["rows"] == expected_rows
            item.update(outcome="success", rows=len(rows))
        summary.append(item)
    return summary, unknown


def self_check():
    schema = {"type": "struct", "fields": [{"name": "r", "type": {
        "type": "array", "elementType": "integer", "containsNull": True},
        "nullable": True, "metadata": {"source": "self-check"}}]}
    check_schema(schema)
    check_value([["1", None, "-2"]], schema)
    check_value([None], schema)
    for value in ([["01"]], [["2147483648"]], [[True]], [], [["bad"]]):
        try:
            check_value(value, schema)
        except (AssertionError, ValueError):
            pass
        else:
            raise AssertionError("invalid result accepted: {!r}".format(value))
    bad = copy.deepcopy(schema)
    del bad["fields"][0]["metadata"]
    try:
        check_schema(bad)
    except AssertionError:
        pass
    else:
        raise AssertionError("incomplete schema accepted")
    case = {"id": "self_check", "sql": "SELECT synthetic", "ordered": False,
            "family": "synthetic", "consumer": "direct"}
    manifest = {"spark_version": VERSION, "spark_revision": REVISION,
                "source_commit": BASELINE, "timezone": "UTC", "group": GROUP,
                "observation_count": 2, "scenario_count": 1, "inputs": [],
                "input_encoding": "synthetic", "ansi_modes": [True, False],
                "scenarios": [case], "identities": [{"id": case["id"], "ansi": ansi}
                                                   for ansi in [True, False]]}
    header = {key: manifest[key] for key in ("spark_version", "spark_revision", "source_commit",
              "timezone", "group", "inputs", "input_encoding")}
    header.update(spark_build=f"version={VERSION}\nrevision={REVISION}\n",
                  observations=2, process_timezone="UTC", provider_input_roundtrip_checked=True)
    success = {**case, "ansi": True, "timezone": "UTC", "schema_stages": dict.fromkeys(STAGES, schema),
               "spark_plans": dict.fromkeys(STAGES[2:], "synthetic plan"),
               "expected": {"schema": schema, "rows": [[None]]}, "spark_collected_rows": [[None]]}
    details = {"condition": "CAST_INVALID_INPUT", "sql_state": "22018", "message_parameters": {},
               "condition_source": "python", "java_causes": [], "metadata_errors": []}
    failure = {**case, "ansi": False, "timezone": "UTC", "schema_stages": {}, "spark_plans": {},
               "expected": {"condition": details["condition"]},
               "expected_error": {"phase": "sql", **{key: details[key] for key in (
                   "condition", "sql_state", "message_parameters")}},
               "spark_error_phase": "sql", "spark_error": "synthetic error",
               "spark_error_details": details, "spark_error_class": "SyntheticError",
               "spark_error_traceback": "synthetic traceback", "query_failure_kind": "sql_error"}
    records = [header, success, failure]
    summary, unknown = verify_records(manifest, records)
    assert [item["outcome"] for item in summary] == ["success", "error"] and unknown == []
    incomplete = copy.deepcopy(records)
    incomplete.pop()
    try:
        verify_records(manifest, incomplete)
    except AssertionError:
        pass
    else:
        raise AssertionError("missing observation accepted")
    unexpected = copy.deepcopy(records)
    unexpected[2]["spark_error_details"]["condition"] = "UNEXPECTED"
    unexpected[2]["expected"]["condition"] = "UNEXPECTED"
    unexpected[2]["expected_error"]["condition"] = "UNEXPECTED"
    unexpected[2]["query_failure_kind"] = "unknown_failure"
    assert len(verify_records(manifest, unexpected)[1]) == 1
    assert "pyspark" not in sys.modules


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", nargs="?", type=Path, default=PACKAGE / "spark-reference.jsonl")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    self_check()
    if args.self_check:
        print(json.dumps({"verifier_self_check": "passed"}))
        return
    manifest_path = PACKAGE / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["capture_script_sha256"] == sha256(PACKAGE / "capture_spark.py")
    assert manifest["verifier_script_sha256"] == sha256(Path(__file__))
    records = [json.loads(line) for line in args.capture.read_text().splitlines()]
    header = records[0]
    assert header["capture_sha256"] == manifest["capture_script_sha256"]
    assert header["manifest_sha256"] == sha256(manifest_path)
    summary, unknown = verify_records(manifest, records)
    result = {"verification": "requires_review" if unknown else "passed",
              "scenarios": manifest["scenario_count"], "observations": len(summary),
              "spark_version": VERSION, "spark_revision": REVISION,
              "capture_sha256": header["capture_sha256"], "manifest_sha256": header["manifest_sha256"],
              "raw_sha256": sha256(args.capture),
              "provider_roundtrip": [{"name": provider["name"], "rows": len(provider["rows"])}
                                     for provider in header["inputs"]],
              "successes": sum(item["outcome"] == "success" for item in summary),
              "errors": sum(item["outcome"] == "error" for item in summary),
              "unknown_failures": unknown, "inventory": summary}
    print(json.dumps(result, indent=2))
    if unknown:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
