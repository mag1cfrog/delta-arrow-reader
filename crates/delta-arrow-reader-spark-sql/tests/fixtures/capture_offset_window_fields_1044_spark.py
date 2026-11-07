"""Record independent Spark Fields and values for LAG/LEAD declarations.

The request supplies SQL and typed providers. Expected results come only from
Spark; existing native and reference outcomes are not read.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True

from capture_date_range_747_spark import (
    REVISION, VERSION, self_check as check_capture_protocol,
)
from capture_spark_requests import capture_requested_queries

HERE = Path(__file__).resolve().parent
REQUEST = HERE / "offset-window-fields-1044-cases.json"
BASE = "5b82dec9131c3fc85b7e57e27429175ffd253a66"
PROVIDERS = [
    "offset_scalar", "empty_offset_scalar", "null_offset_scalar",
    "offset_nested", "empty_offset_nested", "null_offset_nested",
]
FAMILY_COUNTS = {
    "scalar_defaults": 12, "signed_zero_offsets": 12,
    "typed_nested_fields": 12, "sql_array_constructors": 8,
    "outer_consumers": 16, "offset_frame_admission": 12,
    "optimized_nullability": 4,
    "column_defaults": 4, "lag_defaults_without_frame": 6, "literal_null_payloads": 8,
}
HELPERS = {
    "capture_spark_requests.py": "1a813099731f8dc6bba26bb71bcc2fd56cd0b4e9c7b430f27a0a86950d35ef17",
    "capture_date_range_747_spark.py": "b8ec7266c62a60a60262d8b0a988f712d8995c58c6a8410e105c13ac0afa3922",
    "capture_floating_group_keys_spark.py": "9db227451bea43d54e8dcd30d24501e78b18e752f22d971d285f863f058fd934",
    "capture_window_name_stages_spark.py": "52a3efa953716f7c1770952990beb655de1414561f278ef65c25599d29739633",
    "recapture_string_binary_spark.py": "3fa0da4a0a44fb7560097285442819f0e722af2c3791ce2622e02e64712d03b8",
}


def validate_schema(data_type):
    if isinstance(data_type, dict):
        assert data_type["type"] == "struct"
        fields = data_type["fields"]
        assert fields and len(fields) == len({field["name"] for field in fields})
        for field in fields:
            assert set(field) == {"name", "type", "nullable", "metadata"}
            assert type(field["nullable"]) is bool and isinstance(field["metadata"], dict)
            assert all(isinstance(key, str) and isinstance(value, str)
                       for key, value in field["metadata"].items()), (
                "provider Field metadata values must be strings", field["name"])
            validate_schema(field["type"])
    else:
        assert data_type in ["integer", "boolean"]


def validate_value(value, data_type, nullable):
    if value is None:
        assert nullable, "NULL supplied for a required provider Field"
    elif isinstance(data_type, dict):
        assert data_type["type"] == "struct"
        fields = data_type["fields"]
        assert isinstance(value, list) and len(value) == len(fields)
        for item, field in zip(value, fields):
            assert set(field) == {"name", "type", "nullable", "metadata"}
            assert type(field["nullable"]) is bool and isinstance(field["metadata"], dict)
            validate_value(item, field["type"], field["nullable"])
    elif data_type == "integer":
        assert isinstance(value, str) and str(int(value)) == value
        assert -(2**31) <= int(value) < 2**31
    else:
        assert data_type == "boolean" and value in ["True", "False"]


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["source_revision"] == 2 and request["issue"] == 1044
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["accepted_base"] == BASE
    assert request["python_version"] == "3.12" and request["java_specification_version"] == "21"
    assert request["outcome_state"].startswith("UNKNOWN:")
    assert request["sql_source_count"] == 47 and request["expected_record_count"] == 94
    assert request["retained_reference_count"] == 6 and request["retained_native_context_count"] == 12
    assert request["family_record_counts"] == FAMILY_COUNTS
    assert request["native_batch_sizes"] == [1, 32]
    assert request["native_provider_partitions"] == request["native_target_partitions"] == 2
    assert request["spark_provider_partitions_expected"] == 1 and request["spark_master"] == "local[1]"
    assert request["retained_reference_source"]["recapture_requested"] is False
    for name, expected_hash in HELPERS.items():
        with (HERE / name).open("rb") as source:
            assert hashlib.file_digest(source, "sha256").hexdigest() == expected_hash, name
    corpus, = request["corpora"]
    assert corpus["corpus"] == "offset-window-fields-1044-supplement"
    assert corpus["source"]["reads_expected_outputs"] is False
    metadata = corpus["metadata"]
    assert set(metadata) == {"spark_version", "timezone", "inputs"}
    assert metadata["spark_version"] == VERSION and metadata["timezone"] == "UTC"
    assert [provider["name"] for provider in metadata["inputs"]] == PROVIDERS
    assert [len(provider["rows"]) for provider in metadata["inputs"]] == [8, 0, 3, 4, 0, 3]
    for provider in metadata["inputs"]:
        assert set(provider) == {"name", "schema", "rows"}
        validate_schema(provider["schema"])
        for row in provider["rows"]:
            validate_value(row, provider["schema"], False)
    for start in [0, 3]:
        schema = metadata["inputs"][start]["schema"]
        assert all(provider["schema"] == schema for provider in metadata["inputs"][start:start + 3])
        assert [field["nullable"] for field in schema["fields"]] == (
            [False, False, False, True, False] if start == 0 else [False, False, False, True])
    cases = corpus["cases"]
    assert len(cases) == len({(case["corpus"], case["id"], case["ansi"]) for case in cases}) == 94
    assert len({case["sql"] for case in cases}) == 47
    assert Counter(case["coverage"]["family"] for case in cases) == FAMILY_COUNTS
    for case in cases:
        assert set(case) == {"corpus", "id", "ansi", "sql", "ordered", "ownership", "coverage"}
        assert case["corpus"] == corpus["corpus"] and case["ownership"] == "required_offset_window_fields"
        assert isinstance(case["sql"], str) and case["sql"]
        assert type(case["ansi"]) is bool and case["ordered"] is True
        assert case["coverage"]["provider"] in PROVIDERS
        assert set(case["coverage"]["functions"]) <= {"LAG", "LEAD"}
        peers = [other for other in cases if other["id"] == case["id"]]
        assert len(peers) == 2 and {other["ansi"] for other in peers} == {True, False}
        assert peers[0] | {"ansi": case["ansi"]} == peers[1] | {"ansi": case["ansi"]}
    changes = request["source_changes"]
    assert changes["original_case_records_preserved"] == 80 and changes["original_literal_sql_changed"] == 0
    assert changes["added_sql_definitions"] == 7 and changes["added_case_records"] == 14
    assert changes["original_lag_frame_cases_retained"] is True
    assert changes["original_six_historical_references_unchanged"] is True
    assert changes["no_expected_results_edited_or_supplied"] is True
    assert changes["separate_addendum_capture_requested"] is False
    assert len(changes["metadata_value_changes"]) == 12
    assert all(isinstance(change["new_value"], str) for change in changes["metadata_value_changes"])
    removed = (",LAG(r,2,-99) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING "
               "AND 2 PRECEDING) AS required_exact_frame")
    derivations = changes["lag_defaults_derivation"]
    assert len(derivations) == 3
    for derivation in derivations:
        assert derivation["removed_expression"] == removed
        assert derivation["original_sql"].count(removed) == 1
        original = [case for case in cases[:80] if case["id"] == derivation["original_id"]]
        unmasked = [case for case in cases[80:] if case["id"] == derivation["addendum_id"]]
        assert len(original) == len(unmasked) == 2
        assert all(case["sql"] == derivation["original_sql"] for case in original)
        assert all(case["sql"] == derivation["original_sql"].replace(removed, "")
                   for case in unmasked)
    expected_null_sql = {}
    for function in ["LAG", "LEAD"]:
        for provider in PROVIDERS[:2]:
            over = "OVER (ORDER BY id)"
            expected_null_sql[f"{function.lower()}_{provider}_literal_null_payloads"] = (
                f"SELECT id,{function}(CAST(NULL AS INT),2,-99) {over} AS typed_required,"
                f"{function}(CAST(NULL AS INT),2,CAST(NULL AS INT)) {over} AS typed_null,"
                f"{function}(CAST(NULL AS INT),2) {over} AS typed_omitted,"
                f"{function}(NULL,2,-99) {over} AS untyped_required,"
                f"{function}(NULL,2,NULL) {over} AS untyped_null,"
                f"{function}(NULL,2) {over} AS untyped_omitted FROM {provider} ORDER BY id")
    null_cases = [case for case in cases[80:] if case["coverage"]["family"] == "literal_null_payloads"]
    assert len(null_cases) == 8 and {case["id"] for case in null_cases} == set(expected_null_sql)
    assert all(case["sql"] == expected_null_sql[case["id"]] for case in null_cases)
    return request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    assert sys.version_info[:2] == (3, 12), "use Python 3.12"
    request = load_capture_requests()
    if args.self_check:
        assert args.output is None and "pyspark" not in sys.modules
        check_capture_protocol(request)
        return
    capture_requested_queries(
        request, args.output, [Path(__file__).resolve(), REQUEST], [REQUEST],
    )


if __name__ == "__main__":
    main()
