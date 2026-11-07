"""Record Spark LAG/LEAD results when a default needs conversion to the payload.

Valid, malformed and overflowing defaults are isolated in separate queries.
The request supplies inputs without opening existing native or reference results.
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
REQUEST = HERE / "offset-window-default-coercion-1044-cases.json"
PROVIDERS = [
    "offset_scalar", "empty_offset_scalar", "null_offset_scalar",
    "offset_nested", "empty_offset_nested", "null_offset_nested",
]
FAMILY_COUNTS = {
    "required_integer_valid_string": 8,
    "required_integer_invalid_string": 4,
    "typed_integer_null_valid_string": 4,
    "string_to_double": 12, "string_to_decimal": 12,
    "string_to_date": 12, "string_to_timestamp_utc": 12,
    "fractional_double_to_integer": 8, "numeric_to_decimal_boundary": 12,
    "typed_double_null_valid_string": 4, "untyped_null_valid_string": 4,
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
    assert request["python_version"] == "3.12" and request["java_specification_version"] == "21"
    assert request["outcome_state"].startswith("UNKNOWN:")
    assert request["sql_source_count"] == 46 and request["expected_record_count"] == 92
    assert request["retained_reference_count"] == 0 and request["family_record_counts"] == FAMILY_COUNTS
    assert request["native_batch_sizes"] == [1, 32]
    assert request["native_provider_partitions"] == request["native_target_partitions"] == 2
    assert request["spark_provider_partitions_expected"] == 1 and request["spark_master"] == "local[1]"
    assert request["original_provider_source"]["sha256"] == (
        "42c2f1e601bd081d8ff23db96815e3faffc67283829b353d1a44a009f307d9a8")
    assert request["original_provider_source"]["provider_names"] == PROVIDERS
    assert request["isolation"] == {
        "one_window_expression_per_query": True, "valid_invalid_defaults_separate": True,
        "typed_null_payload_separate": True, "untyped_null_payloads": 4,
        "existing94_modified_or_recaptured": False,
        "typed_and_untyped_null_payloads_in_separate_queries": True,
    }
    for name, expected_hash in HELPERS.items():
        with (HERE / name).open("rb") as helper:
            assert hashlib.file_digest(helper, "sha256").hexdigest() == expected_hash, name
    corpus, = request["corpora"]
    assert corpus["corpus"] == "offset-window-default-coercion-1044"
    assert corpus["source"]["reads_expected_outputs"] is False
    metadata = corpus["metadata"]
    assert metadata["spark_version"] == VERSION and metadata["timezone"] == "UTC"
    assert [provider["name"] for provider in metadata["inputs"]] == PROVIDERS
    assert [len(provider["rows"]) for provider in metadata["inputs"]] == [8, 0, 3, 4, 0, 3]
    for provider in metadata["inputs"]:
        validate_schema(provider["schema"])
        for row in provider["rows"]:
            validate_value(row, provider["schema"], False)
    assert metadata["inputs"][0]["schema"]["fields"][2] == {
        "name": "r", "type": "integer", "nullable": False,
        "metadata": {"source": "r", "role": "required-payload"},
    }
    cases = corpus["cases"]
    assert len(cases) == len({(case["id"], case["ansi"]) for case in cases}) == 92
    assert len({case["sql"] for case in cases}) == 46
    assert Counter(case["coverage"]["family"] for case in cases) == FAMILY_COUNTS
    expected_sql = {}
    for function in ["LAG", "LEAD"]:
        for provider in PROVIDERS[:2]:
            expected_sql[f"{function.lower()}_{provider}_valid_string_default"] = (
                f"SELECT id,{function}(r,1,'99') OVER (ORDER BY id) AS shifted "
                f"FROM {provider} ORDER BY id")
        expected_sql[f"{function.lower()}_offset_scalar_invalid_string_default"] = (
            f"SELECT id,{function}(r,1,'bad') OVER (ORDER BY id) AS shifted "
            "FROM offset_scalar ORDER BY id")
        expected_sql[f"{function.lower()}_offset_scalar_typed_null_valid_string_default"] = (
            f"SELECT id,{function}(CAST(NULL AS INT),1,'99') OVER (ORDER BY id) AS shifted "
            "FROM offset_scalar ORDER BY id")
    base_sql = dict(expected_sql)
    prior = request["prior_controls"]
    assert prior["sha256"] == "a015d4036cb66de28b98064701e2cc156892ab95a58255d44dd06325e9c4dd79"
    assert prior["case_records_copied_unchanged"] == 16 and prior["sql_definitions_copied_unchanged"] == 8
    assert prior["added_case_records"] == 76 and prior["added_sql_definitions"] == 38
    assert prior["prior_capture_outputs_opened"] is False
    assert {case["id"] for case in cases[:16]} == set(base_sql)
    assert all(case["sql"] == base_sql[case["id"]] for case in cases[:16])
    for function in ["LAG", "LEAD"]:
        for kind, payload, valid, invalid in [
            ("double", "CAST(r AS DOUBLE)", "'99.5'", "'bad'"),
            ("decimal", "CAST(r AS DECIMAL(12,2))", "'99.125'", "'bad'"),
            ("date", "DATE '2020-01-02'", "'2020-02-29'", "'2021-02-29'"),
            ("timestamp_utc", "TIMESTAMP '2020-01-02 03:04:05.123456'",
             "'2020-02-29 12:34:56.654321'", "'2021-02-29 12:34:56'"),
        ]:
            for provider in PROVIDERS[:2]:
                expected_sql[f"{function.lower()}_{kind}_{provider}_valid_string_default"] = (
                    f"SELECT id,{function}({payload},1,{valid}) OVER (ORDER BY id) AS shifted "
                    f"FROM {provider} ORDER BY id")
            expected_sql[f"{function.lower()}_{kind}_offset_scalar_invalid_string_default"] = (
                f"SELECT id,{function}({payload},1,{invalid}) OVER (ORDER BY id) AS shifted "
                "FROM offset_scalar ORDER BY id")
        for provider in PROVIDERS[:2]:
            expected_sql[f"{function.lower()}_integer_{provider}_fractional_double_default"] = (
                f"SELECT id,{function}(r,1,CAST(-99.75 AS DOUBLE)) OVER (ORDER BY id) AS shifted "
                f"FROM {provider} ORDER BY id")
            expected_sql[f"{function.lower()}_decimal_{provider}_fitting_numeric_default"] = (
                f"SELECT id,{function}(CAST(r AS DECIMAL(12,2)),1,CAST(9999999999 AS BIGINT)) "
                f"OVER (ORDER BY id) AS shifted FROM {provider} ORDER BY id")
        expected_sql[f"{function.lower()}_decimal_offset_scalar_overflow_numeric_default"] = (
            f"SELECT id,{function}(CAST(r AS DECIMAL(12,2)),1,CAST(10000000000 AS BIGINT)) "
            "OVER (ORDER BY id) AS shifted FROM offset_scalar ORDER BY id")
        expected_sql[f"{function.lower()}_offset_scalar_typed_double_null_valid_string_default"] = (
            f"SELECT id,{function}(CAST(NULL AS DOUBLE),1,'99.5') OVER (ORDER BY id) AS shifted "
            "FROM offset_scalar ORDER BY id")
        expected_sql[f"{function.lower()}_offset_scalar_untyped_null_valid_string_default"] = (
            f"SELECT id,{function}(NULL,1,'99.5') OVER (ORDER BY id) AS shifted "
            "FROM offset_scalar ORDER BY id")
    assert len(expected_sql) == 46 and {case["id"] for case in cases} == set(expected_sql)
    for case in cases:
        assert set(case) == {"corpus", "id", "ansi", "sql", "ordered", "ownership", "coverage"}
        assert case["corpus"] == corpus["corpus"] and case["ownership"] == "required_offset_window_fields"
        assert case["sql"] == expected_sql[case["id"]] and case["ordered"] is True
        assert case["sql"].count(" OVER (ORDER BY id)") == 1
        assert case["coverage"]["outcome"] == "UNKNOWN"
        assert type(case["ansi"]) is bool
        peers = [other for other in cases if other["id"] == case["id"]]
        assert len(peers) == 2 and {other["ansi"] for other in peers} == {True, False}
        assert peers[0] | {"ansi": case["ansi"]} == peers[1] | {"ansi": case["ansi"]}
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
