"""Capture standalone #1123 source controls with the maintained UTC protocol.

Rows, Fields, errors and phases are unknown until root captures them. This
driver opens no recorded outcomes and changes no timezone/decoder protocol.
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
REQUEST = HERE / "temporal-fold-1123-cases.json"
BASE = "63cb530bfc0183156ae9a6345abb741ca3451e48"
PROVIDERS = [
    "offset_scalar", "empty_offset_scalar", "null_offset_scalar",
    "offset_nested", "empty_offset_nested", "null_offset_nested",
]
FAMILY_COUNTS = {
    "strict_constant_cast": 6, "safe_try_cast": 6,
    "unreachable_case": 4, "coalesce_guard": 4, "limit_zero_guard": 4,
    "unused_projection": 4, "formatted_parser": 4,
    "explicit_offset_temporal_default": 12,
}
HELPERS = {
    "capture_spark_requests.py": "1a813099731f8dc6bba26bb71bcc2fd56cd0b4e9c7b430f27a0a86950d35ef17",
    "capture_date_range_747_spark.py": "b8ec7266c62a60a60262d8b0a988f712d8995c58c6a8410e105c13ac0afa3922",
    "capture_floating_group_keys_spark.py": "9db227451bea43d54e8dcd30d24501e78b18e752f22d971d285f863f058fd934",
    "capture_window_name_stages_spark.py": "52a3efa953716f7c1770952990beb655de1414561f278ef65c25599d29739633",
    "recapture_string_binary_spark.py": "3fa0da4a0a44fb7560097285442819f0e722af2c3791ce2622e02e64712d03b8",
}
AUTHOR_SQL = {
    "direct_date": "SELECT CAST('2021-02-29' AS DATE) AS cast_value",
    "direct_timestamp": "SELECT CAST('2021-02-29 12:34:56' AS TIMESTAMP) AS cast_value",
    "direct_timestamp_ntz": "SELECT CAST('2021-02-29 12:34:56' AS TIMESTAMP_NTZ) AS cast_value",
    "try_date": "SELECT TRY_CAST('2021-02-29' AS DATE) AS cast_value",
    "try_timestamp": "SELECT TRY_CAST('2021-02-29 12:34:56' AS TIMESTAMP) AS cast_value",
    "try_timestamp_ntz": "SELECT TRY_CAST('2021-02-29 12:34:56' AS TIMESTAMP_NTZ) AS cast_value",
    "unreachable_date": "SELECT CASE WHEN false THEN CAST('2021-02-29' AS DATE) ELSE DATE '2020-01-02' END AS cast_value",
    "unreachable_timestamp": "SELECT CASE WHEN false THEN CAST('2021-02-29 12:34:56' AS TIMESTAMP) ELSE TIMESTAMP '2020-01-02 03:04:05' END AS cast_value",
    "coalesce_date": "SELECT COALESCE(DATE '2020-01-02',CAST('2021-02-29' AS DATE)) AS cast_value",
    "coalesce_timestamp": "SELECT COALESCE(TIMESTAMP '2020-01-02 03:04:05',CAST('2021-02-29 12:34:56' AS TIMESTAMP)) AS cast_value",
    "limit_zero_date": "SELECT CAST('2021-02-29' AS DATE) AS cast_value LIMIT 0",
    "limit_zero_timestamp": "SELECT CAST('2021-02-29 12:34:56' AS TIMESTAMP) AS cast_value LIMIT 0",
    "unused_date": "SELECT 7 AS kept FROM (SELECT CAST('2021-02-29' AS DATE) AS unused_value) t",
    "unused_timestamp": "SELECT 7 AS kept FROM (SELECT CAST('2021-02-29 12:34:56' AS TIMESTAMP) AS unused_value) t",
    "formatted_date": "SELECT TO_DATE('2021-02-29','yyyy-MM-dd') AS cast_value",
    "formatted_timestamp": "SELECT TO_TIMESTAMP('2021-02-29 12:34:56','yyyy-MM-dd HH:mm:ss') AS cast_value"
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
    assert request["version"] == 1 and request["issue"] == 1123 and request["integration_base"] == BASE
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["python_version"] == "3.12" and request["java_specification_version"] == "21"
    assert request["outcome_state"].startswith("UNKNOWN:")
    assert request["sql_source_count"] == 22 and request["expected_record_count"] == 44
    assert request["family_record_counts"] == FAMILY_COUNTS
    assert request["native_batch_sizes"] == [1, 32]
    assert request["native_provider_partitions"] == request["native_target_partitions"] == 2
    assert request["spark_master"] == "local[1]" and request["spark_provider_partitions_expected"] == 1
    # The request records its original codec; HELPERS pins this capture run.
    assert request["codec"] == {
        "base_encoder_sha256": "273495f1945d74dd8a36719ebc40fc01b35cc93cac85d8993cfdeff865261911",
        "capture_encoder_sha256": "e8755f789b2ef55e25817bb864a05b99194d513bd49db11e418a80b371100943",
        "change": "TimestampType output branch only; base Boolean output/input spelling retained",
        "unmerged_1044_Boolean_fix_included": False,
    }
    author = request["author_controls_source"]
    assert author["sha256"] == "45324c2cf4c1fb6d4ad0e4053216051d75c0048e538d99e34d89882551da80c9"
    assert author["sql_definitions"] == 16 and author["records"] == 32
    assert author["all_original_fields_retained"] and author["SQL_ids_ANSI_timezone_ordering_unchanged"]
    for name, expected_hash in HELPERS.items():
        with (HERE / name).open("rb") as helper:
            assert hashlib.file_digest(helper, "sha256").hexdigest() == expected_hash, name
    corpus, = request["corpora"]
    assert corpus["corpus"] == "temporal-folding-dependency-1044"
    assert corpus["source"]["reads_expected_outputs"] is False
    metadata = corpus["metadata"]
    assert metadata["spark_version"] == VERSION and metadata["timezone"] == "UTC"
    assert [provider["name"] for provider in metadata["inputs"]] == PROVIDERS
    assert [len(provider["rows"]) for provider in metadata["inputs"]] == [8, 0, 3, 4, 0, 3]
    for provider in metadata["inputs"]:
        validate_schema(provider["schema"])
        for row in provider["rows"]:
            validate_value(row, provider["schema"], False)
    cases = corpus["cases"]
    assert len(cases) == len({(case["corpus"], case["id"], case["ansi"]) for case in cases}) == 44
    assert len({case["sql"] for case in cases}) == 22
    assert Counter(case["coverage"]["family"] for case in cases) == FAMILY_COUNTS
    assert {case["id"] for case in cases[:32]} == set(AUTHOR_SQL)
    assert all(case["sql"] == AUTHOR_SQL[case["id"]] for case in cases[:32])
    requested_sql = dict(AUTHOR_SQL)
    for function in ["LAG", "LEAD"]:
        for target, payload, default in [
            ("date", "DATE '2020-01-02'", "CAST('2021-02-29' AS DATE)"),
            ("timestamp", "TIMESTAMP '2020-01-02 03:04:05.123456'",
             "CAST('2021-02-29 12:34:56' AS TIMESTAMP)"),
            ("timestamp_ntz", "CAST('2020-01-02 03:04:05.123456' AS TIMESTAMP_NTZ)",
             "CAST('2021-02-29 12:34:56' AS TIMESTAMP_NTZ)"),
        ]:
            requested_sql[f"{function.lower()}_{target}_explicit_invalid_cast_default"] = (
                f"SELECT id,{function}({payload},1,{default}) OVER (ORDER BY id) AS shifted "
                "FROM offset_scalar ORDER BY id")
    assert len(requested_sql) == 22 and {case["id"] for case in cases} == set(requested_sql)
    for case in cases:
        assert set(case) == {"corpus", "id", "sql", "ansi", "timezone", "ordered", "ownership", "coverage"}
        assert case["corpus"] == corpus["corpus"] and case["ownership"] == "strict_temporal_constant_folding"
        assert case["sql"] == requested_sql[case["id"]] and case["ordered"] is True
        assert case["timezone"] == "UTC" and type(case["ansi"]) is bool
        assert case["coverage"]["outcome"] == "UNKNOWN"
        if case["coverage"]["family"] == "explicit_offset_temporal_default":
            assert case["sql"].count(" OVER (ORDER BY id)") == 1
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
