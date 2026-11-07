"""Capture independent issue1086 finite signed-integer RANGE source requests.

Only the sibling source-only JSON supplies SQL and providers. Historical source
paths are provenance labels; no recorded query outcomes are opened.
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
REQUEST = HERE / "finite-range-1086-cases.json"
ORIGINAL_CORPUS_SHA256 = "bc827646ba02f2f44b62d7e32520008df0432fb4c0b43db05b7b11e9bacc6bbc"
FAMILY_COUNTS = {
    "same_width_stages": 36, "all_null_min": 16, "empty_desc_min": 8,
    "key_edge_arithmetic": 32, "regular_signs_max": 32, "special_coercion": 32,
    "consumers": 8, "cross_width_null_controls": 8,
}


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1086
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["expected_record_count"] == 188
    assert request["original_record_count"] == 16 and request["supplement_record_count"] == 172
    original, supplement = request["corpora"]
    serialized = json.dumps(original, sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(serialized).hexdigest() == ORIGINAL_CORPUS_SHA256
    assert supplement["corpus"] == "finite-range-1086-supplement"
    assert dict(Counter(case["coverage"]["family"] for case in supplement["cases"])) == FAMILY_COUNTS
    assert request["supplement_family_counts"] == FAMILY_COUNTS
    cases = [case for corpus in request["corpora"] for case in corpus["cases"]]
    assert len(cases) == len({(case["corpus"], case["id"], case["ansi"]) for case in cases}) == 188
    assert len({case["sql"] for case in cases}) == 94
    assert all(case["ordered"] is True for case in cases)
    for corpus in request["corpora"]:
        assert corpus["metadata"]["spark_version"] == VERSION
        assert corpus["metadata"]["timezone"] == "UTC"
        providers = corpus["metadata"]["inputs"]
        assert len({item["name"] for item in providers}) == len(providers)
        for item in providers:
            fields = item["schema"]["fields"]
            assert item["schema"]["type"] == "struct" and fields
            for field in fields:
                assert field["type"] == "integer" and type(field["nullable"]) is bool
                assert isinstance(field["name"], str) and isinstance(field["metadata"], dict)
            for row in item["rows"]:
                assert len(row) == len(fields)
                for value, field in zip(row, fields):
                    if value is None:
                        assert field["nullable"]
                    else:
                        assert isinstance(value, str) and str(int(value)) == value
                        assert -(2**31) <= int(value) < 2**31
        for case in corpus["cases"]:
            expected_fields = {"corpus", "id", "ansi", "sql", "ordered", "coverage"}
            if corpus is supplement:
                expected_fields.add("ownership")
            assert set(case) == expected_fields
            assert case["corpus"] == corpus["corpus"] and isinstance(case["id"], str)
            assert type(case["ansi"]) is bool and isinstance(case["sql"], str)
            peers = [other for other in corpus["cases"] if other["id"] == case["id"]]
            assert len(peers) == 2 and {other["ansi"] for other in peers} == {True, False}
            assert len({other["sql"] for other in peers}) == 1
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

