"""Capture the separate issue1086 limited-output source diagnostics."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True

from capture_date_range_747_spark import (
    REVISION, VERSION, self_check as check_capture_protocol,
)
from capture_spark_requests import capture_requested_queries

REQUEST = Path(__file__).with_name("finite-range-1086-limited-cases.json")


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1086
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["expected_record_count"] == 8 and request["sql_source_count"] == 4
    corpus, = request["corpora"]
    assert corpus["corpus"] == "finite-range-1086-limited-output-diagnostics"
    assert corpus["metadata"] == {"spark_version": VERSION, "timezone": "UTC", "inputs": []}
    cases = corpus["cases"]
    assert len(cases) == len({(case["id"], case["ansi"]) for case in cases}) == 8
    assert len({case["sql"] for case in cases}) == 4
    for case in cases:
        assert set(case) == {"corpus", "id", "ansi", "sql", "ordered", "ownership", "coverage"}
        assert case["corpus"] == corpus["corpus"] and case["ordered"] is True
        assert isinstance(case["id"], str) and isinstance(case["sql"], str)
        assert type(case["ansi"]) is bool
        peers = [other for other in cases if other["id"] == case["id"]]
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

