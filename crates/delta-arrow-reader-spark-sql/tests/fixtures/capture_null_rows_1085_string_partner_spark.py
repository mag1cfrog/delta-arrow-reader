"""Capture source-only issue 1085 NULL/STRING partner probes with the maintained protocol."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True

from capture_date_range_747_spark import REVISION, VERSION, self_check
from capture_spark_requests import capture_requested_queries

HERE = Path(__file__).resolve().parent
REQUEST = HERE / "null-rows-1085-string-partner-cases.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    request = json.loads(REQUEST.read_text())
    assert sys.version_info[:2] == (3, 12)
    assert request["issue"] == 1085 and request["expected_record_count"] == 8
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["batch_sizes"] == [1, 32]
    assert request["target_partitions"] == request["provider_partitions"] == 2
    corpus, = request["corpora"]
    cases = corpus["cases"]
    assert len(cases) == len({(case["id"], case["ansi"]) for case in cases}) == 8
    assert len({case["sql"] for case in cases}) == request["sql_source_count"] == 4
    assert all(case["corpus"] == corpus["corpus"] and case["ordered"] is False for case in cases)
    assert all("expected" not in case and "status" not in case for case in cases)
    assert {case["ansi"] for case in cases} == {True, False}
    assert [len(provider["rows"]) for provider in corpus["metadata"]["inputs"]] == [78, 0]
    if args.self_check:
        assert args.output is None and "pyspark" not in sys.modules
        self_check(request)
        return
    capture_requested_queries(request, args.output, [Path(__file__).resolve(), REQUEST], [REQUEST])


if __name__ == "__main__":
    main()
