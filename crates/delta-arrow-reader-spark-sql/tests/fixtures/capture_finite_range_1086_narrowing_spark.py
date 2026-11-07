"""Capture the separate source-only signed RANGE narrowing controls with pinned Spark."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True

from capture_date_range_747_spark import (
    REVISION, VERSION, self_check as check_capture_protocol,
)
from capture_spark_requests import capture_requested_queries

HERE = Path(__file__).resolve().parent
REQUEST = HERE / "finite-range-1086-narrowing-cases.json"
PAIRS = [
    ("TINYINT", "SMALLINT", "128"), ("TINYINT", "INT", "128"),
    ("TINYINT", "BIGINT", "128"), ("SMALLINT", "INT", "32768"),
    ("SMALLINT", "BIGINT", "32768"), ("INT", "BIGINT", "2147483648"),
]


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1086
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["expected_record_count"] == 24 and request["sql_source_count"] == 12
    assert request["narrowing_pairs"] == [
        dict(key_type=key, source_offset_type=source, positive_offset=value)
        for key, source, value in PAIRS
    ]
    corpus, = request["corpora"]
    assert corpus["corpus"] == "finite-range-1086-narrowing-controls"
    assert corpus["metadata"] == dict(spark_version=VERSION, timezone="UTC", inputs=[])
    expected = {(key, source, offset, shape, ansi)
                for key, source, offset in PAIRS
                for shape in ["unbounded", "zero_following"] for ansi in [True, False]}
    observed = set()
    identities = set()
    for case in corpus["cases"]:
        assert set(case) == {"corpus", "id", "ansi", "sql", "ordered", "ownership", "coverage"}
        assert case["corpus"] == corpus["corpus"] and isinstance(case["id"], str)
        assert case["ordered"] is True and type(case["ansi"]) is bool
        assert case["ownership"] == "finite_signed_integer_narrowing_control"
        coverage = case["coverage"]
        key, source, offset, shape = (coverage[name] for name in [
            "key_type", "source_offset_type", "positive_offset", "shape"])
        context = key, source, offset, shape, case["ansi"]
        assert context in expected and context not in observed
        observed.add(context)
        identity = case["id"], case["ansi"]
        assert identity not in identities
        identities.add(identity)
        lower = "UNBOUNDED PRECEDING" if shape == "unbounded" else f"CAST(0 AS {source}) FOLLOWING"
        frame = f"RANGE BETWEEN {lower} AND CAST({offset} AS {source}) FOLLOWING"
        values = [[index, f"CAST({value} AS {key})"] for index, value in enumerate([-1, 0, 1])]
        assert coverage["family"] == "narrowing_coercion"
        assert coverage["frame"] == frame and coverage["inline_values"] == values
        assert coverage["order"] == "ASC NULLS FIRST" and coverage["outer_order"] == "id"
        sql_values = ",".join(f"({index},{value})" for index, value in values)
        assert case["sql"] == (
            f"SELECT id,COUNT(*) OVER (ORDER BY k ASC NULLS FIRST {frame}) AS n "
            f"FROM VALUES {sql_values} AS t(id,k) ORDER BY id")
    assert observed == expected and len(identities) == len(corpus["cases"]) == 24
    assert len({case["sql"] for case in corpus["cases"]}) == 12
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
