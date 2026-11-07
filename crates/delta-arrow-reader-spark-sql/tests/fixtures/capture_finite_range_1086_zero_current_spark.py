"""Capture separate zero-expression and CURRENT ROW signed RANGE controls."""

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
REQUEST = HERE / "finite-range-1086-zero-current-cases.json"
KEY_TYPES = ["TINYINT", "SMALLINT", "INT", "BIGINT"]
SHAPES = {
    "zero_preceding_current": "RANGE BETWEEN 0 PRECEDING AND CURRENT ROW",
    "zero_preceding_shorthand": "RANGE 0 PRECEDING",
    "zero_following_current": "RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW",
}
WRAPPED = [("TINYINT", "INT", "256"), ("SMALLINT", "INT", "65536"),
           ("INT", "BIGINT", "4294967296")]


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1086
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["expected_record_count"] == 30 and request["sql_source_count"] == 15
    assert request["signed_key_types"] == KEY_TYPES
    assert request["wrapped_zero_pairs"] == [
        dict(key_type=key, source_offset_type=source, positive_offset=offset)
        for key, source, offset in WRAPPED
    ]
    corpus, = request["corpora"]
    assert corpus["corpus"] == "finite-range-1086-zero-current-controls"
    assert corpus["metadata"] == dict(spark_version=VERSION, timezone="UTC", inputs=[])
    expected = {(key, "INT", "0", shape, ansi) for key in KEY_TYPES
                for shape in SHAPES for ansi in [True, False]}
    expected.update((key, source, offset, "wrapped_preceding_current", ansi)
                    for key, source, offset in WRAPPED for ansi in [True, False])
    observed, identities = set(), set()
    for case in corpus["cases"]:
        assert set(case) == {"corpus", "id", "ansi", "sql", "ordered", "ownership", "coverage"}
        assert case["corpus"] == corpus["corpus"] and isinstance(case["id"], str)
        assert case["ordered"] is True and type(case["ansi"]) is bool
        assert case["ownership"] == "finite_signed_integer_zero_current_control"
        coverage = case["coverage"]
        key, source, offset, shape = (coverage[name] for name in [
            "key_type", "source_offset_type", "positive_offset", "shape"])
        context = key, source, offset, shape, case["ansi"]
        assert context in expected and context not in observed
        observed.add(context)
        identity = case["id"], case["ansi"]
        assert identity not in identities
        identities.add(identity)
        wrapped = shape == "wrapped_preceding_current"
        frame = (f"RANGE BETWEEN CAST({offset} AS {source}) PRECEDING AND CURRENT ROW"
                 if wrapped else SHAPES[shape])
        values = [[index, f"CAST({value} AS {key})"]
                  for index, value in enumerate(["NULL", "NULL", "0", "0", "1"])]
        assert coverage["family"] == ("wrapped_zero_current" if wrapped else "zero_current")
        assert coverage["frame"] == frame and coverage["inline_values"] == values
        assert coverage["order"] == "ASC NULLS FIRST" and coverage["outer_order"] == "id"
        sql_values = ",".join(f"({index},{value})" for index, value in values)
        assert case["sql"] == (
            f"SELECT id,COUNT(*) OVER (ORDER BY k ASC NULLS FIRST {frame}) AS n "
            f"FROM VALUES {sql_values} AS t(id,k) ORDER BY id")
    assert observed == expected and len(identities) == len(corpus["cases"]) == 30
    assert len({case["sql"] for case in corpus["cases"]}) == 15
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
