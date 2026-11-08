"""Capture source-only mixed-nullability UNPIVOT requests with the maintained protocol.

Output rows, Fields, plans and errors remain unknown until Spark runs.
"""

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
REQUEST = HERE / "unpivot-nullability-1076-cases.json"
PROVIDERS = ["unpivot_partial_required", "empty_unpivot_partial_required"]


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1076
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["sql_source_count"] == 4 and request["expected_record_count"] == 8
    assert request["retained_reference_count"] == 0
    assert request["native_batch_sizes"] == [1, 32]
    assert request["native_provider_partitions"] == request["native_target_partitions"] == 2
    assert request["spark_provider_partitions_expected"] == 1
    corpus, = request["corpora"]
    assert corpus["corpus"] == "unpivot-nullability-1076-supplement"
    assert corpus["source"] == {"kind": "synthetic mixed-nullability fixture"}
    schema = {"type": "struct", "fields": [
        {"name": name, "type": "integer", "nullable": name == "v", "metadata": {"source": name}}
        for name in ["id", "g", "v"]
    ]}
    assert corpus["metadata"] == {
        "spark_version": VERSION, "timezone": "UTC", "inputs": [
            {"name": PROVIDERS[0], "schema": schema, "rows": [["0", "1", None], ["1", "2", "3"]]},
            {"name": PROVIDERS[1], "schema": schema, "rows": []},
        ],
    }
    expected = []
    for provider in PROVIDERS:
        for mode, clause in [("default", ""), ("include", "INCLUDE NULLS ")]:
            for ansi in [True, False]:
                expected.append({
                    "corpus": corpus["corpus"], "id": f"{provider}_{mode}", "ansi": ansi,
                    "sql": (f"SELECT o.* FROM (SELECT id,g,v FROM {provider}) p UNPIVOT "
                            f"{clause}(value FOR label IN (g,v)) o"),
                    "ordered": False, "ownership": "single_value_unpivot_null_rows",
                    "coverage": {"family": "mixed_nullability_boundary", "provider": provider, "mode": mode},
                })
    assert corpus["cases"] == expected
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
