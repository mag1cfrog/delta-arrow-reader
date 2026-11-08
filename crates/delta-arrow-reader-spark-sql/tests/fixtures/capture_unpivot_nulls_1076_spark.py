"""Capture source-only single-value UNPIVOT requests with the maintained protocol.

New outcomes remain unknown until Spark runs. The four existing mixed-default
references stay unchanged; this driver opens no recorded outcomes.
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
REQUEST = HERE / "unpivot-null-1076-cases.json"
ORIGINAL_MANIFEST = HERE / "date-range-747-cases.json"
PROVIDERS = ["inputs", "empty_inputs", "null_inputs"]
FAMILY_COUNTS = {
    "missing_core": 16, "accepted_alias_equivalence": 4,
    "required_metadata_duplicate_names": 4, "required_error_probe": 6,
}


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1076
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["expected_record_count"] == 30 and request["sql_source_count"] == 15
    assert request["retained_reference_count"] == 4
    assert request["family_record_counts"] == FAMILY_COUNTS
    assert request["native_batch_sizes"] == [1, 32]
    assert request["native_provider_partitions"] == request["native_target_partitions"] == 2
    assert request["spark_provider_partitions_expected"] == 1
    provenance = request["original_provider_source"]
    assert provenance["file"] == ORIGINAL_MANIFEST.name
    assert provenance["corpus"] == "original" and provenance["provider_names"] == PROVIDERS
    with ORIGINAL_MANIFEST.open("rb") as source:
        assert hashlib.file_digest(source, "sha256").hexdigest() == provenance["sha256"]
    original = json.loads(ORIGINAL_MANIFEST.read_text())
    inherited, = [corpus for corpus in original["corpora"]
                  if corpus["corpus"] == provenance["corpus"]]
    corpus, = request["corpora"]
    assert corpus["corpus"] == "unpivot-null-1076-supplement"
    metadata = corpus["metadata"]
    assert set(metadata) == {"spark_version", "timezone", "inputs"}
    assert metadata["spark_version"] == VERSION and metadata["timezone"] == "UTC"
    assert metadata["inputs"][:3] == inherited["metadata"]["inputs"]
    assert [item["name"] for item in metadata["inputs"][:3]] == PROVIDERS
    assert [len(item["rows"]) for item in metadata["inputs"][:3]] == [11, 0, 2]
    required, = metadata["inputs"][3:]
    assert set(required) == {"name", "schema", "rows"}
    assert required["name"] == "unpivot_required" and required["schema"] == {
        "type": "struct", "fields": [
            {"name": name, "type": "integer", "nullable": False,
             "metadata": {"source": name}} for name in ["id", "g", "v"]],
    }
    assert required["rows"] == [["0", "1", "2"], ["0", "1", "2"]]
    cases = corpus["cases"]
    assert len(cases) == len({(case["corpus"], case["id"], case["ansi"])
                             for case in cases}) == 30
    assert len({case["sql"] for case in cases}) == 15
    assert Counter(case["coverage"]["family"] for case in cases) == FAMILY_COUNTS
    for case in cases:
        assert set(case) == {"corpus", "id", "ansi", "sql", "ordered", "ownership", "coverage"}
        assert case["corpus"] == corpus["corpus"]
        assert case["ownership"] == "single_value_unpivot_null_rows"
        assert isinstance(case["sql"], str) and case["sql"]
        assert type(case["ansi"]) is bool and case["ordered"] is False
        assert case["coverage"]["provider"] in PROVIDERS + ["unpivot_required"]
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
        request, args.output,
        [Path(__file__).resolve(), REQUEST, ORIGINAL_MANIFEST], [REQUEST],
    )


if __name__ == "__main__":
    main()
