"""Capture source-only NULL ROWS admission requests with the maintained Spark protocol.

Outcomes remain unknown until capture. The original eight nonempty Spark records
stay unchanged in their existing fixture; this driver opens no recorded outcomes.
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
REQUEST = HERE / "null-rows-1085-cases.json"
ORIGINAL_MANIFEST = HERE / "null-range-754-cases.json"
ORIGINAL_IDS = {
    "admission_rows_lower_typed_null", "admission_rows_upper_typed_null",
    "admission_rows_lower_bare_null", "admission_rows_upper_bare_null",
}
PROVIDERS = ["null_range_mixed", "null_range_empty"]
FAMILY_COUNTS = {
    "original_core": 8, "core_empty": 8, "paired_precedence": 40,
    "null_expression": 16, "direction": 8, "function_precedence": 12,
}


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1085
    assert request["spark_version"] == VERSION and request["spark_revision"] == REVISION
    assert request["batch_sizes"] == [1, 32]
    assert request["target_partitions"] == request["provider_partitions"] == 2
    assert request["expected_record_count"] == 92 and request["sql_source_count"] == 46
    assert request["original_record_count"] == 8 and request["supplement_record_count"] == 84
    assert request["family_record_counts"] == FAMILY_COUNTS
    provenance = request["original_source_manifest"]
    assert provenance["file"] == ORIGINAL_MANIFEST.name
    assert provenance["corpus"] == "null-range-754-supplement"
    assert set(provenance["ids"]) == ORIGINAL_IDS and provenance["provider_names"] == PROVIDERS
    with ORIGINAL_MANIFEST.open("rb") as source:
        assert hashlib.file_digest(source, "sha256").hexdigest() == provenance["sha256"]
    original = json.loads(ORIGINAL_MANIFEST.read_text())
    inherited, = [corpus for corpus in original["corpora"]
                  if corpus["corpus"] == provenance["corpus"]]
    retained, supplement = request["corpora"]
    metadata = dict(inherited["metadata"], inputs=[
        item for item in inherited["metadata"]["inputs"] if item["name"] in PROVIDERS])
    assert [item["name"] for item in metadata["inputs"]] == PROVIDERS
    assert [len(item["rows"]) for item in metadata["inputs"]] == [78, 0]
    assert metadata["inputs"][0]["schema"] == metadata["inputs"][1]["schema"]
    assert retained["corpus"] == inherited["corpus"] and retained["source"] == inherited["source"]
    assert retained["metadata"] == supplement["metadata"] == metadata
    assert retained["cases"] == [
        case for case in inherited["cases"] if case["id"] in ORIGINAL_IDS]
    assert len(retained["cases"]) == 8
    assert supplement["corpus"] == "null-rows-1085-supplement"

    catalog = request["probe_catalog"]
    assert len(catalog) == len({probe["shape"] for probe in catalog}) == 26
    assert Counter(probe["family"] for probe in catalog) == {
        "core_empty": 4, "paired_precedence": 10, "null_expression": 4,
        "direction": 2, "function_precedence": 6,
    }
    expected = {}
    for probe in catalog:
        assert set(probe) == {"shape", "family", "function", "window", "providers"}
        family = probe["family"]
        assert all(isinstance(probe[key], str) and probe[key]
                   for key in ["shape", "family", "function", "window"])
        providers = (["null_range_empty"] if family == "core_empty"
                     else ["null_range_mixed"] if family == "function_precedence"
                     else PROVIDERS)
        assert probe["providers"] == providers
        if family == "core_empty":
            original_id = "admission_rows_" + probe["shape"].removeprefix("core_")
            assert original_id in ORIGINAL_IDS
            original_case, = [case for case in retained["cases"]
                              if case["id"] == original_id and case["ansi"]]
            assert probe["function"] == "COUNT(*)"
            assert probe["window"] == original_case["coverage"]["window"]
        for provider in providers:
            label = "empty" if provider == "null_range_empty" else "mixed"
            for ansi in [True, False]:
                case_id = f"{probe['shape']}_{label}"
                identity = supplement["corpus"], case_id, ansi
                assert identity not in expected
                expected[identity] = {
                    "corpus": supplement["corpus"], "id": case_id, "ansi": ansi,
                    "sql": (f"SELECT id,{probe['function']} OVER ({probe['window']}) AS n "
                            f"FROM {provider}"),
                    "ordered": False, "ownership": "null_rows_bound_admission",
                    "coverage": {
                        "family": family, "provider": provider, "shape": probe["shape"],
                        "function": probe["function"], "window": probe["window"],
                        "empty": provider == "null_range_empty",
                    },
                }
    observed = {}
    for case in supplement["cases"]:
        identity = case["corpus"], case["id"], case["ansi"]
        assert type(case["ansi"]) is bool and case["ordered"] is False
        assert identity not in observed and case == expected[identity]
        observed[identity] = case
    assert observed == expected and len(observed) == 84
    counts = Counter(case["coverage"]["family"] for case in observed.values())
    assert dict(counts, original_core=8) == FAMILY_COUNTS
    cases = retained["cases"] + supplement["cases"]
    assert len(cases) == len({(case["corpus"], case["id"], case["ansi"])
                             for case in cases}) == 92
    assert len({case["sql"] for case in cases}) == 46
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
