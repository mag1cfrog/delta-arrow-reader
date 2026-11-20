#!/usr/bin/env python3
"""Compare the reduced Spark 4.2.0 captures and choose the Spark-only timing cases."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from run import cases, probe


MODES = ("spark-off", "spark-on")


def write(path, value):
    with path.open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def result_types(value):
    if isinstance(value, dict):
        return {key: result_types(item) for key, item in value.items()
                if key not in ("nullable", "containsNull", "valueContainsNull", "metadata")}
    if isinstance(value, list):
        return [result_types(item) for item in value]
    return value


def ordered_rows(rows):
    return sorted(rows, key=lambda row: json.dumps(row, separators=(",", ":")))


def classify(actual, reference):
    if reference["status"] == "error":
        return "spark-rejects"
    assert reference["status"] == "ok", reference
    if actual["status"] != "ok":
        return "dar-fails"
    computed = actual["result"]
    expected = reference["schema_stages"]["executed"]
    if result_types(computed["schema"]) != result_types(expected):
        return "executed-type-difference"
    if ordered_rows(computed["rows"]) != ordered_rows(reference["rows_raw"]):
        return "value-difference"
    return "equal" if computed["schema"] == expected else "field-flags-only"


def compare(driver, capture, output, bitmap):
    output.mkdir(parents=True, exist_ok=False)
    reference_group = "bitmap-reduced" if bitmap else "spark"
    reference_cases = [c for c in cases() if c["reference"] == reference_group]
    observe = output / "observation-manifest.json"
    write(observe, {"functions": reference_cases})
    with capture.open() as source:
        header = json.loads(next(source))
        records = [json.loads(line) for line in source]
    assert header["spark_version"] == "4.2.0" and header["spark_only"]
    assert len(records) == 2 * len(reference_cases)
    spark = {(r["id"], "spark-on" if r["ansi"] else "spark-off"): r for r in records}
    assert len(spark) == len(records)

    observed = {}
    for mode in MODES:
        records = probe(driver, "reference", ["observe", str(observe), mode], output / f"{mode}.jsonl")
        assert records[-1] == {"status": "complete"}
        assert len(records) == len(reference_cases) + 1
        observed.update({(r["id"], r["configuration"]): r["observation"] for r in records[:-1]})
    assert set(observed) == set(spark)
    summarized, differences, eligible = [], [], []
    for case in reference_cases:
        id_ = case["id"]
        row = {"id": id_, "spark_sql": case["spark_sql"]}
        for mode in MODES:
            key = id_, mode
            reference, actual = spark[key], observed[key]
            assert reference["sql"] == case["spark_sql"]
            row[mode] = classify(actual, reference)
            if row[mode] not in {"equal", "field-flags-only"}:
                differences.append({"id": id_, "sql": case["spark_sql"], "configuration": mode,
                                    "class": row[mode], "dar": actual, "spark": reference})
        summarized.append(row)
        if all(row[mode] in {"equal", "field-flags-only"} for mode in MODES):
            eligible.append({**case, "native_sql": None})
    write(output / "classification.json", summarized)
    write(output / "differences.json", differences)
    write(output / "timing-manifest.json", {"functions": eligible})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("driver", type=Path)
    parser.add_argument("capture", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--bitmap", action="store_true")
    args = parser.parse_args()
    compare(args.driver.resolve(), args.capture.resolve(), args.output.resolve(), args.bitmap)


if __name__ == "__main__":
    main()
