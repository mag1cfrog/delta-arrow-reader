"""Freeze Apache Spark observations, or check an engine against that fixed oracle."""

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from reference import ROOT, corpus_hash, load_corpus


def row_key(row):
    return json.dumps(row, sort_keys=True)


def checked_rows(case, rows):
    mode = case.get("comparison", "multiset")
    if mode == "ordered":
        return rows
    if mode == "multiset":
        return Counter(map(row_key, rows))
    # These queries expose id and partition id so we can check partition-local
    # properties without freezing one engine's partition assignment.
    groups = defaultdict(list)
    values = []
    for row in rows:
        expected_width = 3 if mode == "monotonic_id" else 2
        if len(row) != expected_width or int(row[1]) < 0:
            raise ValueError("invalid partition output")
        groups[int(row[1])].append(int(row[-1] if mode == "monotonic_id" else row[0]))
        if mode == "monotonic_id":
            values.append(int(row[2]))
    if mode in {"partition_sort", "monotonic_id"}:
        if any(group != sorted(group) for group in groups.values()):
            raise ValueError("partition-local order violated")
    if mode == "monotonic_id" and (len(values) != len(set(values)) or any(x < 0 for x in values)):
        raise ValueError("monotonic IDs must be unique and nonnegative")
    return Counter(row[0] for row in rows)


def schema_dimensions(schema):
    nullability, metadata = [], []

    def strip(value, path=()):
        if isinstance(value, list):
            return [strip(v, (*path, i)) for i, v in enumerate(value)]
        if not isinstance(value, dict):
            return value
        result = {}
        for key, item in value.items():
            if key in {"nullable", "containsNull", "valueContainsNull"}:
                nullability.append([list((*path, key)), item])
            elif key == "metadata":
                metadata.append([list(path), item])
            else:
                result[key] = strip(item, (*path, key))
        return result

    stripped = strip(schema)
    return {
        "names": [field["name"] for field in schema["fields"]],
        "types": [field["type"] for field in stripped["fields"]],
        "nullability": nullability,
        "metadata": metadata,
    }


def compare(case, expected, actual):
    differences = []
    if expected["status"] == "requires_host_adapter":
        return {"status": "pending_host_adapter", "differences": []}
    if expected["status"] != actual["status"]:
        differences.append("status")
    if expected["status"] == actual["status"] == "ok":
        try:
            if checked_rows(case, expected["rows"]) != checked_rows(case, actual["rows"]):
                differences.append("rows")
        except (ValueError, TypeError, IndexError) as error:
            differences.append(f"row_invariant: {error}")
        expected_schema = schema_dimensions(expected["schema"])
        actual_schema = schema_dimensions(actual["schema"])
        differences += [key for key in expected_schema if expected_schema[key] != actual_schema[key]]
    elif expected["status"].endswith("_error") and actual["status"].endswith("_error"):
        # Same error stage does not prove the same cause. Spark conditions are
        # recorded separately; a Rust adapter can later map its typed errors.
        if expected.get("condition") != actual.get("condition"):
            differences.append("error_condition")
    return {"status": "difference" if differences else "match", "differences": differences}


def validate_capture(capture, inputs, cases):
    if capture.get("corpus_sha256") != corpus_hash(inputs, cases):
        raise ValueError("capture does not match current inputs/settings/queries")
    if [row["id"] for row in capture["observations"]] != [case["id"] for case in cases]:
        raise ValueError("capture is incomplete, reordered or has duplicate/unknown IDs")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["freeze", "check"])
    parser.add_argument("capture", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--against", type=Path, help="Compare with another capture; defaults to the Spark oracle")
    args = parser.parse_args()
    inputs, cases = load_corpus()
    capture = json.loads(args.capture.read_text())
    validate_capture(capture, inputs, cases)
    oracle_path = ROOT / "spark-oracle.json"
    if args.command == "freeze":
        if args.against:
            parser.error("--against is only valid with check")
        if capture["engine"] != "spark" or capture["reference"] != inputs["spark_reference"]:
            raise ValueError("only the pinned Apache Spark engine can create the oracle")
        if capture["spark_version"] != inputs["spark_reference"]["version"]:
            raise ValueError("Apache Spark runtime version does not match its package")
        for case, observation in zip(cases, capture["observations"], strict=True):
            if bool(case.get("host_only")) != (observation["status"] == "requires_host_adapter"):
                raise ValueError(f"unexpected missing execution: {case['id']}")
            if observation["status"] == "ok":
                checked_rows(case, observation["rows"])
            elif observation["status"].endswith("_error") and not observation.get("condition"):
                raise ValueError(f"cannot freeze an unclassified Spark failure: {case['id']}")
            observation.pop("error", None)  # Keep stable condition/stage, not JVM stack traces.
        # One case per line keeps generated expectations reviewable in diffs.
        observations = capture.pop("observations")
        header = json.dumps(capture, indent=2)
        oracle_path.write_text(header[:-2] + ',\n  "observations": [\n' +
                               ',\n'.join('    ' + json.dumps(row) for row in observations) + '\n  ]\n}\n')
        print(f"Froze {len(observations)} cases from Apache Spark {capture['spark_version']}")
        return

    oracle = json.loads((args.against or oracle_path).read_text())
    validate_capture(oracle, inputs, cases)
    results = [{"id": case["id"], **compare(case, expected, actual)}
               for case, expected, actual in zip(cases, oracle["observations"], capture["observations"], strict=True)]
    summary = dict(Counter(result["status"] for result in results))
    input_rows_match = all(Counter(map(row_key, oracle["input_rows"][name])) ==
                           Counter(map(row_key, capture["input_rows"][name])) for name in inputs["tables"])
    report = {"reference": capture["reference"], "summary": summary,
              "input_schema_match": oracle["input_schemas"] == capture["input_schemas"],
              "input_rows_match": input_rows_match, "cases": results}
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "cases"}))
    raise SystemExit(1 if summary.get("difference") or not report["input_schema_match"] or not input_rows_match else 0)


if __name__ == "__main__":
    main()
