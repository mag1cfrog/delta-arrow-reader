"""Compare recorded arithmetic schemas and error causes without changing value checks."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re

import pyarrow as pa

from decimal_arithmetic_types import cause as decimal_cause
from early_arithmetic import cause as arithmetic_cause
from extracted import spark_field
from oracle import schema_dimensions


def ambiguous_decimal_cast(actual):
    return not actual.get("condition") and re.fullmatch(
        r"Execution error: Cannot cast Some\(.*\) to DECIMAL\(\d+,-?\d+\)",
        actual.get("error", "").rsplit("\ncaused by\n", 1)[-1],
    ) is not None


def cause(actual):
    # The selected string-to-Decimal adapter erases parsing, source-range and
    # target-precision failures into the same message. Do not infer one cause.
    if ambiguous_decimal_cast(actual):
        return None
    known = arithmetic_cause(actual)
    if actual.get("condition") or known not in (None, "CAST_INVALID_INPUT"):
        return known
    text = actual.get("error", "").rsplit("\ncaused by\n", 1)[-1]
    if text == "Arrow error: Arithmetic overflow: Spark decimal division result exceeds its precision":
        return "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"
    # Reuse the Decimal classifier only for Arrow's own CAST/range messages.
    # A nested range message inside ROUND keeps its already known condition.
    if text.startswith(("Arrow error: Cast error: Cannot cast to Decimal128(",
                        "Arrow error: Invalid argument error: ")):
        return decimal_cause(actual) or known
    return known


def native_schema(actual, phase):
    payload = actual.get(f"{phase}_schema_ipc")
    if payload is None:
        return None
    schema = pa.ipc.open_stream(bytes(payload)).schema
    names = actual["output_names"]
    if len(names) != len(schema):
        raise ValueError("output names do not match the captured schema")
    # Keep the unmodified IPC bytes in the capture. NamedPlan supplies the same
    # public names used by the host runner's existing output-renaming adapter.
    return {"type": "struct", "fields": [spark_field(field.with_name(name))
            for field, name in zip(schema, names, strict=True)]}


def schema_checks(expected, actual):
    if expected is None or actual is None:
        return {key: "unobserved" for key in ("names", "types", "nullability", "metadata")}
    left, right = schema_dimensions(expected), schema_dimensions(actual)
    return {key: "match" if left[key] == right[key] else "difference" for key in left}


def compare(reference, candidate):
    cases = reference["cases"]
    ids = [f"{case['id']}_{ansi}" for case in cases for ansi in ("true", "false")]
    if (candidate["cases"] != cases or len(set(ids)) != len(ids)
            or [row["id"] for row in reference["results"]] != ids
            or [row["id"] for row in candidate["results"]] != ids):
        raise ValueError("captures must contain identical SQL and complete ordered unique IDs")
    checks = []
    for left, right in zip(reference["results"], candidate["results"], strict=True):
        expected, actual = left["actual"], right["actual"]
        schemas = {phase: native_schema(actual, phase) for phase in ("logical", "physical")}
        row = {"id": left["id"], "stage": [expected["status"], actual["status"]],
               "stage_check": "match" if expected["status"] == actual["status"] else "difference",
               "reference_schema": expected.get("schema"), "native_schemas": schemas,
               "schemas": {phase: schema_checks(expected.get("schema"), schema)
                           for phase, schema in schemas.items()}}
        if expected["status"] != "ok" and actual["status"] != "ok":
            causes = [cause(expected), cause(actual)]
            row["error_causes"] = causes
            row["error_cause_check"] = ("ambiguous" if any(ambiguous_decimal_cast(a) for a in (expected, actual)) else
                                       "unclassified" if None in causes else
                                       "match" if causes[0] == causes[1] else "difference")
            # Inferred cause equality does not verify SQLSTATE, message
            # parameters, first-invalid input, or an engine's typed error API.
            row["error_parameters_check"] = "unobserved"
        else:
            row["error_cause_check"] = "not_applicable"
        checks.append(row)
    summary = {"total": len(checks),
               "stage": dict(Counter(row["stage_check"] for row in checks)),
               "error_cause": dict(Counter(row["error_cause_check"] for row in checks)),
               "schemas": {phase: {key: dict(Counter(row["schemas"][phase][key] for row in checks))
                                   for key in ("names", "types", "nullability", "metadata")}
                           for phase in ("logical", "physical")}}
    return {"summary": summary, "cases": checks}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("reference", "candidate", "report"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    result = compare(json.loads(args.reference.read_text()), json.loads(args.candidate.read_text()))
    args.report.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summary"]))
