"""Summarize the paired macOS planning observations from committed raw samples."""

import argparse
from collections import defaultdict
import gzip
import json
from pathlib import Path
from statistics import median

METRICS = ("logical", "physical")
NATIVE = "native-configured"
SPARK = ("spark-off", "spark-on")
MERGE_MODES = ("native-plain", NATIVE, *SPARK)
ARMS = ("a", "b")

# Entries nested in other entries are reported separately.
SPANS = {
    "logical": {
        "parse": ("logical.parsing", "native.parsing"),
        "lexing": ("logical.parsing.fast_lexing", "logical.parsing.lexing"),
        "parser_construction": ("logical.parsing.fast_construction", "logical.parsing.parser_construction"),
        "grammar": ("logical.parsing.fast_grammar", "logical.parsing.parse"),
        "ast_conversion": ("logical.ast_conversion",),
        "resolver_preparation": ("logical.resolver_preparation",),
        "resolution": ("logical.resolution", "native.resolution"),
        "substitution": ("logical.substitution",),
        "timezone": ("logical.timezone",),
    },
    "physical": {
        "preparation": ("physical.preparation",),
        "analyzer": ("physical.analyzer",),
        "logical_optimizer": ("physical.optimizer",),
        "projection_rule": ("*/optimize_projections/body",),
        "logical_invariants": ("physical.analyzer.input_validation", "physical.analyzer.final_validation",
                               "physical.optimizer.input_validation", "physical.optimizer.final_validation",
                               "physical.optimizer.final_schema_validation", "*/validation"),
        "metadata_alignment": ("physical.planner.align_result_metadata",),
        "initial_physical_plan": ("default.lowering",),
        "alias_conversion": ("default.projection.expressions",),
        "physical_projection_constructor": ("default.projection.build",),
        "physical_optimizer": ("default.optimization",),
        "physical_invariants": ("default.invariants.start", "default.invariants.finish", "*/invariants"),
        "output_rename": ("physical.rename",),
    },
}


def selected(events, phase, rules):
    total = 0
    for name, elapsed in events:
        boundary, event = name.split("/", 1)
        if boundary == phase and any(event.endswith(rule[1:]) if rule.startswith("*") else event == rule
                                     for rule in rules):
            total += elapsed
    return total


def measures(record):
    samples = record["events"]
    assert len(samples) == 9
    result = {}
    for phase, parts in SPANS.items():
        assert len(record[f"{phase}_ns"]) == len(samples)
        result[phase] = {"wall": median(record[f"{phase}_ns"])}
        for part, includes in parts.items():
            result[phase][part] = median(selected(sample, phase, includes) for sample in samples)
    return result


def noise(series):
    a, b = (series[arm] for arm in ARMS)
    return max(abs(median(a) - median(b)), (max(a) - min(a)) / 2, (max(b) - min(b)) / 2)


def session_rows(path, statements):
    by_cell = defaultdict(dict)
    with gzip.open(path, "rt") as stream:
        header = json.loads(next(stream))
        assert header["kind"] == "session"
        for line in stream:
            entry = json.loads(line)
            assert entry["kind"] == "process" and entry["status"] == "ok", entry.get("label")
            record, = entry["records"]
            key = record["id"], record["configuration"]
            slot = entry["arm"], entry["block"]
            assert slot not in by_cell[key]
            by_cell[key][slot] = measures(record)
    assert set(by_cell) == {(case, mode) for case in statements for mode in MERGE_MODES}
    slots = {(arm, block) for arm in ARMS for block in range(1, 10)}
    assert all(set(value) == slots for value in by_cell.values())
    return by_cell


def contrast(spark, native, phase):
    def series(cell, component):
        return {arm: [cell[arm, block][phase][component] for block in range(1, 10)] for arm in ARMS}

    output = {}
    for component in ("wall", *SPANS[phase]):
        left, right = series(native, component), series(spark, component)
        rows = [median(right[arm][i] for arm in ARMS) - median(left[arm][i] for arm in ARMS) for i in range(9)]
        output[component] = {"native_ns": median(left["a"] + left["b"]),
                             "spark_ns": median(right["a"] + right["b"]),
                             "paired_excess_ns": median(rows),
                             "noise_ns": noise(left) + noise(right)}
    gap = output["wall"]["paired_excess_ns"]
    for stat in output.values():
        stat["fraction_of_gap"] = stat["paired_excess_ns"] / gap if gap > 0 else None
        stat["above_noise"] = stat["paired_excess_ns"] > stat["noise_ns"]
    return output


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for arg in ("protocol", "samples", "output"):
        cli.add_argument(arg, type=Path)
    args = cli.parse_args()
    spec = json.loads(args.protocol.read_text())
    cells = session_rows(args.samples, spec["statements"])
    results = [{"statement": case, "configuration": mode, "phase": phase,
                "observations": contrast(cells[case, mode], cells[case, NATIVE], phase)}
               for case in spec["statements"] for mode in SPARK for phase in METRICS]
    with args.output.open("x") as stream:
        stream.write(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
