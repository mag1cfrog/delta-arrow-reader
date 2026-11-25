"""Write the complete Mac baseline and the status of historical open cells."""

import argparse
from collections import Counter
import csv
import gzip
import json
from pathlib import Path
from statistics import median
import sys

sys.dont_write_bytecode = True
from cells import FUNCTIONS, read
from native import HERE, paired, safe_record

FIELDS = ("machine", "suite", "case", "name", "configuration", "owner", "metric", "unit", "reference",
          "reference_median", "spark_median", "paired_difference", "aa_floor", "status", "reason", "session",
          "historical_report", "status_of_historical_gap")


def row(result, data, phase):
    record = {**data, "session": phase, "machine": "Apple Silicon, native macOS"}
    record.update({"reference_median": result.get("before"), "spark_median": result.get("after"),
                   "paired_difference": result.get("delta"), "aa_floor": result.get("floor")})
    if result["status"] == "ok":
        record["status"] = result["decision"]
        record["reason"] = ""
    else:
        record["status"] = result["status"]
        record["reason"] = result.get("reason", "")
    if isinstance(record["reason"], list):
        record["reason"] = ". ".join(record["reason"])
    if record["status"] == "slower" and not record["owner"]:
        record["owner"] = 1560
    return record


def functions(eligibility, sweep, repeat):
    owners = read(HERE / "issue-owners.json")["function_cases"]
    cases = {case["id"]: case for case in read(FUNCTIONS)["functions"]}
    eligible = {case["id"]: case for case in eligibility["results"]}
    assert len(cases) == 633 and cases.keys() == eligible.keys()
    rows = []
    for case, item in cases.items():
        e = eligible[case]
        for mode in ("spark-off", "spark-on"):
            key = f"{case}/{mode}"
            initial = sweep.get(key)
            replay = repeat.get(key)
            if initial is None:
                assert e["status"] != "eligible"
            else:
                assert e["status"] == "eligible"
            result = replay or initial or e
            if result["status"] == "ok" and result["decision"] == "slower":
                assert replay, f"missing required repeat: {key}"
            reference = "native" if e["class"] == "paired" else "projection"
            rows.append(row(result, {"suite": "functions", "case": case, "name": item["name"], "configuration": mode,
                                     "owner": owners.get(case), "metric": "execution minus projection",
                                     "unit": "ns/input row", "reference": reference},
                            "function-repeat" if replay else "function-sweep" if initial else "preflight"))
    return rows


def statements(results, raw):
    specs = read(HERE / "baseline-cells.json")["cells"]
    assert len(results) == len(specs)
    rows = []
    for spec in specs:
        suite = spec["suite"]
        metric = spec.get("metric", {"functions": "execution minus projection", "nullif_620": "execution_ns"}
                          .get(suite, "query_complete_ns"))
        reference = spec.get("reference_id", spec["reference"])
        result = row(results[spec["label"]], {"suite": suite, "case": spec["id"], "name": spec["id"],
                     "configuration": spec["configuration"], "owner": spec["issue"], "metric": metric,
                     "unit": "ns/input row" if suite == "functions" else "ns/statement", "reference": reference}, "statements")
        if suite == "functions" and result["status"] not in ("untimable", "semantic_mismatch"):
            result.update(status="spark-only", reference="projection", reason="No equivalent native statement")
        elif "reference_id" in spec or spec["reference"] == spec["configuration"]:
            if result["status"] in ("slower", "faster", "within floor"):
                result.update(status="projection control", reason="Same-ANSI projection or repeated control")
        elif spec["configuration"] == "native-plain":
            result["status"] = f"native control ({result['status']})"
        rows.append(result)
    rows.extend(planning_phases(raw, rows))
    return rows


def planning_phases(raw, primary):
    phases = ("logical_ns", "physical_ns", "total_ns")
    selected = {f"planning/{r['case']}/{r['configuration']}": r for r in primary
                if r["suite"] == "planning_attribution"}
    arms = {}
    with gzip.open(raw, "rt") as stream:
        for line in stream:
            item = json.loads(line)
            key = item.get("label")
            if key not in selected or item["status"] != "ok":
                continue
            record, = item["records"]
            arm = arms.setdefault(key, {}).setdefault(item["arm"], {})
            assert item["block"] not in arm
            arm[item["block"]] = {phase: median(record[phase]) for phase in phases}
    additions = []
    for key, existing in selected.items():
        sample = arms.get(key, {})
        for phase in phases:
            if phase == existing["metric"]:
                continue
            spec = {field: existing[field] for field in ("suite", "case", "name", "configuration", "owner", "unit", "reference")}
            spec["metric"] = phase
            if set(sample) != {"before", "after", "a", "b"} or any(len(arm) != 9 for arm in sample.values()):
                measurement = {"status": "untimable", "reason": existing["reason"]}
            else:
                values = {arm: [blocks[b][phase] for b in range(1, 10)] for arm, blocks in sample.items()}
                measurement = {"status": "ok", **paired(values, 9)}
            result = row(measurement, spec, "statements")
            if spec["configuration"] == "native-plain":
                result["status"] = f"native control ({result['status']})"
            additions.append(result)
    for mode in ("spark-off", "spark-on"):
        keys = [key for key in selected if key.startswith("planning/paired/") and key.endswith(f"/{mode}")]
        spec = {"suite": "planning_attribution", "owner": 1390, "metric": "logical_ns", "unit": "ns/statement",
                "reference": "native-configured", "name": "Median of 25 paired statements",
                "case": "paired/logical_median", "configuration": mode}
        needed = ("before", "after", "a", "b")
        if len(keys) != 25 or any(any(len(arms.get(key, {}).get(arm, {})) != 9 for arm in needed) for key in keys):
            measurement = {"status": "untimable", "reason": "The logical aggregate requires all 25 paired statements"}
        else:
            values = {arm: [median(arms[key][arm][i]["logical_ns"] for key in keys) for i in range(1, 10)]
                      for arm in needed}
            measurement = {"status": "ok", **paired(values, 9)}
        additions.append(row(measurement, spec, "statements"))
    return additions


def historical_gaps(rows):
    historical = read(HERE / "historical-gaps.json")["gaps"]
    coverage = Counter()
    for gap in historical:
        for mode in ([gap["configuration"]] if gap.get("configuration") else ["spark-off", "spark-on"]):
            matches = [r for r in rows if (r["suite"], r["case"], r["configuration"]) ==
                       (gap["suite"], gap["case"], mode) and (not gap.get("metric") or r["metric"] == gap["metric"])]
            match, = matches
            match["historical_report"] = gap["source"]
            if reason := gap.get("reference"):
                decision = f"{reason}. The Mac row records the corrected cost."
            elif match["paired_difference"] is None:
                decision = f"Untimable: {match['reason']}"
            else:
                delta, floor = match["paired_difference"], match["aa_floor"]
                decision = ("slower" if delta > floor else "faster" if delta < -floor else
                            "not resolvable on Apple Silicon" if abs(delta) < floor else "within floor")
            match["status_of_historical_gap"] = decision
            coverage[gap["issue"]] += 1
    return coverage


def noise_table(noise):
    def number(value):
        return "-" if value is None else f"{value:.6g}"

    output = []
    for case, item in noise["first"].items():
        repeated = noise["rerun"].get(case)
        if item["status"] != "ok":
            output.append(f"| {case} | - | - | - | Untimable |")
        else:
            floor = repeated["floor"] if repeated and repeated["status"] == "ok" else None
            fraction = (repeated or item).get("floor_fraction")
            ratio = f"{fraction * 100:.3f}%" if fraction is not None else "-"
            output.append(f"| {case} | {number(item['first_three_blocks']['floor'])} | {number(item['floor'])} | "
                          f"{number(floor)} | {ratio} |")
    return "\n".join(output)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for name in ("function_eligibility", "noise", "sweep", "repeat", "statements", "statement_samples", "fixture", "self_checks", "output"):
        cli.add_argument(name, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    assert not args.output.exists()
    args.output.mkdir()
    n = read(args.noise)
    fixture = read(args.fixture)
    checks = read(args.self_checks)
    rows = (functions(read(args.function_eligibility), read(args.sweep), read(args.repeat))
            + statements(read(args.statements), args.statement_samples))
    rows = safe_record(rows, ())
    coverage = historical_gaps(rows)
    table = args.output / "macos-baseline-1560.csv"
    with table.open("x", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    timings = Counter(row["status"] for row in rows)
    cases = Counter()
    for row in rows:
        if row["status"] == "slower" and row["configuration"].startswith("spark-"):
            cases[row["owner"]] += 1
    groups = "\n".join(f"| #{issue} | {count} |" for issue, count in sorted(cases.items()))
    history = "\n".join(f"| #{issue} | {count} |" for issue, count in sorted(coverage.items()))
    passed = sum(row["status"] == "passed" for row in checks["checks"].values())
    bands = ", ".join(f"{suite} {count}" for suite, count in sorted(n["samples"].items()))
    proof = "byte-identical Parquet files and Delta logs" if fixture["snapshot_matches"] else "decoded equality"
    assert fixture["snapshot_matches"] or fixture.get("content_matches")
    report = f"""# SQL baseline on Apple Silicon (#1560)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Source `{checks['source']}`. [Committed protocol](../macos_baseline/protocol.md).

The adapter passed {passed} of {len(checks['checks'])} end-to-end self-checks.
The verified SF1 snapshot has {fixture['rows']:,} rows and {fixture['snapshot_columns']}
columns, with {proof}. [Fixture proof](macos-baseline-1560-fixture.json).
The host-specific generator executable and preparation manifest have different
hashes. A full export through the stock CLI matched every decoded row. The
export proof records the Parquet and executed Arrow types for all 81 columns.
[Stock CLI export proof](macos-baseline-1560-stock-export.json).

## A/A floors

**Apple Silicon, native macOS.** Function values are ns per input row above
projection. Planning, SF1 and NULLIF values are ns per statement.

| Cell | First 3 blocks | 9 blocks, 3 samples | 9 blocks, 9 samples | Final floor / cost |
| --- | ---: | ---: | ---: | ---: |
{noise_table(n)}

Nine blocks are fixed. Retained samples per process are {bands}.

## Baseline

**Apple Silicon, native macOS.** The [baseline CSV](macos-baseline-1560.csv) has
both ANSI rows for all 633 function entries and the planning, parser, string,
and NULLIF statement cells. It includes the exact reason for each exclusion
and each incomplete measurement. Cases with the same physical functions and
complete typed results retain their verified inventory status. Spark-only
costs keep their projection reference.

| Owner | Spark cells slower than their native counterpart beyond the floor |
| --- | ---: |
{groups}

There are {timings['slower']} above-floor rows and {timings['untimable']} untimable
rows in the table. #1560 owns the above-floor rows without another open issue.

Each x86 difference stays open for confirmation. The historical comparison
uses statements with the same inputs and references. Mac comparisons that
fall under their A/A floor are not resolvable on Apple Silicon.

**Apple Silicon, native macOS.** The CSV records a Mac status for each
historical gap and links it by report filename. The #1513 correction rows
carry the corrected Spark cost because the prior implementation returned
different results. #1414's Spark-only functions have projection costs.

| Issue | Historical cells with a Mac status or exclusion |
| --- | ---: |
{history}

[Noise samples](macos-baseline-1560-noise.jsonl.gz),
[function sweep samples](macos-baseline-1560-functions.jsonl.gz),
[function repeat samples](macos-baseline-1560-repeat.jsonl.gz), and
[planning, parser, string and NULLIF samples](macos-baseline-1560-statements.jsonl.gz).
"""
    (args.output / "macos-baseline-1560.md").write_text(report)


if __name__ == "__main__":
    main()
