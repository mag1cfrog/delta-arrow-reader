#!/usr/bin/env python3
"""Summarize the frozen planning schedule without changing thresholds or samples."""

import csv
import gzip
import json
import pathlib
import statistics
import sys
from collections import defaultdict

HERE = pathlib.Path(__file__).resolve().parent
PHASES = ("logical", "physical", "total")


def stats(values):
    quartiles = statistics.quantiles(values, n=4, method="inclusive")
    return {
        "median": statistics.median(values) / 1e6,
        "min": min(values) / 1e6,
        "max": max(values) / 1e6,
        "iqr": (quartiles[2] - quartiles[0]) / 1e6,
    }


def aggregate(records):
    groups = defaultdict(list)
    identities = set()
    for row in records:
        identity = (row["series"], row.get("arm"), row["configuration"], row["process"], row["id"])
        assert identity not in identities, identity
        identities.add(identity)
        assert row["total_ns"] == [a + b for a, b in zip(row["logical_ns"], row["physical_ns"], strict=True)]
        for phase in PHASES:
            groups[(row["series"], row.get("arm"), row["configuration"], row["id"], phase)].append(statistics.median(row[phase + "_ns"]))
    assert all(len(values) == 5 for values in groups.values())
    return {key: stats(values) for key, values in groups.items()}


def self_check():
    # A total median must use paired iterations, not the sum of phase medians.
    row = {"series":"main", "arm":None, "configuration":"test", "id":"case", "logical_ns":[1, 1, 100], "physical_ns":[1, 100, 1], "total_ns":[2, 101, 101]}
    records = [dict(row, process=str(i)) for i in range(5)]
    values = aggregate(records)
    assert values[("main",None,"test","case","total")]["median"] == 101 / 1e6
    assert values[("main",None,"test","case","logical")]["median"] + values[("main",None,"test","case","physical")]["median"] == 2 / 1e6
    for invalid in (records[:-1], records + [records[0]]):
        try:
            aggregate(invalid)
        except AssertionError:
            pass
        else:
            raise AssertionError("missing or duplicate process samples were accepted")


def summarize(run_directory, output_directory):
    run_directory, output_directory = map(pathlib.Path, (run_directory, output_directory))
    assert (run_directory / "complete.json").exists(), "incomplete schedule"
    with gzip.open(run_directory / "samples.jsonl.gz", "rt") as samples:
        records = [json.loads(line) for line in samples]
    manifest = json.loads((HERE / "queries.json").read_text())
    queries = {query["id"]: query for query in manifest["queries"]}
    eligibility = [json.loads(line) for line in (HERE / "eligibility.jsonl").read_text().splitlines()]
    schedule = json.loads((HERE / "schedule.json").read_text())
    assert all(len(row[phase + "_ns"]) == schedule["samples"] for row in records for phase in PHASES), "sample count differs"
    expected_processes = {(item["series"], item.get("arm"), item["configuration"], item["process"]) for item in schedule["runs"]}
    actual_processes = {(row["series"], row.get("arm"), row["configuration"], row["process"]) for row in records}
    assert actual_processes == expected_processes, "scheduled process coverage differs"
    data = aggregate(records)
    configurations = ("native-plain", "native-configured", "spark-off", "spark-on")
    for configuration in configurations:
        expected = {query["id"] for query in queries.values() if query["group"] == "paired" or (query["group"] == "stress" and configuration != "native-plain")}
        if configuration.startswith("spark-"):
            expected.update(row["id"] for row in eligibility if row["configuration"] == configuration and row["status"] == "included")
        actual = {key[3] for key in data if key[:3] == ("main",None,configuration)}
        assert actual == expected, (configuration, expected - actual, actual - expected)
    def median(configuration, query, phase="total"):
        return data[("main",None,configuration,query,phase)]["median"]
    main_rows = []
    for configuration in configurations:
        for query in queries:
            if ("main",None,configuration,query,"total") not in data:
                continue
            row = {"id":query,"group":queries[query]["group"],"configuration":configuration}
            for phase in PHASES:
                row.update({phase+"_"+name+"_ms":value for name,value in data[("main",None,configuration,query,phase)].items()})
            main_rows.append(row)
    noise_rows = []
    for query in queries:
        if queries[query]["group"] == "sweep":
            continue
        for phase in PHASES:
            left = data[("noise","a","native-configured",query,phase)]
            right = data[("noise","b","native-configured",query,phase)]
            noise_rows.append({"id":query,"phase":phase,"a_median_ms":left["median"],"b_median_ms":right["median"],"absolute_difference_ms":abs(left["median"]-right["median"]),"b_over_a":right["median"]/left["median"],"a_min_ms":left["min"],"a_max_ms":left["max"],"b_min_ms":right["min"],"b_max_ms":right["max"]})
    outliers = {"spark_over_native_configured":[],"spark_above_20_ms":[],"native_configured_over_plain":[]}
    for query, definition in queries.items():
        if definition["group"] == "paired":
            baseline = median("native-configured",query)
            plain = median("native-plain",query)
            if baseline > plain * 1.1 and baseline - plain > 0.1:
                outliers["native_configured_over_plain"].append({"id":query,"native_plain_ms":plain,"native_configured_ms":baseline,"ratio":baseline/plain,"difference_ms":baseline-plain})
        for configuration in ("spark-off","spark-on"):
            if ("main",None,configuration,query,"total") not in data:
                continue
            value = median(configuration,query)
            if definition["group"] == "paired":
                baseline = median("native-configured",query)
                if value > 3 * baseline and value - baseline > 2:
                    outliers["spark_over_native_configured"].append({"id":query,"configuration":configuration,"spark_ms":value,"native_configured_ms":baseline,"ratio":value/baseline,"difference_ms":value-baseline})
            elif value > 20:
                outliers["spark_above_20_ms"].append({"id":query,"configuration":configuration,"total_ms":value})
    slowest = []
    for query in queries:
        available = [configuration for configuration in ("spark-off","spark-on") if ("main",None,configuration,query,"total") in data]
        if not available:
            continue
        configuration = max(available,key=lambda name:median(name,query))
        phase = max(("logical","physical"),key=lambda name:median(configuration,query,name))
        slowest.append({"id":query,"configuration":configuration,"phase":phase,"phase_ms":median(configuration,query,phase),"total_ms":median(configuration,query)})
    slowest.sort(key=lambda row:(-row["total_ms"],row["id"]))
    output_directory.mkdir(parents=True,exist_ok=True)
    for name, rows in (("planning-1379-results.csv",main_rows),("planning-1379-noise.csv",noise_rows)):
        with (output_directory / name).open("w",newline="") as output:
            writer = csv.DictWriter(output,fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    summary = {"outliers":outliers,"slowest":slowest[:5],"exclusions":[row for row in eligibility if row["status"]=="excluded"],"noise":{}}
    for phase in PHASES:
        differences = sorted(row["absolute_difference_ms"] for row in noise_rows if row["phase"] == phase)
        summary["noise"][phase] = {"median_absolute_difference_ms":statistics.median(differences),"p95_absolute_difference_ms":statistics.quantiles(differences,n=20,method="inclusive")[18],"max_absolute_difference_ms":max(differences)}
    (output_directory / "planning-1379-summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps({"query_configurations":len(main_rows),"noise_cases":len(noise_rows),"outliers":{key:len(value) for key,value in outliers.items()},"slowest":slowest[:5]},indent=2))


if __name__ == "__main__":
    self_check()
    if sys.argv[1:] == ["--check"]:
        print("planning aggregation check passed")
    elif len(sys.argv) == 3:
        summarize(*sys.argv[1:])
    else:
        raise SystemExit("usage: summarize.py --check | RUN_DIRECTORY OUTPUT_DIRECTORY")
