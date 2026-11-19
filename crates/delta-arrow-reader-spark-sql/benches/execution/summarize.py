#!/usr/bin/env python3
"""Summarize the fixed SF1 execution records without selecting new measurements."""

import collections
import csv
import gzip
import json
import pathlib
import statistics
import sys

SUITE = pathlib.Path(__file__).resolve().parent
CAST_SUITE_SELECTED = False
TIMINGS = ("planning_ns", "execution_first_batch_ns", "query_first_batch_ns", "execution_complete_ns", "query_complete_ns")
COUNTERS = ("rows", "peak_pool_bytes", "output_partitions")
IO = ("source_file_count", "source_bytes_received", "recorded_range_bytes", "source_range_gets", "source_full_gets", "file_tasks_started", "file_tasks_completed")


def spread(values):
    available = [value for value in values if value is not None]
    if not available:
        return dict(median=None, minimum=None, maximum=None, iqr=None, processes_with_value=0)
    quartiles = statistics.quantiles(available,n=4,method="inclusive") if len(available)>1 else [available[0]]*3
    return dict(median=statistics.median(available),minimum=min(available),maximum=max(available),iqr=quartiles[2]-quartiles[0],processes_with_value=len(available))


def aggregate(records):
    groups = collections.defaultdict(list)
    identities = set()
    for record in records:
        identity = (record["series"],record.get("arm"),record["configuration"],record["id"],record["process"])
        assert identity not in identities, "duplicate process result"
        identities.add(identity)
        assert len(record["samples"]) == 3
        for metric in TIMINGS+COUNTERS+IO+("process_peak_rss_bytes","setup_rss_bytes"):
            if metric in ("process_peak_rss_bytes","setup_rss_bytes"):
                value = record[metric]
            else:
                values = [(sample["source_io"][metric] if metric in IO else sample[metric]) for sample in record["samples"]]
                available = [value for value in values if value is not None]
                value = statistics.median(available) if available else None
            groups[identity[:-1]+(metric,)].append(value)
    assert all(len(values)==3 for values in groups.values()), "missing process result"
    return {key:spread(values) for key,values in groups.items()}


def outlier_flags(native_time, spark_time, native_memory, spark_memory):
    return (spark_time > 1.25*native_time and spark_time-native_time > 50_000_000,
            spark_memory > 1.25*native_memory and spark_memory-native_memory > 16*1024**2)


def self_check():
    samples = []
    for planning,execution in zip((1,1,100),(1,100,1),strict=True):
        samples.append({"planning_ns":planning,"execution_first_batch_ns":1,"query_first_batch_ns":planning+1,"execution_complete_ns":execution,"query_complete_ns":planning+execution,"rows":1,"peak_pool_bytes":0,"output_partitions":1,"source_io":{key:0 for key in IO}})
    records = [{"series":"main","configuration":"native","id":"check","process":str(index),"process_peak_rss_bytes":100,"setup_rss_bytes":100,"samples":samples} for index in range(3)]
    values = aggregate(records)
    assert values[("main",None,"native","check","query_complete_ns")]["median"] == 101
    assert values[("main",None,"native","check","planning_ns")]["median"] + values[("main",None,"native","check","execution_complete_ns")]["median"] == 2
    empty = [dict(record,samples=[dict(sample,execution_first_batch_ns=None,query_first_batch_ns=None,rows=0) for sample in record["samples"]]) for record in records]
    missing_batch = aggregate(empty)[("main",None,"native","check","execution_first_batch_ns")]
    assert missing_batch["median"] is None and missing_batch["processes_with_value"] == 0
    assert outlier_flags(400_000_000,500_000_000,0,0) == (False,False)
    assert outlier_flags(100_000_000,150_000_000,0,0) == (False,False)
    assert outlier_flags(200_000_000,250_000_001,0,0) == (True,False)
    assert outlier_flags(1,1,0,32*1024**2) == (False,True)
    assert outlier_flags(1,1,0,8*1024**2) == (False,False)
    for invalid in (records[:-1],records+[records[0]]):
        try:
            aggregate(invalid)
        except AssertionError:
            pass
        else:
            raise AssertionError("missing/duplicate process was accepted")


def summarize(run_directory, output_directory):
    run_directory,output_directory=map(pathlib.Path,(run_directory,output_directory))
    assert (run_directory/"complete.json").exists(), "schedule incomplete"
    with gzip.open(run_directory/"samples.jsonl.gz","rt") as source:
        all_records=[json.loads(line) for line in source]
    records=[record for record in all_records if record["kind"]=="query_samples"]
    concurrency=[record for record in all_records if record["kind"]=="concurrency"]
    schedule=json.loads((SUITE/"schedule.json").read_text())
    assert {record["process"] for record in records} == {run["process"] for run in schedule["runs"]}
    assert len(records)==len(schedule["runs"]) and len(concurrency)==len(schedule["concurrency"])
    if CAST_SUITE_SELECTED:
        assert len(records)==45 and concurrency==[]
    assert {record["process"] for record in concurrency} == {case["process"] for case in schedule["concurrency"]}
    data=aggregate(records)
    query_ids=[query["id"] for query in json.loads((SUITE/"queries.json").read_text())["queries"]]
    configurations=("native","spark-off","spark-on")
    def value(configuration,query,metric):
        return data[("main",None,configuration,query,metric)]["median"]
    table=[]
    for query in query_ids:
        for configuration in configurations:
            for metric in TIMINGS+COUNTERS+IO+("process_peak_rss_bytes","setup_rss_bytes"):
                unit="ns" if metric in TIMINGS else "bytes" if metric.endswith("bytes") or metric=="source_bytes_received" else "count"
                table.append(dict(id=query,configuration=configuration,metric=metric,unit=unit,**data[("main",None,configuration,query,metric)]))
    noise=[]
    for query in query_ids:
        for metric in TIMINGS+COUNTERS+IO+("process_peak_rss_bytes","setup_rss_bytes"):
            a=data[("noise","a","native",query,metric)]
            b=data[("noise","b","native",query,metric)]
            difference=abs(a["median"]-b["median"]) if a["median"] is not None and b["median"] is not None else None
            ratio=b["median"]/a["median"] if a["median"] else None
            noise.append(dict(id=query,metric=metric,a_median=a["median"],b_median=b["median"],absolute_difference=difference,b_over_a=ratio,a_minimum=a["minimum"],a_maximum=a["maximum"],b_minimum=b["minimum"],b_maximum=b["maximum"]))
    outliers=[]
    for query in query_ids:
        for configuration in configurations[1:]:
            native_time,spark_time=value("native",query,"query_complete_ns"),value(configuration,query,"query_complete_ns")
            native_memory,spark_memory=value("native",query,"peak_pool_bytes"),value(configuration,query,"peak_pool_bytes")
            time_flag,memory_flag=outlier_flags(native_time,spark_time,native_memory,spark_memory)
            if time_flag or memory_flag:
                outliers.append(dict(id=query,configuration=configuration,completion_flag=time_flag,pool_flag=memory_flag,native_query_complete_ns=native_time,spark_query_complete_ns=spark_time,native_peak_pool_bytes=native_memory,spark_peak_pool_bytes=spark_memory))
    related={query:{configuration:{"query_complete_ns":value(configuration,query,"query_complete_ns"),"peak_pool_bytes":value(configuration,query,"peak_pool_bytes"),"above_threshold":any(case["id"]==query and case["configuration"]==configuration for case in outliers)} for configuration in configurations} for query in (() if CAST_SUITE_SELECTED else ("nullif_nullable","in_10","in_1000","mixed_boolean"))}
    output_directory.mkdir(parents=True,exist_ok=True)
    prefix="casts-1385" if CAST_SUITE_SELECTED else "execution-1380"
    for name,rows in ((prefix+"-results.csv",table),(prefix+"-noise.csv",noise)):
        with (output_directory/name).open("w",newline="") as output:
            writer=csv.DictWriter(output,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    summary={"query_configurations":len(query_ids)*3,"process_records":len(records),"concurrency":concurrency,"outliers":outliers,"related_issue_shapes":related,"profile_cases":[] if CAST_SUITE_SELECTED else [{"id":case["id"],"configuration":case["configuration"]} for case in outliers]}
    (output_directory/(prefix+"-summary.json")).write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps({"query_configurations":len(query_ids)*3,"metric_rows":len(table),"noise_rows":len(noise),"outliers":len(outliers),"concurrency_processes":len(concurrency)},indent=2))


if __name__ == "__main__":
    if sys.argv[1:2] == ["--casts-1385"]:
        CAST_SUITE_SELECTED = True
        SUITE = SUITE.parent / "casts"
        del sys.argv[1]
    self_check()
    if sys.argv[1:]==["--check"]:
        print("execution aggregation checks passed")
    elif len(sys.argv)==3:
        summarize(*sys.argv[1:])
    else:
        raise SystemExit("usage: summarize.py [--casts-1385] --check | RUN_DIRECTORY OUTPUT_DIRECTORY")
