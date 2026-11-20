#!/usr/bin/env python3
"""Untimed eligibility, frozen fresh-process schedule, and signed cost summaries."""

import argparse
import collections
import csv
import gzip
import hashlib
import json
import os
import pathlib
import platform
import resource
import statistics
import subprocess
import sys
import time
import tarfile

sys.dont_write_bytecode = True
import inventory

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[3]
REPORTS = HERE.parent / "reports"
MANIFEST = HERE / "manifest.json"
RECEIPT = HERE / "preflight-receipt.json"
CONFIGS = ("native", "spark-off", "spark-on")


def read(path):
    return json.loads(pathlib.Path(path).read_text())


def digest(path):
    with pathlib.Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def sources():
    paths = set(HERE.glob("*.py")) | {MANIFEST, HERE / "schedule.json", HERE / "native-registry.json", HERE.parent / "functions.rs", HERE.parent.parent / "Cargo.toml", REPORTS / "functions-1391-protocol.md"}
    for row in read(MANIFEST)["functions"]:
        if row.get("anchor"):
            paths.add(ROOT/row["anchor"].rsplit(":",1)[0])
        for anchor in row.get("implementation_anchors", []):
            paths.add(ROOT / anchor.rsplit(":",1)[0])
    paths.update({inventory.FUNCTION/"mod.rs",inventory.FUNCTION/"scalar/mod.rs",HERE.parent.parent/"tests/support/spark_results.rs",HERE.parent.parent/"tests/support/c06_receipts.rs",inventory.FUNCTION.parents[4]/"dependencies/datafusion-functions/src/macros.rs",inventory.FUNCTION.parents[4]/"dependencies/datafusion-physical-expr/src/scalar_function.rs"})
    return {str(path.relative_to(ROOT)): digest(path) for path in sorted(paths)}


def limit(bound):
    def apply():
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        ceiling = bound if hard == resource.RLIM_INFINITY else min(bound, hard)
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
    return apply


def parse_lines(data, interrupted=False):
    lines = data.splitlines()
    records = []
    for index,line in enumerate(lines):
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            if interrupted and index == len(lines)-1 and not data.endswith("\n"):
                break
            raise
    return records


def invoke(binary, args, work, schedule, deadline, preflight=False):
    work.mkdir(parents=True, exist_ok=False)
    timeout = min(deadline-time.monotonic(), schedule["preflight_process_timeout_seconds"] if preflight else schedule["process_timeout_seconds"])
    if timeout <= 0:
        return {"status":"schedule_timeout","error":"fixed overall schedule deadline exhausted","records":[]}
    command = ["taskset", "-c", ",".join(map(str,schedule["cpus"])), str(binary), *map(str,args)]
    with (work/"stdout.jsonl").open("x") as output, (work/"stderr.log").open("x") as error:
        try:
            process = subprocess.run(command, stdout=output, stderr=error, timeout=timeout, preexec_fn=limit(schedule["address_space_bytes"]))
        except subprocess.TimeoutExpired:
            return {"status":"process_timeout","command":command,"evidence":str(work),"records":parse_lines((work/"stdout.jsonl").read_text(),True)}
    records = parse_lines((work/"stdout.jsonl").read_text(),process.returncode != 0)
    if process.returncode:
        return {"status":"process_error","returncode":process.returncode,"command":command,"error":(work/"stderr.log").read_text()[-3000:],"evidence":str(work),"records":records}
    assert records, work
    return {"status":"ok","records":records,"command":command}


def eligibility(case, receipt):
    if case["exclusion"]:
        return {"class":case["class"],"status":"excluded","reason":case["exclusion"]["kind"]+": "+case["exclusion"]["reason"]}
    if receipt.get("status") != "ok":
        return {"class":case["class"],"status":"setup_failure","reason":receipt.get("status","missing_preflight")+": "+receipt.get("error","")}
    results = {row["configuration"]:row for row in receipt["results"]}
    required = CONFIGS if case["native_sql"] else CONFIGS[1:]
    failed = [config+": "+results.get(config,{}).get("status","missing")+": "+results.get(config,{}).get("error","") for config in required if results.get(config,{}).get("status") != "ok"]
    if failed:
        return {"class":case["class"],"status":"setup_failure","reason":"; ".join(failed)}
    assert all(results[config].get("source_guard_rows") == 1_000_000 for config in required), "missing input consumption evidence"
    if case["native_sql"]:
        differences = [config for config in CONFIGS[1:] if not results[config].get("comparison",{}).get("equal")]
        if differences:
            return {"class":case["class"],"status":"semantic_mismatch","reason":"full rows or exact executed types differ: "+", ".join(differences)}
        if case.get("kind") != "shape" and all(results[config].get("same_physical_function") for config in CONFIGS[1:]):
            return {"class":"same","status":"same_verified","reason":"Same SQL, actual physical UDF identities/arguments, complete rows and executed types in both ANSI modes."}
    return {"class":case["class"],"status":"eligible","reason":"Both ANSI modes completed full-input preflight"}


def self_check():
    case = {"class":"paired","exclusion":None,"native_sql":"SELECT i"}
    good = {"status":"ok","results":[{"configuration":c,"status":"ok","source_guard_rows":1_000_000,"comparison":{"equal":True}} for c in CONFIGS]}
    assert eligibility(case,good)["status"] == "eligible"
    for config in CONFIGS:
        failed = json.loads(json.dumps(good))
        next(row for row in failed["results"] if row["configuration"] == config)["status"] = "execution_error"
        assert eligibility(case,failed)["status"] == "setup_failure"
    different = json.loads(json.dumps(good))
    different["results"][1]["comparison"]["equal"] = False
    assert eligibility(case,different)["status"] == "semantic_mismatch"
    assert eligibility(case,{"status":"process_timeout"})["status"] == "setup_failure"
    assert noise([1,1,1],[2,2,2]) == 1
    assert noise([-1,0,1],[-1,0,1]) == 1
    assert signed_stats([-3,-2,-1])["median"] == -2
    assert parse_lines('{"a":1}\n{"a":',True) == [{"a":1}]
    for invalid,interrupted in [('{"a":1}\n{"a":',False),('{"a":1}\n{"a":\n',True),('{"a":\n{"b":1}',True)]:
        try: parse_lines(invalid,interrupted)
        except json.JSONDecodeError: pass
        else: raise AssertionError("invalid complete/interior JSONL record accepted")


def preflight(binary, destination, manifest, schedule, build_receipt):
    assert not git("status","--porcelain"), "commit protocol source before final preflight"
    build = read(build_receipt)
    assert build["source_commit"] == git("rev-parse","HEAD") and build["binary_sha256"] == digest(binary)
    assert build["profile"] == "release" and build["target"] == "functions" and "--release" in build["command"] and not build.get("instrumented",False)
    assert not destination.exists(), "preflight destination must be new"
    destination.mkdir(parents=True)
    deadline = time.monotonic()+schedule["preflight_timeout_seconds"]
    registry = invoke(binary,["registry"],destination/"registry",schedule,deadline,True)
    assert registry["status"] == "ok" and registry["records"] == [read(HERE/"native-registry.json")], "runtime registry drift"
    checked = invoke(binary,["check"],destination/"self-check",schedule,deadline,True)
    assert checked["status"] == "ok" and checked["records"] == [{"status":"passed"}]
    fixture = invoke(binary,["fixture"],destination/"fixture",schedule,deadline,True)
    assert fixture["status"] == "ok" and fixture["records"][0]["rows"] == 1_000_000
    assert all(column["nulls"] == (0 if column["name"] in ("id","grp","high_key") else 62500) for column in fixture["records"][0]["columns"])
    results = []
    for index, case in enumerate(manifest["functions"]):
        if case["exclusion"]:
            result = {"status":"not_executed"}
        else:
            print(f"preflight {index+1}/{len(manifest['functions'])} {case['id']}",flush=True)
            work = destination/case["id"]
            output = invoke(binary,["preflight",MANIFEST,case["id"],work/"plans"],work,schedule,deadline,True)
            result = dict(output["records"][0],status="ok") if output["status"] == "ok" else output
            if result["status"] == "ok":
                result["plan_hashes"] = {str(path.relative_to(destination)):digest(path) for path in sorted((work/"plans").glob("*.plan"))}
        classified = eligibility(case,result)
        results.append(dict(id=case["id"],**classified,preflight=result))
    metadata = {"version":1,"source_commit":git("rev-parse","HEAD"),"baseline_commit":manifest["source_commit"],"binary_sha256":digest(binary),"source_hashes":sources(),"registry":registry["records"][0],"fixture":fixture["records"][0],"preflight_evidence":str(destination),"schedule":schedule,"results":results}
    (destination/"preflight.json").write_text(json.dumps(metadata,indent=2)+"\n")
    # The committed receipt carries classes and proofs, never full output rows.
    RECEIPT.write_text(json.dumps(metadata,indent=2)+"\n")
    print(json.dumps({"receipt":str(RECEIPT),"counts":dict(collections.Counter(row["status"] for row in results))}))


def shards(ids, replica, size):
    if not ids:
        return []
    offset = replica*(len(ids)//3)
    ordered = ids[offset:]+ids[:offset]
    return [part if index%2 == 0 else list(reversed(part)) for index,start in enumerate(range(0,len(ordered),size)) for part in [ordered[start:start+size]]]


def measure(binary, destination, frozen_commit, build_receipt, manifest, schedule):
    assert platform.system() == "Linux", "Linux worker required"
    assert git("rev-parse","HEAD") == frozen_commit and len(frozen_commit) == 40
    assert not git("status","--porcelain"), "commit the protocol and full preflight receipt before clocks"
    assert git("show",f"{frozen_commit}:{RECEIPT.relative_to(ROOT)}") == RECEIPT.read_text().strip(), "receipt is not frozen in the exact commit"
    receipt = read(RECEIPT)
    assert receipt["source_hashes"] == sources(), "preflight source/statement drift"
    assert receipt["binary_sha256"] == digest(binary), "preflight release binary drift"
    build = read(build_receipt)
    assert build["source_commit"] == receipt["source_commit"] and build["binary_sha256"] == digest(binary)
    subprocess.run(["git","merge-base","--is-ancestor",build["source_commit"],frozen_commit],cwd=ROOT,check=True)
    assert set(git("diff","--name-only",build["source_commit"],frozen_commit).splitlines()) <= {str(RECEIPT.relative_to(ROOT))}, "only eligibility receipt may change after the release build commit"
    assert "--release" in build["command"] and build["profile"] == "release" and build["target"] == "functions" and not build.get("instrumented",False), "one instrument-free release binary required"
    assert not destination.exists(), "once-only destination must be new"
    destination.mkdir(parents=True)
    selected = {row["id"]:row for row in receipt["results"] if row["status"] == "eligible"}
    ids = sorted(selected)
    deadline = time.monotonic()+schedule["schedule_timeout_seconds"]
    metadata = {"frozen_commit":frozen_commit,"binary_sha256":digest(binary),"preflight_receipt_sha256":digest(RECEIPT),"build":build,"schedule":schedule,"uname":platform.uname()._asdict(),"cpuinfo":pathlib.Path("/proc/cpuinfo").read_text(),"affinity":sorted(os.sched_getaffinity(0)),"governors":{str(cpu):pathlib.Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor").read_text().strip() for cpu in schedule["cpus"]},"eligible_ids":ids}
    (destination/"metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
    process_number = 0
    spawned_processes = 0
    for series in ("noise","main"):
        for replica in range(3):
            orders = schedule["noise_orders"][replica] if series == "noise" else schedule["main_orders"][replica]
            for shard_number, shard in enumerate(shards(ids,replica,schedule["shard_size"])):
                for arm in orders:
                    configuration = "native" if series == "noise" else arm
                    chosen = [id for id in shard if configuration != "native" or series == "noise" or selected[id]["class"] == "paired"]
                    if not chosen:
                        continue
                    process_number += 1
                    process_id = f"{process_number:04}-{series}-{replica+1}-{shard_number+1}-{arm}"
                    idfile = destination/(process_id+".json")
                    idfile.write_text(json.dumps(chosen)+"\n")
                    print(process_id,len(chosen),flush=True)
                    result = invoke(binary,["measure-set",MANIFEST,idfile,configuration],destination/process_id,schedule,deadline)
                    spawned_processes += result["status"] != "schedule_timeout"
                    observed = {row["id"]:row for row in result.get("records",[]) if "id" in row}
                    records = []
                    for id in chosen:
                        row = observed.get(id,{"id":id,"configuration":configuration,"status":result["status"] if result["status"] != "ok" else "missing_case","error":result.get("error","case did not finish in the original bounded process")})
                        row.update(process=process_id,series=series,replica=replica+1,arm=arm,shard=shard_number+1)
                        records.append(row)
                        if row.get("status") != "ok":
                            continue
                        expected = selected[row["id"]]["preflight"]
                        reference_config = configuration if selected[row["id"]]["class"] == "paired" or configuration != "native" else None
                        expected_rows = next(result["rows"] for result in expected["results"] if result["configuration"] == reference_config) if reference_config else 1_000_000
                        expected_types = next(result["types"] for result in expected["results"] if result["configuration"] == reference_config) if reference_config else next(result["types"] for result in expected["baselines"] if result["configuration"] == "native")
                        if row["types"] != expected_types or len(row["samples"]) != 3 or not all(sample["input_rows"] == 1_000_000 and sample["output_rows"] == expected_rows and sample["increment_ns"] == sample["execution_ns"]-sample["projection_ns"] for sample in row["samples"]):
                            row.update(status="timed_validation_failure",error="output count/type/sample accounting drifted from committed preflight")
                    (destination/process_id/"verified.json").write_text(json.dumps(records)+"\n")
    (destination/"complete.json").write_text(json.dumps({"scheduled_processes":process_number,"spawned_processes":spawned_processes,"eligible_functions":len(ids),"status":"complete"})+"\n")


def signed_stats(values):
    assert values
    quartiles = statistics.quantiles(values,n=4,method="inclusive") if len(values)>1 else [values[0]]*3
    return {"median":statistics.median(values),"min":min(values),"max":max(values),"iqr":quartiles[2]-quartiles[0]}


def noise(a,b):
    return max(abs(statistics.median(a)-statistics.median(b)),(max(a)-min(a))/2,(max(b)-min(b))/2)


def summarize(destination, reports, manifest):
    assert read(destination/"complete.json")["status"] == "complete", "partial runs must not become accepted timings"
    receipt = read(RECEIPT)
    eligible = {row["id"]:row for row in receipt["results"]}
    records = [row for path in sorted(destination.glob("*/verified.json")) for row in read(path)]
    medians = collections.defaultdict(list)
    execution = collections.defaultdict(list)
    for row in records:
        if row.get("status") != "ok":
            continue
        key = row["id"],row["series"],row["arm"]
        medians[key].append(statistics.median(sample["increment_ns"]/1_000_000 for sample in row["samples"]))
        execution[key].append(statistics.median(sample["execution_ns"] for sample in row["samples"]))
    rows = []
    for case in manifest["functions"]:
        result = eligible[case["id"]]
        if result["status"] == "eligible":
            required = [("noise",arm) for arm in ("a","b")] + [("main",config) for config in (CONFIGS if result["class"] == "paired" else CONFIGS[1:])]
            missing = [f"{series}/{arm}" for series,arm in required if len(medians[case["id"],series,arm]) != 3]
            failures = [row for row in records if row["id"] == case["id"] and row.get("status") != "ok"]
            if missing or failures:
                result = dict(result,status="timing_failure",reason="Incomplete bounded original schedule: "+", ".join(missing)+"; "+"; ".join(row["status"]+": "+row.get("error","") for row in failures))
        for config in CONFIGS[1:]:
            row = {"id":case["id"],"function":case["name"],"kind":case["kind"],"category":case["category"],"configuration":config,"class":result["class"],"status":result["status"],"reason":result["reason"]}
            if result["status"] == "eligible":
                values = medians[case["id"],"main",config]
                assert len(values) == 3
                spread = signed_stats(values)
                floor = noise(medians[case["id"],"noise","a"],medians[case["id"],"noise","b"])
                row.update(ns_per_input_row=spread["median"],process_min_ns=spread["min"],process_max_ns=spread["max"],process_iqr_ns=spread["iqr"],aa_floor_ns=floor,input_rows_per_second=1_000_000*1e9/statistics.median(execution[case["id"],"main",config]))
                if result["class"] == "paired":
                    native = signed_stats(medians[case["id"],"main","native"])
                    effective_floor = max(floor,(spread["max"]-spread["min"])/2,(native["max"]-native["min"])/2)
                    delta = spread["median"]-native["median"]
                    row.update(native_ns_per_input_row=native["median"],excess_ns_per_input_row=delta,noise_floor_ns=effective_floor,slower_than_noise=delta>effective_floor,ratio=spread["median"]/native["median"] if native["median"]>floor else None,ratio_reason="" if native["median"]>floor else "native projection-subtracted cost is nonpositive or within its A/A noise floor")
            rows.append(row)
    reports.mkdir(parents=True,exist_ok=True)
    with (reports/"functions-1391-results.csv").open("w",newline="") as output:
        fields = list(dict.fromkeys(key for row in rows for key in row))
        writer = csv.DictWriter(output,fieldnames=fields,lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)
    with gzip.GzipFile(filename=str(reports/"functions-1391-samples.jsonl.gz"),mode="wb",mtime=0) as compressed:
        for row in records: compressed.write((json.dumps(row,separators=(",",":"))+"\n").encode())
    preflight_evidence = pathlib.Path(receipt["preflight_evidence"])
    with gzip.GzipFile(filename=str(reports/"functions-1391-plans.tar.gz"),mode="wb",mtime=0) as compressed:
        with tarfile.open(fileobj=compressed,mode="w") as archive:
            for case in receipt["results"]:
                for path,expected in case["preflight"].get("plan_hashes",{}).items():
                    source = preflight_evidence/path
                    assert digest(source) == expected, "physical plan evidence drift"
                    entry = archive.gettarinfo(str(source),arcname=path)
                    entry.mtime = 0; entry.uid = 0; entry.gid = 0; entry.uname = ""; entry.gname = ""
                    with source.open("rb") as stream: archive.addfile(entry,stream)
    ranked = sorted((row for row in rows if row["class"] == "Spark only" and row["status"] == "eligible"),key=lambda row:row["ns_per_input_row"],reverse=True)
    slow = sorted((row for row in rows if row.get("slower_than_noise") and row["kind"] != "shape"),key=lambda row:row["excess_ns_per_input_row"],reverse=True)
    evidence = {case["id"]:case.get("implementation_anchors",[]) for case in manifest["functions"]}
    unique = list({row["id"]:None for row in ranked})[:30]
    top30 = [dict(next(row for row in ranked if row["id"] == id),ansi_costs={row["configuration"]:row["ns_per_input_row"] for row in ranked if row["id"] == id},source_anchors=evidence[id]) for id in unique]
    (reports/"functions-1391-summary.json").write_text(json.dumps({"metadata":read(destination/"metadata.json"),"all_rows":rows,"paired_above_noise":slow,"spark_only_ranked":ranked,"close_noise_workloads":[row for row in rows if row["kind"] == "shape"],"top30_source_review":top30,"exclusions":[row for row in rows if row["status"] not in ("eligible","same_verified")]},indent=2)+"\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode",choices=("check","preflight","measure","summarize"))
    parser.add_argument("paths",nargs="*")
    parser.add_argument("--frozen-commit")
    parser.add_argument("--build-receipt",type=pathlib.Path)
    args = parser.parse_args()
    assert read(MANIFEST) == inventory.build(), "inventory freeze drift"
    self_check()
    manifest,schedule = read(MANIFEST),read(HERE/"schedule.json")
    assert schedule["input_rows"] == 1_000_000 and schedule["replicas"] == 3 and schedule["samples_per_case"] == 3
    if args.mode == "check":
        assert not args.paths
        print(json.dumps({"status":"passed","functions":len(manifest["functions"]),"registry_counts":manifest["registry_counts"]}))
        return
    if args.mode == "summarize":
        assert len(args.paths) == 2
        summarize(pathlib.Path(args.paths[0]).resolve(),pathlib.Path(args.paths[1]).resolve(),manifest)
        return
    assert len(args.paths) == 2
    binary,destination = map(lambda path:pathlib.Path(path).resolve(),args.paths)
    assert binary.is_file()
    assert set(schedule["cpus"]).issubset(os.sched_getaffinity(0)), "fixed CPU affinity unavailable"
    if args.mode == "preflight": preflight(binary,destination,manifest,schedule,args.build_receipt)
    else: measure(binary,destination,args.frozen_commit,args.build_receipt,manifest,schedule)


if __name__ == "__main__":
    main()
