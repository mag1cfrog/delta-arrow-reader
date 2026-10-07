"""Run the frozen selective-read schedule against one prepared MinIO server."""

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
from urllib.parse import unquote, urlsplit

import observe
import storage
import network
import run
import matrix
import large_workloads
import metadata_cache
from run import digest, save
from supervise import integer, require

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from run_order import balanced_orders

READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")
SESSIONS = {"reuse.li": "li.clustered.eq2-in20", "reuse.wide": "wide.clustered.eq2-in20",
            "reuse.files4096": "files4096.eq2-in20"}
DEFAULT_CASES = ("li.clustered.eq2-in20", "li.shuffled.eq2-in20")
COMBINED_DIAGNOSTICS = run.PROTOCOL.with_name("selective-read-combined-diagnostics.md")
GATE_WARMUP = run.PROTOCOL.with_name("selective-read-gate-warmup.md")
GATE_DIAGNOSTICS = run.PROTOCOL.with_name("selective-read-gate-diagnostics.md")


def validation_warmup(config):
    enabled = config.get("gate_warmup", False)
    require(type(enabled) is bool, "gate warmup must be a boolean")
    if enabled:
        require(config.get("comparison_revision") == 6 and
                config.get("warmup_amendment_sha256") == digest(GATE_WARMUP),
                "missing or stale gate-warmup amendment")
    else:
        require(config.get("warmup_amendment_sha256") is None, "standalone warmup cannot carry the amendment")
    return enabled


def combined_diagnostics(config):
    enabled = config.get("combined_diagnostics", False)
    require(type(enabled) is bool, "combined diagnostics must be a boolean")
    if enabled:
        require(config.get("comparison_revision") == 6 and
                config.get("diagnostics_amendment_sha256") == digest(COMBINED_DIAGNOSTICS),
                "missing or stale combined-diagnostic amendment")
    else:
        require(config.get("diagnostics_amendment_sha256") is None, "separate diagnostics cannot carry the amendment")
    return enabled


def validation_diagnostics(config):
    enabled = config.get("gate_diagnostics", False)
    require(type(enabled) is bool, "gate diagnostics must be a boolean")
    if enabled:
        require(config.get("comparison_revision") == 6 and config.get("combined_diagnostics") is True
                and config.get("gate_warmup") is True
                and config.get("gate_diagnostics_amendment_sha256") == digest(GATE_DIAGNOSTICS),
                "missing or stale gate-diagnostic mode/amendment")
    else:
        require(config.get("gate_diagnostics_amendment_sha256") is None,
                "separate gate diagnostics cannot carry the amendment")
    return enabled


def slot_purpose(slot, combined=False):
    if slot["stage"] == "gate":
        return "validation"
    if slot["stage"] in ("warmup", "timing"):
        return "timing"
    if slot["stage"] == "plan" or combined and slot["stage"] == "diagnostic" and slot["reader_id"] != "duckdb":
        return "diagnostic"
    return "io"


def plan_evidence(record, artifacts):
    evidence = []
    for query in record["queries"]:
        name = query["physical_plan"]
        require(isinstance(name, str) and Path(name).name == name, "missing or invalid native plan")
        path = artifacts / "reader" / name
        require(not path.is_symlink() and path.is_file() and path.stat().st_size > 0,
                "native plan export is empty or missing")
        evidence.append({"query_index": query["query_index"], "path": str(path), "sha256": digest(path)})
    return evidence


def schedule(inventory, campaign_id, comparison=None, *, combined=None, gate_warmup=None, gate_diagnostics=None):
    slots = []
    def mode(key, override, label):
        values = [entry.get(key, False) for entries in inventory.values() for entry in entries.values()]
        require(all(type(value) is bool for value in values) and len(set(values)) <= 1,
                f"reader inventory has inconsistent {label} modes")
        return (values[0] if values else False) if override is None else override
    combined = mode("combined_diagnostics", combined, "diagnostic")
    gate_warmup = mode("gate_warmup", gate_warmup, "warmup")
    gate_diagnostics = mode("gate_diagnostics", gate_diagnostics, "gate-diagnostic")
    require(type(gate_diagnostics) is bool and (not gate_diagnostics or
            combined and gate_warmup and (comparison or {}).get("comparison_revision") == 6),
            "gate diagnostics require revision 6, combined diagnostics and gate warmup")
    require(type(gate_warmup) is bool and (not gate_warmup or (comparison or {}).get("comparison_revision") == 6),
            "gate warmup requires comparison revision 6")
    if gate_warmup:
        cases = {job.removeprefix("reuse.") for job in inventory}
        require(len(cases) == 1 and set(inventory) == {next(iter(cases)), "reuse." + next(iter(cases))},
                "gate warmup requires one snapshot with both open and reuse profiles")
    require(type(combined) is bool and (not combined or (comparison or {}).get("comparison_revision") == 6),
            "combined diagnostics require comparison revision 6")
    roster = run.reader_roster(comparison or {})
    require(all(set(entries) == set(roster) for entries in inventory.values()), "reader inventory differs from the comparison roster")
    reduced = (comparison or {}).get("comparison_revision", 2) >= 4
    samples = 2 if (comparison or {}).get("sampling_stage") == "pilot" else 5
    def add(job, stage, readers, repetition, *, traced=False):
        for position, reader in enumerate(readers):
            slots.append({"run_id": f"{campaign_id}-{len(slots):05d}", "job_id": job,
                          "reader_id": reader, "stage": stage, "repetition": repetition,
                          "order": position, "traced": traced, **(comparison or {})})

    # Inventory is ordered: lexical isolated cases, then lexical reuse sessions.
    for job, entries in inventory.items():
        readers = tuple(r for r in roster if entries[r]["runnable"])
        if not readers:
            continue
        if not gate_warmup:
            for repetition, order in enumerate((readers,) if reduced else (readers, readers[::-1])):
                add(job, "warmup", order, repetition)
        orders = balanced_orders(readers)
        orders *= math.ceil((samples if reduced else 10) / len(orders))
        for repetition, order in enumerate(orders[:samples] if reduced else orders):
            add(job, "timing", order, repetition)
    # Separate diagnostic invocations follow every timing slot.
    for job, entries in inventory.items():
        readers = tuple(r for r in roster if entries[r]["runnable"])
        # DuckDB EXPLAIN reads additional data; keep it outside the I/O invocation.
        add(job, "plan", tuple(r for r in readers if r == "duckdb") if combined else readers, 0)
        if gate_diagnostics:
            continue
        if reduced:
            add(job, "diagnostic", readers, 0, traced=True)
            continue
        for repetition, order in enumerate((readers, readers[::-1])):
            add(job, "diagnostic_warmup", order, repetition, traced=True)
        for repetition, order in enumerate((readers, readers[::-1])):
            for position, reader in enumerate(order):
                # Same-query off/on then on/off pairs expose observer overhead.
                for traced in ((False, True) if repetition == 0 else (True, False)):
                    add(job, "diagnostic" if traced else "observer_baseline", (reader,), repetition, traced=traced)
                    slots[-1]["order"] = position
    return slots


def validate_diagnostics(config, inventory, slots, observations):
    combined = combined_diagnostics(config)
    gate_warmup = validation_warmup(config)
    gate_diagnostics = validation_diagnostics(config)
    require(slots == schedule(inventory, config["campaign_id"], run.comparison_identity(config),
                             combined=combined, gate_warmup=gate_warmup, gate_diagnostics=gate_diagnostics),
            "schedule differs from the declared diagnostic mode")
    if gate_diagnostics:
        require(all(entry.get("gate_diagnostics") is True for entries in inventory.values() for entry in entries.values()),
                "gate diagnostics were not declared in the frozen inventory")
    if gate_warmup:
        require(all(entry.get("gate_warmup") is True for entries in inventory.values() for entry in entries.values()),
                "gate warmup was not declared in the frozen inventory")
        by_id = {row["run_id"]: row for row in observations}
        for entries in inventory.values():
            for gate in entries.values():
                if gate["runnable"]:
                    require(warmed_gate(gate, by_id), "storage-warmup gate is missing or incomplete")
    if not combined:
        return
    require(all(entry.get("combined_diagnostics") is True for entries in inventory.values() for entry in entries.values()),
            "combined diagnostics were not declared in the frozen inventory")
    by_id = {slot["run_id"]: slot for slot in slots}
    require({row["run_id"] for row in observations if row["stage"] != "gate"} == set(by_id),
            "combined diagnostic schedule has missing or extra observations")
    for row in observations:
        if row["stage"] != "gate":
            require(all(row[key] == value for key, value in by_id[row["run_id"]].items()),
                    "observation differs from the declared diagnostic schedule")
        require(row["request"]["purpose"] == slot_purpose(row, combined), "diagnostic purpose changed")
        if gate_diagnostics:
            is_gate = row["stage"] == "gate"
            require(row["request"].get("validation_diagnostics", False) is is_gate and
                    (not is_gate or row["traced"] is True), "gate diagnostic request/tracing changed")
        record = row["observation"]
        if record["status"] != "success":
            continue
        if row["request"]["purpose"] == "diagnostic" or (
                gate_diagnostics and row["stage"] == "gate" and row["reader_id"] != "duckdb"):
            require(record.get("plan_artifacts") == plan_evidence(record, Path(row["artifacts"])),
                    "native plan artifacts changed")
        if row["stage"] == "diagnostic" or gate_diagnostics and row["stage"] == "gate":
            require(record["storage_capture"]["status"] == "passed" and
                    record["storage_environment"]["trace_enabled"] and record.get("storage_io") is not None,
                    "combined diagnostic lacks reconciled I/O")


def warmed_gate(gate, observations):
    row = observations.get(gate.get("run_id"), {})
    record = row.get("observation", {})
    return (row.get("stage") == "gate" and row.get("status") == "success"
            and gate.get("status") == "success" and isinstance(gate.get("identity"), dict)
            and record.get("status") == "success" and record.get("identity") == gate.get("identity")
            and record.get("correctness", {}).get("status") == "passed"
            and record.get("cleanup", {}).get("status") == "passed"
            and (not gate.get("gate_diagnostics") or (
                row.get("traced") is True and row.get("request", {}).get("validation_diagnostics") is True
                and record.get("validation_diagnostics") is True
                and record.get("storage_capture", {}).get("status") == "passed"
                and record.get("storage_environment", {}).get("trace_enabled") is True
                and isinstance(record.get("storage_io"), dict)
                and (row.get("reader_id") == "duckdb" or bool(record.get("plan_artifacts"))))))


def distribution(values):
    values = sorted(values)
    require(bool(values), "empty distribution")
    def quantile(p):
        index = (len(values) - 1) * p
        low, high = math.floor(index), math.ceil(index)
        return values[low] + (values[high] - values[low]) * (index - low)
    q1, median, q3 = (quantile(p) for p in (.25, .5, .75))
    return {"samples": len(values), "median": median, "q1": q1, "q3": q3, "iqr": q3 - q1}


def timer_resolution():
    previous = time.perf_counter_ns()
    ticks = []
    for _ in range(10000):
        current = time.perf_counter_ns()
        if current > previous:
            ticks.append(current - previous)
        previous = current
    nominal = math.ceil(time.get_clock_info("perf_counter").resolution * 10**9)
    return {"clock": "Linux CLOCK_MONOTONIC (Rust Instant/Python perf_counter)",
            "nominal_ns": nominal, "minimum_observed_tick_ns": min(ticks),
            "ratio_floor_ns": max(nominal, min(ticks))}


def validate(record, payload, reader, gate=None):
    """Reject incomplete successful records before admitting any sample."""
    if record["status"] != "success":
        return
    identity = record["identity"]
    comparison = run.comparison_identity(payload)
    require(identity["reader_id"] == reader, "reader identity changed")
    for key in (*comparison, "fixture_manifest_sha256", "case_id", "snapshot_version"):
        require(identity[key] == payload[key], "observation identity differs: " + key)
    require(identity["canonical_sql_sha256"] == hashlib.sha256(payload["canonical_sql"].encode()).hexdigest(), "SQL hash differs")
    for key in ("campaign_id", "run_id", "purpose", "execution_mode", "repetition", "order", "table_uri", "profile", "canonical_sql"):
        require(record[key] == payload[key], "observation request differs: " + key)
    require(record["capability"]["status"] == "supported" and record["cleanup"]["status"] == "passed", "incomplete capability or cleanup")
    require(record["external_resource_limits"]["process_memory_bytes"] == run.BUDGET["process_memory_bytes"], "missing enforced memory limit")
    if payload["comparison_revision"] in (3, 4, 5, 6) and payload["purpose"] == "validation":
        require(record["external_resource_limits"]["max_file_size_bytes"] == record["validation_export_limit_bytes_per_file"],
                "validation export limit was not inherited by the reader")
    queries = record["queries"]
    count = run.query_count(payload)
    require(isinstance(queries, list) and len(queries) == count, "incomplete query count")
    for index, query in enumerate(queries):
        require(query["query_index"] == index and integer(query["output_rows"]) and integer(query["output_batches"]), "invalid query counts")
    if gate is not None:
        require(identity == gate["identity"], "validated build/configuration changed")
    if payload["purpose"] in ("validation", "timing"):
        require(record["correctness"]["status"] == "passed", "missing correctness gate")
    if payload.get("validation_diagnostics"):
        require(payload["validation_diagnostics"] is True and payload["purpose"] == "validation"
                and payload["comparison_revision"] == 6 and record.get("validation_diagnostics") is True,
                "undeclared native gate diagnostics")
        require(record.get("storage_capture", {}).get("status") == "passed"
                and record.get("storage_environment", {}).get("trace_enabled") is True
                and isinstance(record.get("storage_io"), dict)
                and integer(record.get("diagnostic_session_ns")), "missing reconciled gate I/O")
        require(len([e for e in record.get("diagnostic_events", []) if e["event"] == "stream_complete"]) == count,
                "missing gate stream boundaries")
        require(all(q["completion_ns"] is None and q["first_batch_ns"] is None for q in queries)
                and record["open_query_ns"] is None and record["session_elapsed_ns"] is None,
                "diagnostic gate contains timing samples")
        if reader != "duckdb":
            require(len(record.get("plan_artifacts", [])) == count, "missing gate native plans")
        else:
            require(all(q["physical_plan"] is None for q in queries), "DuckDB EXPLAIN entered gate I/O")
    if payload["purpose"] != "timing":
        return
    for query in queries:
        require(integer(query["completion_ns"]), "invalid completion time")
        first = query["first_batch_ns"]
        require((first is None and bool(query["first_batch_unavailable_reason"])) or
                (integer(first) and first <= query["completion_ns"]), "invalid first-batch time")
    require(integer(record["cleanup_ns"]) and integer(record["session_elapsed_ns"]), "missing lifecycle clocks")
    if count == 1:
        require(record["open_query_ns"] == queries[0]["completion_ns"], "open/query clocks disagree")
    else:
        init = record["initialization_ns"]
        require(integer(init), "missing initialization time")
        require(record["initialization_plus_query1_ns"] == init + queries[0]["completion_ns"] and
                record["initialization_plus_all_queries_ns"] == init + sum(q["completion_ns"] for q in queries), "session totals disagree")
        require(record["session_elapsed_ns"] >= record["initialization_plus_all_queries_ns"], "enclosing session is shorter than its intervals")
    for key in ("process_user_cpu_ns", "process_system_cpu_ns", "process_cpu_ns", "peak_rss_bytes", "process_elapsed_ns"):
        require(integer(record["external_metrics"][key]), "missing process resource: " + key)


def measurements(record):
    values = {key: record[key] for key in ("open_query_ns", "initialization_ns", "initialization_plus_query1_ns",
              "initialization_plus_all_queries_ns", "session_elapsed_ns", "cleanup_ns") if record[key] is not None}
    if record.get("startup_ns") is not None:
        values["startup_ns"] = record["startup_ns"]
    values.update({key: record["external_metrics"][key] for key in (
        "process_user_cpu_ns", "process_system_cpu_ns", "process_cpu_ns", "peak_rss_bytes", "process_elapsed_ns")})
    for query in record["queries"]:
        for key in ("completion_ns", "first_batch_ns"):
            if query[key] is not None:
                values[f"query_{query['query_index']}.{key}"] = query[key]
    return values


def summarize(inventory, slots, rows, resolution, integrity=True):
    by_id = {row["run_id"]: row for row in rows}
    result = {}
    for job, entries in inventory.items():
        readers = {}
        for reader, gate in entries.items():
            selected = [s for s in slots if s["job_id"] == job and s["reader_id"] == reader]
            timed = [s for s in selected if s["stage"] == "timing"]
            required = [s for s in selected if s["stage"] in ("warmup", "timing")]
            statuses = Counter(by_id.get(s["run_id"], {}).get("status", "not_run") for s in timed)
            eligible = integrity and gate["runnable"] and bool(timed) and all(
                by_id.get(s["run_id"], {}).get("status") == "success" for s in required)
            if gate.get("gate_warmup"):
                eligible = eligible and warmed_gate(gate, by_id)
            values = [measurements(by_id[s["run_id"]]["observation"]) for s in timed] if eligible else []
            metrics = {key: distribution([v[key] for v in values]) for key in values[0]
                       if all(key in v for v in values)} if values else {}
            durations = {key: [v[key] for v in values] for key in metrics if key.endswith("_ns")}
            readers[reader] = {"gate_status": gate["status"], "eligible": eligible, "scheduled_samples": len(timed),
                               "sample_statuses": dict(statuses), "metrics": metrics, "speedup_vs_dar": {},
                               "first_batch_unavailable": [q["first_batch_unavailable_reason"] for s in timed
                                   if by_id.get(s["run_id"], {}).get("status") == "success"
                                   for q in by_id[s["run_id"]]["observation"]["queries"] if q["first_batch_ns"] is None],
                               "_durations": durations}
            if gate.get("gate_warmup"):
                readers[reader]["warmup"] = {"source": "exact-validation-gate", "run_id": gate.get("run_id"),
                                             "passed": warmed_gate(gate, by_id)}
            diagnostics = {stage: [by_id.get(s["run_id"], {"run_id": s["run_id"], "status": "not_run"})
                                  for s in selected if s["stage"] == stage]
                           for stage in ("diagnostic", "observer_baseline")}
            if gate.get("gate_diagnostics"):
                diagnostics["diagnostic"] = [by_id.get(gate.get("run_id"),
                    {"run_id": gate.get("run_id"), "status": "not_run"})]
            readers[reader]["diagnostics"] = {stage: [{"run_id": row["run_id"], "status": row["status"],
                "session_ns": row.get("observation", {}).get("diagnostic_session_ns"),
                "io": row.get("observation", {}).get("storage_io")} for row in stage_rows]
                for stage, stage_rows in diagnostics.items()}
            if gate.get("gate_diagnostics"):
                readers[reader]["diagnostics"]["diagnostic"][0]["source"] = "exact-validation-gate"
            overhead = {stage: [r.get("observation", {}).get("diagnostic_session_ns") for r in observations]
                        for stage, observations in diagnostics.items()}
            if all(len(v) == 2 and all(integer(x) for x in v) for v in overhead.values()) and all(
                    r["status"] == "success" for observations in diagnostics.values() for r in observations):
                off, on = (distribution(overhead[stage])["median"] for stage in ("observer_baseline", "diagnostic"))
                readers[reader]["observer_overhead"] = {"off_median_ns": off, "on_median_ns": on,
                    "difference_ns": on - off, "ratio": on / off if min(overhead["observer_baseline"] + overhead["diagnostic"]) >= resolution else None,
                    "note": "two descriptive pairs; separate observations from headline timing"}
        dar = readers[READERS[0]]
        for reader in (r for r in readers if r != READERS[0]):
            comparator = readers[reader]
            for key in ("open_query_ns", "initialization_plus_query1_ns", "initialization_plus_all_queries_ns"):
                ratio, reason = None, "both readers must pass validation and every scheduled warmup/timing slot"
                if dar["eligible"] and comparator["eligible"] and key in dar["metrics"] and key in comparator["metrics"]:
                    if min(dar["_durations"][key] + comparator["_durations"][key]) >= resolution:
                        ratio = comparator["metrics"][key]["median"] / dar["metrics"][key]["median"]
                        reason = None
                    else:
                        reason = "duration below timer resolution"
                comparator["speedup_vs_dar"][key] = {"value": ratio, "unavailable_reason": reason}
        for reader in readers.values():
            reader.pop("_durations")
        result[job] = readers
    return result


def read_results(directory, frozen_error="frozen campaign inputs changed"):
    """Load frozen campaign results and verify their summary against the journal."""
    frozen = json.loads((directory / "frozen.json").read_text())
    require(frozen == {name: digest(directory / name) for name in ("campaign.json", "inventory.json", "schedule.json")}, frozen_error)
    inventory, summary, slots = (json.loads((directory / (name + ".json")).read_text())
                                 for name in ("inventory", "summary", "schedule"))
    observations = [json.loads(line) for line in (directory / "observations.jsonl").read_text().splitlines()]
    require(summary["jobs"] == summarize(inventory, slots, observations,
            summary["timer_resolution"]["ratio_floor_ns"], summary["integrity_passed"]), "summary differs from raw observations")
    return inventory, summary, observations, slots


def execute(args):
    with metadata_cache.bindings():
        return execute_cached(args)


def execute_cached(args):
    output = args.output.resolve()
    output.mkdir()
    campaign_id = output.name
    fixtures, state = args.fixtures.resolve(), args.state.resolve()
    upload_tables = getattr(args, "upload_table", None)
    if upload_tables:
        require(not args.upload.exists(), "inline upload requires a new receipt path")
        storage.upload(state, fixtures, args.upload, upload_tables, checksum=True)
    receipt = json.loads(args.upload.read_text())
    require(receipt["status"] == "verified" and receipt["server_sha256"] == digest(state / "server.json") and
            receipt["fixture_manifest_sha256"] == digest(fixtures / "manifest.json") and
            receipt["table_root"] == f"s3://{storage.BUCKET}/{receipt['fixture_manifest_sha256']}", "upload receipt differs from server/fixtures")
    require(storage.inventory(fixtures, receipt.get("table_ids")) == receipt["objects"], "fixture objects differ from verified upload")
    binaries, builds = {}, {}
    for binary in args.binary:
        binary = binary.resolve()
        build = json.loads((binary.parent / "build.json").read_text())
        reader = build["reader_id"]
        require(reader in (*READERS, "spark") and reader not in binaries, "unknown or duplicate reader")
        require(digest(binary) == build["executable_sha256"], "reader executable changed")
        binaries[reader], builds[reader] = binary, build
    prepared = matrix.load(args.matrix, fixtures) if args.matrix else None
    workload_path = getattr(args, "workload", None)
    workload = large_workloads.load(workload_path) if workload_path else None
    require(not (workload and prepared), "use one workload revision per campaign")
    comparison = large_workloads.identity(workload_path) if workload else {
        "comparison_revision": 2, "protocol_sha256": digest(run.PROTOCOL)}
    gate_diagnostics = getattr(args, "gate_diagnostics", False)
    combined = getattr(args, "combined_diagnostics", False) or gate_diagnostics
    diagnostic_config = {"combined_diagnostics": combined,
                         **({"diagnostics_amendment_sha256": digest(COMBINED_DIAGNOSTICS)} if combined else {})}
    combined_diagnostics(comparison | diagnostic_config)
    gate_warmup = getattr(args, "gate_warmup", False) or gate_diagnostics
    warming_config = {"gate_warmup": gate_warmup,
                      **({"warmup_amendment_sha256": digest(GATE_WARMUP)} if gate_warmup else {})}
    validation_warmup(comparison | warming_config)
    gate_diagnostic_config = {"gate_diagnostics": gate_diagnostics,
        **({"gate_diagnostics_amendment_sha256": digest(GATE_DIAGNOSTICS)} if gate_diagnostics else {})}
    validation_diagnostics(comparison | diagnostic_config | warming_config | gate_diagnostic_config)
    roster = run.reader_roster(comparison)
    require(set(binaries) <= set(roster), "binary is outside the comparison reader roster")
    if prepared:
        require(not (args.case or args.reference or args.session or args.no_sessions), "--matrix supplies all cases and references; do not mix case/session overrides")
        for reader, build in builds.items():
            if reader in prepared["translation_locks"]:
                require(build["lockfile_sha256"] == prepared["translation_locks"][reader], "native translation lock changed")
    cases = {row["case_id"]: row for row in prepared["cases"]} if prepared else {}
    references = {case: Path(row["reference"]) for case, row in cases.items() if row["status"] == "prepared"}
    sessions = SESSIONS
    if workload:
        cases = {r["case_id"]: dict(r, status="prepared") for r in workload["cases"]
                 if r["case_id"] in args.case} if args.case else {
                     r["case_id"]: dict(r, status="prepared") for r in workload["cases"]
                     if r["fixture_manifest_sha256"] == digest(fixtures / "manifest.json")}
        require(cases and (not args.case or set(cases) == set(args.case)), "unknown/empty workload case selection")
        sessions = {s: c for s, c in workload["sessions"].items() if c in cases}
        require(not args.session or set(args.session) <= set(sessions), "reuse session is outside the selected workload cases")
        for reader, translation in workload["translations"].items():
            if reader in builds:
                require(builds[reader]["lockfile_sha256"] == translation["lock_sha256"], "native translation lock changed")
    for reference in args.reference:
        metadata = json.loads((reference / "reference.json").read_text())
        require(metadata["case_id"] not in references, "duplicate reference case")
        references[metadata["case_id"]] = reference.resolve()
    jobs = [{"id": case, "case_id": case, "execution_mode": "open"} for case in sorted(cases or set(args.case or DEFAULT_CASES))]
    if not prepared and not args.no_sessions:
        require(not args.session or set(args.session) <= set(sessions), "unknown reuse session")
        jobs += [{"id": session, "case_id": sessions[session], "execution_mode": "reuse"} for session in sorted(set(args.session or sessions))]
    if gate_warmup:
        require(len(cases) == 1 and {job["execution_mode"] for job in jobs} == {"open", "reuse"},
                "gate warmup requires one snapshot with both open and reuse profiles")
    config = storage.state(state)
    if network.config(state) is not None:
        config = dict(config, network=network.config(state))
    previous_affinity = os.sched_getaffinity(0)
    os.sched_setaffinity(0, config["cpus"]["observer"])
    resolution = timer_resolution()
    sources = [Path(__file__), HERE / "observe.py", HERE / "storage.py", HERE / "oracle.py", HERE.parent / "run_order.py",
               HERE / "runners/run.py", HERE / "runners/supervise.py", HERE / "metadata_cache.py", run.PROTOCOL]
    if "network" in config:
        sources += [HERE / "network.py", state / "network.json"]
    if combined:
        sources.append(COMBINED_DIAGNOSTICS)
    if gate_warmup:
        sources.append(GATE_WARMUP)
    if gate_diagnostics:
        sources.append(GATE_DIAGNOSTICS)
    if prepared:
        sources += [HERE / "matrix.py", matrix.CATALOG, matrix.EXPRESSIONS, args.matrix.resolve()]
    if workload:
        sources += [HERE / "large_workloads.py", workload_path.resolve(), run.AMENDMENT]
        if comparison["comparison_revision"] >= 4:
            sources.append(run.SAMPLING)
        if comparison["comparison_revision"] in (5, 6):
            sources += [run.SPARK_MATRIX if comparison["comparison_revision"] == 6 else run.PRODUCTION, HERE / "production_workloads.py"]
    hashes = {str(p): digest(p) for p in sources}
    save(output / "campaign.json", {"campaign_id": campaign_id, **comparison, **diagnostic_config, **warming_config, **gate_diagnostic_config,
         "fixtures": str(fixtures), "upload": str(args.upload.resolve()),
         "upload_sha256": digest(args.upload), "server": config, "reader_builds": builds, "jobs": jobs,
         "upload_verification": receipt.get("verification"), "in_process_upload": bool(upload_tables),
         "matrix": {"path": str(args.matrix.resolve()), "sha256": digest(args.matrix)} if prepared else None,
         "workload": str(workload_path.resolve()) if workload else None,
         "source_sha256": hashes, "timer_resolution": resolution, "started_ns": time.time_ns(),
         "cache_policy": "fresh clients, reused MinIO/OS caches, no flushes; complete warmup/run history in observations.jsonl",
         "scope": "requested cases/sessions only; not a complete publication campaign"})
    inventory, templates, rows = {}, {}, []
    abort_reason = None
    with storage.exclusive(state), (output / "observations.jsonl").open("x") as journal:
        def invoke(slot, job, gate=None):
            nonlocal abort_reason
            stage = slot["stage"]
            purpose = slot_purpose(slot, combined)
            payload = dict(templates[job["id"]], campaign_id=campaign_id, run_id=slot["run_id"], purpose=purpose,
                           repetition=slot["repetition"], order=slot["order"],
                           correctness_file=gate["correctness_file"] if gate else None)
            if gate_diagnostics and stage == "gate":
                payload["validation_diagnostics"] = True
            destination = output / slot["run_id"]
            if abort_reason:
                record = {"status": "not_run", "failure_reason": abort_reason}
            else:
                try:
                    record = observe.invoke(state, binaries[slot["reader_id"]], payload, destination, fixtures,
                                            references[job["case_id"]], traced=slot["traced"], _locked=True)
                    if record.get("cleanup", {}).get("status") != "passed":
                        abort_reason = "previous invocation did not prove process/server cleanup: " + slot["run_id"]
                    try:
                        if combined and record["status"] == "success" and (purpose == "diagnostic" or
                                gate_diagnostics and stage == "gate" and slot["reader_id"] != "duckdb"):
                            record["plan_artifacts"] = plan_evidence(record, destination)
                        validate(record, payload, slot["reader_id"], gate)
                        if prepared or workload:
                            matrix.check_translation(record, cases[job["case_id"]])
                    except (ValueError, KeyError, TypeError) as error:
                        record = dict(record, status="operational_failure", failure_reason="invalid observation: " + str(error))
                except Exception as error:
                    record = {"status": "operational_failure", "failure_reason": type(error).__name__ + ": " + str(error)}
                    abort_reason = "invocation stopped without proven cleanup: " + slot["run_id"]
            row = {**slot, "status": record["status"], "request": payload, "session_id": job["id"] if job["execution_mode"] == "reuse" else None,
                   "gate_run_id": gate["run_id"] if gate else None, "artifacts": str(destination), "observation": record}
            journal.write(json.dumps(row, sort_keys=True) + "\n")
            journal.flush()
            rows.append(row)
            print(json.dumps({k: row[k] for k in ("run_id", "job_id", "reader_id", "stage", "status")}), flush=True)
            return record

        try:
            for job in jobs:
                entries = inventory[job["id"]] = {}
                try:
                    if prepared:
                        require(cases[job["case_id"]]["status"] == "prepared", cases[job["case_id"]].get("failure_reason", "missing matrix reference"))
                    payload = run.request(fixtures, job["case_id"], job["execution_mode"], "validation", "pending", workload=workload_path)
                    location = Path(unquote(urlsplit(payload["table_uri"]).path)).relative_to(fixtures)
                    require(any(item["path"].startswith(str(location) + "/") for item in receipt["objects"]),
                            "table is absent from verified upload")
                    payload["table_uri"] = receipt["table_root"] + "/" + str(location)
                    reference = references[job["case_id"]]
                    metadata = json.loads((reference / "reference.json").read_text())
                    require(metadata["status"] == "complete" and metadata["fixture_manifest_sha256"] == payload["fixture_manifest_sha256"] and
                            metadata["protocol_sha256"] == payload["protocol_sha256"] and metadata["canonical_sql"] == payload["canonical_sql"] and
                            metadata["snapshot_version"] == payload["snapshot_version"], "reference differs from request")
                    require(run.comparison_identity(metadata) == comparison, "reference comparison/workload identity differs")
                    templates[job["id"]] = payload
                    missing = None
                except (ValueError, KeyError, OSError, StopIteration) as error:
                    missing = "fixture/query/reference not prepared: " + (str(error) or job["case_id"])
                for reader in roster:
                    entry = entries[reader] = {"runnable": False, "status": "preparation_failed", "failure_reason": missing,
                                               "case_id": job["case_id"], "execution_mode": job["execution_mode"],
                                               **({"combined_diagnostics": True} if combined else {}),
                                               **({"gate_warmup": True} if gate_warmup else {}),
                                               **({"gate_diagnostics": True} if gate_diagnostics else {})}
                    if missing:
                        continue
                    if reader not in binaries:
                        entry.update(status="operational_failure", failure_reason="reader build not supplied")
                        continue
                    slot = {"run_id": f"{campaign_id}-gate-{len(rows):05d}", "job_id": job["id"], "reader_id": reader,
                            "stage": "gate", "repetition": 0, "order": roster.index(reader), "traced": gate_diagnostics,
                            **(comparison if workload else {})}
                    record = invoke(slot, job)
                    entry.update(status=record["status"], runnable=record["status"] == "success", run_id=slot["run_id"],
                                 failure_reason=record.get("failure_reason"), identity=record.get("identity"),
                                 correctness_file=str(output / slot["run_id"] / "correctness.json"),
                                 reference=str(reference), reference_sha256=digest(reference / "reference.json"))
            save(output / "inventory.json", inventory)
            slots = schedule(inventory, campaign_id, comparison if workload else None, combined=combined,
                             gate_warmup=gate_warmup, gate_diagnostics=gate_diagnostics)
            save(output / "schedule.json", slots)
            frozen = {name: digest(output / name) for name in ("campaign.json", "inventory.json", "schedule.json")}
            save(output / "frozen.json", frozen)
            by_job = {job["id"]: job for job in jobs}
            for slot in slots:
                invoke(slot, by_job[slot["job_id"]], inventory[slot["job_id"]][slot["reader_id"]])
            intact = hashes == {str(p): digest(p) for p in sources} and frozen == {name: digest(output / name) for name in frozen}
            summary = summarize(inventory, slots, rows, resolution["ratio_floor_ns"], intact)
            failures = any(entry["status"] not in ("success", "unsupported") for entries in inventory.values() for entry in entries.values())
            failed_slots = any(row["status"] != "success" for row in rows if row["stage"] != "gate")
            status = "complete" if intact and not failures and not failed_slots and not abort_reason else "incomplete"
            save(output / "summary.json", {"status": status, "integrity_passed": intact, "abort_reason": abort_reason,
                 **comparison,
                 "campaign_id": campaign_id, "finished_ns": time.time_ns(), "timer_resolution": resolution,
                 "scheduled_slots": len(slots), "recorded_slots": sum(row["stage"] != "gate" for row in rows), "jobs": summary})
        finally:
            os.sched_setaffinity(0, previous_affinity)
    print(json.dumps({"status": status, "summary": str(output / "summary.json")}), flush=True)
    return 0 if status == "complete" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("state", "fixtures", "upload", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--binary", type=Path, action="append", default=[])
    parser.add_argument("--reference", type=Path, action="append", default=[])
    parser.add_argument("--case", action="append", help="repeat for selected cases; default: both compound layouts")
    parser.add_argument("--upload-table", action="append",
                        help="upload exact native table IDs to a new --upload receipt in this process before running")
    parser.add_argument("--matrix", type=Path, help="prepared 30-case matrix; replaces case/reference/session defaults")
    parser.add_argument("--workload", type=Path, help="explicit workload; select staged cases with --case")
    parser.add_argument("--combined-diagnostics", action="store_true",
                        help="revision 6: capture plans during traced I/O, retaining separate DuckDB EXPLAIN")
    parser.add_argument("--gate-warmup", action="store_true",
                        help="revision 6: one snapshot, use exact open/reuse gates as storage warmup")
    parser.add_argument("--gate-diagnostics", action="store_true",
                        help="revision 6: collect I/O and plans in exact gates; implies combined diagnostics and gate warmup")
    sessions = parser.add_mutually_exclusive_group()
    sessions.add_argument("--session", action="append", help="repeat for selected sessions; checked against the workload")
    sessions.add_argument("--no-sessions", action="store_true", help="run only the isolated cases")
    sys.exit(execute(parser.parse_args()))
