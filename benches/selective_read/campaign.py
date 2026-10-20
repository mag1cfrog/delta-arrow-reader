"""Run the frozen selective-read schedule against one prepared MinIO server."""

import argparse
import json
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
import native
from run import digest, save
from python_common import require

HERE = Path(__file__).resolve().parent

READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")
SESSIONS = {"reuse.li": "li.clustered.eq2-in20", "reuse.wide": "wide.clustered.eq2-in20",
            "reuse.files4096": "files4096.eq2-in20"}
DEFAULT_CASES = ("li.clustered.eq2-in20", "li.shuffled.eq2-in20")
COMBINED_DIAGNOSTICS = run.PROTOCOL.with_name("selective-read-combined-diagnostics.md")
GATE_WARMUP = run.PROTOCOL.with_name("selective-read-gate-warmup.md")
GATE_DIAGNOSTICS = run.PROTOCOL.with_name("selective-read-gate-diagnostics.md")


def validation_warmup(config):
    return native.call("campaign", {"action": "mode", "config": config, "mode": "gate-warmup",
                                    "protocols": str(run.PROTOCOL.parent)})


def combined_diagnostics(config):
    return native.call("campaign", {"action": "mode", "config": config, "mode": "combined-diagnostics",
                                    "protocols": str(run.PROTOCOL.parent)})


def validation_diagnostics(config):
    return native.call("campaign", {"action": "mode", "config": config, "mode": "gate-diagnostics",
                                    "protocols": str(run.PROTOCOL.parent)})


def slot_purpose(slot, combined=False):
    if slot["stage"] == "gate":
        return "validation"
    if slot["stage"] in ("warmup", "timing"):
        return "timing"
    if slot["stage"] == "plan" or combined and slot["stage"] == "diagnostic" and slot["reader_id"] != "duckdb":
        return "diagnostic"
    return "io"


def plan_evidence(record, artifacts):
    return native.call("campaign", {"action": "plan-evidence", "record": record, "artifacts": str(artifacts)})


def schedule(inventory, campaign_id, comparison=None, *, combined=None, gate_warmup=None, gate_diagnostics=None):
    return native.call("campaign", {"action": "schedule", "inventory": list(inventory.items()),
        "campaign_id": campaign_id, "comparison": comparison or {}, "combined": combined,
        "gate_warmup": gate_warmup, "gate_diagnostics": gate_diagnostics})


def timer_resolution():
    return native.call("campaign", {"action": "timer-resolution"})


def validate(record, payload, reader, gate=None):
    native.call("campaign", {"action": "validate", "record": record, "payload": payload,
        "reader": reader, "gate": gate, "protocols": str(run.PROTOCOL.parent)})


def summarize(inventory, slots, rows, resolution, integrity=True):
    return native.call("campaign", {"action": "summarize", "inventory": inventory, "slots": slots,
        "rows": rows, "resolution": resolution, "integrity": integrity})


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
    sources = [Path(__file__), HERE / "observe.py", HERE / "storage.py", HERE / "oracle.py",
               HERE / "runners/run.py", HERE / "runners/python_common.py", HERE / "metadata_cache.py", run.PROTOCOL]
    root = HERE.parents[1]
    sources += [Path(native.__file__), native.executable(), root / "Cargo.lock", root / "xtask/Cargo.toml",
                *sorted((root / "xtask/src").rglob("*.rs"))]
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
