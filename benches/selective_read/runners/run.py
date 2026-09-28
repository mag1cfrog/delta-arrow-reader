"""Create one public-case request, invoke a reader, and optionally validate every export.

This is a single invocation, not a performance campaign or resource scheduler.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time


HERE = Path(__file__).resolve().parent
PROTOCOL = HERE.parents[2] / "docs/content/benchmarks/selective-read-protocol.md"
BUDGET = {"worker_threads": 8, "max_blocking_threads": 64, "target_partitions": 8,
          "batch_rows": 8192, "datafusion_pool_bytes": 4 * 1024**3,
          "process_memory_bytes": 8 * 1024**3, "logical_cpus": 8}


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def save(path, value):
    with path.open("x") as out:
        json.dump(value, out, sort_keys=True, indent=2)
        out.write("\n")


def request(fixtures, case_id, execution_mode, purpose, run_id, table_uri=None, correctness=None):
    manifest_path = fixtures / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest["status"] != "complete" or manifest["protocol"] != "selective-read-v1":
        raise ValueError("incomplete or unknown fixture manifest")
    fixture_id, query = case_id.rsplit(".", 1)
    table = next(t for t in manifest["tables"] if t["id"] == fixture_id)
    source = next(s for s in manifest["sources"] if s["scale_factor"] == table["scale_factor"])
    sql = source["wide_queries" if fixture_id.startswith("wide.") else "queries"][query]
    location = (fixtures / table["path"]).resolve()
    if not location.is_relative_to(fixtures.resolve()):
        raise ValueError("table path escapes fixture root")
    return {
        "table_uri": table_uri or location.as_uri(), "snapshot_version": table["snapshot_version"],
        "case_id": case_id, "canonical_sql": sql, "comparison_revision": 2,
        "protocol_sha256": digest(PROTOCOL), "fixture_manifest_sha256": digest(manifest_path),
        "profile": manifest["profile"], "execution_mode": execution_mode, "purpose": purpose,
        "resource_budget": BUDGET, "correctness_file": str(correctness.resolve()) if correctness else None,
        "campaign_id": None, "run_id": run_id, "repetition": None, "order": None,
    }


def check_result(record, output, fixtures, reference):
    sys.path.insert(0, str(HERE.parent))
    import oracle
    checks = []
    try:
        for query in record["queries"]:
            checks.append(oracle.check(reference, fixtures,
                output / "reader" / query["result"], output / "reader" / query["identity"]))
        proof = {"status": "passed", "checks": checks}
    except (OSError, ValueError, KeyError, TypeError) as error:
        proof = {"status": "validation_failed", "checks": checks, "failure_reason": str(error)}
        record.update(status="validation_failed", failure_reason=str(error))
    save(output / "correctness.json", proof)
    record["correctness"] = {"status": proof["status"], "path": str((output / "correctness.json").resolve()),
                             "artifact_sha256": digest(output / "correctness.json")}


def invoke(binary, payload, output, fixtures=None, reference=None, *, env=None, command_prefix=(), supervised=False, defer_validation=False):
    output.mkdir()
    save(output / "request.json", payload)
    supervision = None
    with (output / "stdout.jsonl").open("x") as stdout, (output / "stderr.log").open("x") as stderr:
        started = time.time_ns()
        command = [*command_prefix, str(binary.resolve()), str((output / "request.json").resolve()), str((output / "reader").resolve())]
        if supervised:
            import supervise
            supervision = supervise.launch(command, payload, output, stdout, stderr, env)
            returncode = supervision["returncode"]
            save(output / "process.json", supervision)
        else:
            returncode = subprocess.run(command, stdout=stdout, stderr=stderr, env=env).returncode
        exited = time.time_ns()
    record_path = output / "reader/record.json"
    if record_path.exists():
        try:
            record = json.loads(record_path.read_text())
            if not isinstance(record, dict) or not isinstance(record.get("status"), str):
                raise ValueError("missing observation status")
        except (ValueError, TypeError) as error:
            record = {"status": "operational_failure", "failure_reason": "malformed reader record: " + str(error)}
    else:
        error = (output / "stderr.log").read_text()
        try:
            rejected = json.loads(error).get("status") == "invalid_input"
        except (ValueError, AttributeError):
            rejected = False
        record = {"status": "invalid_input" if rejected else "operational_failure",
                  "failure_reason": error or "reader exited without an observation record"}
    if returncode and record["status"] == "success":
        record["status"] = "operational_failure"
        record["failure_reason"] = f"reader exited with status {returncode} after writing its record"
    if supervision:
        record["supervision"] = supervision
        if supervision["status"] != "success":
            record.update(status=supervision["status"], failure_reason=supervision["failure_reason"])
        if record["status"] == "success" and (supervision["last_phase"] != "cleanup" or
                len(supervision["completed_queries"]) != (10 if payload["execution_mode"] == "reuse" else 1)):
            record.update(status="operational_failure", failure_reason="reader did not complete the watchdog lifecycle")
        if type(record.get("cleanup_ns")) is int and record["cleanup_ns"] > supervision["cleanup_deadline_seconds"] * 10**9:
            record.update(status="timeout", failure_reason="reported cleanup exceeded its deadline")
        record.setdefault("queries", supervision["completed_queries"])
        record.setdefault("external_metrics", {}).update({k: supervision[k] for k in (
            "process_user_cpu_ns", "process_system_cpu_ns", "process_cpu_ns", "peak_rss_bytes", "process_elapsed_ns")})
    if returncode == 0 and record["status"] == "success" and payload["purpose"] == "validation" and not defer_validation:
        check_result(record, output, fixtures, reference)
    if payload["purpose"] in ("diagnostic", "io"):
        record["diagnostic_process"] = {"started_ns": started, "exited_ns": exited}
    save(output / "observation.json", record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--execution", choices=("open", "reuse"), default="open")
    parser.add_argument("--purpose", choices=("validation", "timing", "diagnostic", "io"), required=True)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--correctness", type=Path)
    parser.add_argument("--table-uri")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.purpose == "validation" and args.reference is None:
        parser.error("validation requires --reference")
    if args.purpose == "timing" and args.correctness is None:
        parser.error("timing requires --correctness")
    payload = request(args.fixtures, args.case, args.execution, args.purpose, args.output.name,
                      args.table_uri, args.correctness)
    record = invoke(args.binary, payload, args.output, args.fixtures, args.reference)
    print(json.dumps({"status": record["status"], "observation": str(args.output / "observation.json")}))
    return 0 if record["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
