"""Validate frozen workload bindings and export audited campaign results."""

import argparse
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import PROTOCOL, comparison_identity, digest
import native

READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")
SESSIONS = {"reuse.large.date30": "large.wide.shuffled.date30-wide",
            "reuse.large.compound": "large.wide.clustered.eq2-in20"}
FILE_SESSIONS = {"reuse.large.files4096" + suffix: "large.files4096.eq2-in20" + suffix for suffix in ("", ".dv")}
PILOT_SESSIONS = {"reuse.large.date30": SESSIONS["reuse.large.date30"]}
SOURCES = (HERE / "oracle.py", HERE / "oracle-requirements.txt", Path(__file__), HERE / "matrix.py", HERE / "runners/run.py",
           HERE / "wide_files.py", HERE / "file_organizations.py")


def shapes():
    return {name: (tuple(columns), predicate, limit) for name, (columns, predicate, limit) in
            native.call("workload", {"action": "large-shapes"}).items()}


def definitions(family="data", pilot_case=None):
    return {case: tuple(row) for case, row in native.call("workload", {
        "action": "large-definitions", "family": family, "pilot_case": pilot_case}).items()}


def identity(path):
    value = json.loads(Path(path).read_text())
    return comparison_identity(value | {"workload_manifest_sha256": digest(Path(path))})


def query_fields(case, manifest):
    return native.call("workload", {"action": "large-query", "case": case, "manifest": manifest})


def load(path):
    value = json.loads(Path(path).read_text())
    identity(path)
    if value["format"] == "selective-read-production-workload-v1":
        import production_workloads
        return production_workloads.load(path, value)
    return native.call("workload", {"action": "large-load", "value": value, "harness": str(HERE)})


def binding(value, fixtures, case_id):
    if value["comparison_revision"] in (5, 6):
        import production_workloads
        return production_workloads.binding(value, fixtures, case_id)
    row = native.call("workload", {"action": "large-binding", "value": value, "fixtures": str(fixtures), "case": case_id})
    if value.get("family") == "files":
        import wide_files
        wide_files.geometry(json.loads((Path(fixtures) / "manifest.json").read_text()), large=True, smoke=value["scope"] == "smoke")
    return row


def report(campaign, output):
    """Bind independent queries before the native campaign evidence audit."""
    config = json.loads((campaign / "campaign.json").read_text())
    path = Path(config["workload"])
    frozen = load(path)
    jobs = {job["id"]: job for job in config["jobs"]}
    observed = {json.loads(line)["job_id"] for line in (campaign / "observations.jsonl").read_text().splitlines()}
    cases = {jobs[job]["case_id"] for job in observed}
    bindings = {case: binding(frozen, Path(config["fixtures"]), case) for case in cases}
    return native.call("campaign-report", {"campaign": str(campaign), "output": str(output),
        "format": {"kind": "workload", "frozen": frozen, "identity": identity(path),
                   "bindings": bindings, "protocols": str(PROTOCOL.parent)}})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("report",))
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = report(args.campaign, args.output)
    print(json.dumps({"status": result["status"], "entries": len(result["rows"])}))
    sys.exit(0 if result["status"] == "complete" else 1)
