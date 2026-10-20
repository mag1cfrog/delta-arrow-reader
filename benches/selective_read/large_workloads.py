"""Bind the independent oracle to frozen native workload definitions."""

import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import comparison_identity, digest
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
    return native.call("workload", {"action": "large-binding", "value": value, "fixtures": str(fixtures), "case": case_id})
