"""Bind the independent oracle to frozen native workload definitions."""

from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import same_comparison
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


def query_fields(case, manifest):
    return native.call("workload", {"action": "large-query", "case": case, "manifest": manifest})


def resolve(path, fixtures, case):
    return native.call("workload", {"action": "resolve", "path": str(path), "fixtures": str(fixtures),
                                    "case": case, "harness": str(HERE)})


def verify(context, fixtures, case):
    current = resolve(context["path"], fixtures, case)
    if not same_comparison(context["identity"], current["identity"]):
        raise ValueError("workload changed during reference preparation")
