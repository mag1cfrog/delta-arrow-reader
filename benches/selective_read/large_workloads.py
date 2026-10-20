"""Validate frozen workload bindings and export audited campaign results."""

import argparse
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import PROTOCOL, comparison_identity, digest
from python_common import require
import native

SCALES = (1, 10, 30, 100, 300)
READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")
SESSIONS = {"reuse.large.date30": "large.wide.shuffled.date30-wide",
            "reuse.large.compound": "large.wide.clustered.eq2-in20"}
FILE_SESSIONS = {"reuse.large.files4096" + suffix: "large.files4096.eq2-in20" + suffix for suffix in ("", ".dv")}
PILOT_SESSIONS = {"reuse.large.date30": SESSIONS["reuse.large.date30"]}
SOURCES = (HERE / "oracle.py", HERE / "oracle-requirements.txt", Path(__file__), HERE / "matrix.py", HERE / "runners/run.py",
           HERE / "wide_files.py", HERE / "file_organizations.py")


def shapes():
    import oracle
    return {"all-wide": oracle.WIDE_CASES["all-wide"],
            "date30-wide": (oracle.WIDE69, "date30", None),
            "date7-wide": (oracle.WIDE69, "date7", None),
            **{k: oracle.WIDE_CASES[k] for k in ("eq1", "eq2", "eq2-in20", "eq2-in20-keys")}}


def definitions(family="data", pilot_case=None):
    if family == "production":
        import production_shapes
        return {case: ("production", case, row["shape"]) for case, row in production_shapes.cases().items()}
    require(family in ("data", "files", "pilot"), "unknown workload family")
    require(family == "pilot" or pilot_case is None, "case selection requires the pilot family")
    if family == "pilot":
        case = SESSIONS["reuse.large.date30"] if pilot_case is None else pilot_case
        declared = definitions()
        require(case in declared and declared[case][0] == "data" and not case.endswith(".dv"),
                "pilot requires a declared no-DV data case")
        return {case: declared[case]}
    if family == "files":
        return {f"large.{name}.eq2-in20{suffix}": ("files", fixture + suffix, "eq2-in20")
                for name, fixture in (("files.normal", "wide.clustered"), ("files4096", "wide.files4096"))
                for suffix in ("", ".dv")}
    cases = {}
    for layout in ("clustered", "shuffled"):
        for name in shapes():
            cases[f"large.wide.{layout}.{name}"] = ("data", f"wide.{layout}", name)
        cases[f"large.wide.{layout}.date30-wide.dv"] = ("data", f"wide.{layout}.dv", "date30-wide")
    cases["scale-control.wide.shuffled.date30-wide"] = ("control", "wide.shuffled", "date30-wide")
    cases["scale-control.wide.clustered.eq2-in20"] = ("control", "wide.clustered", "eq2-in20")
    return cases


def identity(path):
    value = json.loads(Path(path).read_text())
    return comparison_identity(value | {"workload_manifest_sha256": digest(Path(path))})


def query_fields(case, manifest):
    import oracle
    role, fixture, name = (definitions() | definitions("files"))[case]
    table = next(t for t in manifest["tables"] if t["id"] == fixture)
    source = next(s for s in manifest["sources"] if s["scale_factor"] == table["scale_factor"])
    projection, predicate, limit = shapes()[name]
    literals = source["in_literals"]
    require(literals and all(type(n) is int for n in literals) and literals == sorted(set(literals)), "invalid frozen literals")
    require(len(literals) == 20 or manifest["profile"] == "smoke" and len(literals) < 20, "wrong literal count")
    dv = fixture.endswith(".dv")
    require(table["deletion_vectors"] is dv and type(table["snapshot_version"]) is int
            and table["snapshot_version"] == int(dv), "wrong large snapshot/DV state")
    sql = oracle.sql_for(projection, predicate, limit, literals)
    if name in source["wide_queries"]:
        require(sql == source["wide_queries"][name], "source SQL differs from large query")
    query_case = fixture.removesuffix(".dv") + "." + name + (".dv" if dv else "")
    if dv:
        require(table["queries"][query_case] == sql, "DV SQL differs from large query")
    columns = list(dict.fromkeys(p[0] for p in oracle.conditions(predicate, literals)))
    return {"case_id": case, "role": role, "query_case_id": query_case,
            "fixture_id": fixture, "fixture_path": table["path"], "snapshot_version": table["snapshot_version"],
            "deletion_vectors": dv, "scale_factor": source["scale_factor"], "source_rows": source["rows"],
            "layout": fixture.split(".")[1], "in_literals": literals,
            "projection": list(projection), "predicate": predicate, "predicate_columns": columns,
            "projection_columns": len(projection), "predicate_only_columns": [c for c in columns if c not in projection],
            "limit": limit, "canonical_sql": sql, "canonical_sql_sha256": oracle.digest_bytes(sql.encode()),
            "geometry": {"files": table["file_count"], "physical_bytes": table["bytes"],
                         "file_bytes": [f["bytes"] for f in table["files"]],
                         "row_groups": [len(f["row_groups"]) for f in table["files"]]}}


def load(path):
    value = json.loads(Path(path).read_text())
    identity(path)
    if value["format"] == "selective-read-production-workload-v1":
        import production_workloads
        return production_workloads.load(path, value)
    require(value["format"] == "selective-read-large-workload-v1" and value["comparison_revision"] in (3, 4),
            "unknown large workload")
    require(value["source_sha256"] == {str(p.relative_to(HERE)): digest(p) for p in SOURCES}, "workload harness sources changed")
    family = value.get("family", "data")
    require(value["scope"] in ("smoke", "pilot" if family == "pilot" else "candidate") and value["publication_ready"] is False,
            "formal workload freeze is not implemented by this slice")
    expected = definitions(family, value.get("pilot_case"))
    sessions = FILE_SESSIONS if family == "files" else {k: v for k, v in SESSIONS.items() if v in expected}
    require(value["readers"] == list(READERS) and value["sessions"] == sessions, "workload readers/sessions changed")
    require(len(value["cases"]) == len(expected) and {r["case_id"] for r in value["cases"]} == set(expected), "incomplete workload family")
    scales = value["scales"]
    if family == "pilot":
        require(scales == {"data": .01} if value["scope"] == "smoke" else
                set(scales) == {"data"} and scales["data"] in SCALES, "invalid pilot scale")
    elif family == "files":
        require(scales == {"files": .01} if value["scope"] == "smoke" else
                set(scales) == {"files"} and scales["files"] in SCALES[1:], "invalid file-pair scale")
    else:
        require(scales == {"data": .01, "control": .01} if value["scope"] == "smoke" else
            set(scales) == {"data", "control"} and scales["data"] in SCALES[1:]
            and scales["control"] == SCALES[SCALES.index(scales["data"]) - 1], "invalid source-scale controls")
    for row in value["cases"]:
        role, fixture, name = expected[row["case_id"]]
        projection, predicate, limit = shapes()[name]
        require(row["role"] == role and row["fixture_id"] == fixture and row["scale_factor"] == scales[role]
                and row["projection"] == list(projection) and row["predicate"] == predicate and row["limit"] == limit,
                "large workload shape or scale changed")
    limits = value["oracle_limits"]
    require(set(limits) == {"memory_bytes", "disk_bytes", "elapsed_seconds"}
            and all(type(v) is int and v > 0 for v in limits.values())
            and limits["disk_bytes"] >= 9 * 1024**2
            and limits["memory_bytes"] == 16 * 1024**3 and limits["elapsed_seconds"] < 2**31, "invalid oracle ceilings")
    require(set(value["translations"]) == {"polars", "daft"}, "missing native translations")
    import matrix
    for reader, translation in value["translations"].items():
        require(translation["lock_sha256"] == digest(HERE / "runners" / reader / "lock.json"), "translation lock changed")
        require(set(translation["expressions"]) == set(expected), "incomplete native translations")
        for case, expression in translation["expressions"].items():
            require(matrix.sha(expression["native_expression"]) == expression["sha256"], "native expression changed")
            row = next(r for r in value["cases"] if r["case_id"] == case)
            require(row["native_expression_sha256"][reader] == expression["sha256"], "native expression binding changed")
    return value


def binding(value, fixtures, case_id):
    if value["comparison_revision"] in (5, 6):
        import production_workloads
        return production_workloads.binding(value, fixtures, case_id)
    row = next(r for r in value["cases"] if r["case_id"] == case_id)
    path = Path(fixtures) / "manifest.json"
    require(digest(path) == row["fixture_manifest_sha256"], "workload fixture identity changed")
    manifest = json.loads(path.read_text())
    if value.get("family") == "files":
        import wide_files
        wide_files.geometry(manifest, large=True, smoke=value["scope"] == "smoke")
    expected = query_fields(case_id, manifest)
    require(all(row[k] == v for k, v in expected.items()), "workload SQL, literals, scale or geometry changed")
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
