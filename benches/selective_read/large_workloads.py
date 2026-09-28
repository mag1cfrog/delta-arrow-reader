"""Bind the large query families to immutable fixtures before any reader timing."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import AMENDMENT, PROTOCOL, comparison_identity, digest, save
from supervise import require

SCALES = (1, 10, 30, 100, 300)
READERS = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")
SESSIONS = {"reuse.large.date30": "large.wide.shuffled.date30-wide",
            "reuse.large.compound": "large.wide.clustered.eq2-in20"}
SOURCES = (HERE / "oracle.py", Path(__file__), HERE / "matrix.py", HERE / "runners/run.py")


def shapes():
    import oracle
    return {"all-wide": oracle.WIDE_CASES["all-wide"],
            "date30-wide": (oracle.WIDE69, "date30", None),
            "date7-wide": (oracle.WIDE69, "date7", None),
            **{k: oracle.WIDE_CASES[k] for k in ("eq1", "eq2", "eq2-in20", "eq2-in20-keys")}}


def definitions():
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
    role, fixture, name = definitions()[case]
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


def define(fixtures, controls, binaries, output, disk_limit_mib, elapsed_limit_seconds, smoke=False):
    """Freeze candidate inputs; publication/environment selection belongs to the pilot."""
    require(type(disk_limit_mib) is int and disk_limit_mib > 0
            and type(elapsed_limit_seconds) is int and 0 < elapsed_limit_seconds < 2**31, "finite oracle limits required")
    inputs, scales = {}, {}
    for role, roots in (("data", fixtures), ("control", controls)):
        for root in roots:
            root = root.resolve()
            manifest = json.loads((root / "manifest.json").read_text())
            require(manifest["status"] == "complete" and manifest["protocol"] == "selective-read-v1", "incomplete fixtures")
            if smoke:
                require(manifest["profile"] == "smoke", "smoke workload requires smoke fixtures")
            else:
                require(manifest["profile"] == "large" and manifest["protocol_sha256"] == digest(AMENDMENT)
                        and manifest["base_protocol_sha256"] == digest(PROTOCOL), "large fixture amendment differs")
            for table in manifest["tables"]:
                if table["id"] not in {v[1] for v in definitions().values()}:
                    continue
                key = role, table["id"]
                require(key not in inputs, "duplicate workload fixture: " + str(key))
                inputs[key] = root, manifest
                scale = table["scale_factor"]
                require(role not in scales or scales[role] == scale, "mixed source scales in one role")
                scales[role] = scale
    require(set(scales) == {"data", "control"}, "data and preceding-scale fixtures required")
    if smoke:
        require(scales == {"data": .01, "control": .01}, "smoke scale must be SF0.01")
    else:
        require(scales["data"] in SCALES[1:] and scales["control"] == SCALES[SCALES.index(scales["data"]) - 1],
                "control must use the immediately preceding ladder rung")
    cases = []
    for case, (role, fixture, _) in definitions().items():
        require((role, fixture) in inputs, "missing workload fixture: " + str((role, fixture)))
        root, manifest = inputs[role, fixture]
        row = query_fields(case, manifest)
        row.update(fixtures=str(root), fixture_manifest_sha256=digest(root / "manifest.json"))
        cases.append(row)
    for role in scales:
        rows = [r for r in cases if r["role"] == role]
        require(all((r["source_rows"], r["in_literals"]) == (rows[0]["source_rows"], rows[0]["in_literals"]) for r in rows),
                "layouts/DV pairs disagree on source rows or literals")
    output.mkdir()
    queries = {r["case_id"]: r["canonical_sql"] for r in cases}
    save(output / "sql.json", queries)
    translations = {}
    for binary in binaries:
        binary = binary.resolve()
        build = json.loads(binary.with_name("build.json").read_text())
        reader = build["reader_id"]
        require(reader in ("polars", "daft") and reader not in translations, "provide one Polars and one Daft binary")
        path = output / (reader + "-translations.json")
        subprocess.run([str(binary.with_name("venv") / "bin/python"), "-I", "-B", str(HERE / "matrix.py"),
                        "translations", "--binary", str(binary), "--request", str((output / "sql.json").resolve()),
                        "--output", str(path.resolve())], check=True)
        translations[reader] = json.loads(path.read_text())
    require(set(translations) == {"polars", "daft"}, "both native translations required")
    for row in cases:
        row["native_expression_sha256"] = {r: v["expressions"][row["case_id"]]["sha256"] for r, v in translations.items()}
    result = {"format": "selective-read-large-workload-v1", "comparison_revision": 3,
              "protocol_sha256": digest(AMENDMENT), "base_protocol_sha256": digest(PROTOCOL),
              "scope": "smoke" if smoke else "candidate", "publication_ready": False,
              "source_sha256": {str(p.relative_to(HERE)): digest(p) for p in SOURCES},
              "scales": scales, "readers": list(READERS), "cases": cases, "sessions": SESSIONS,
              "oracle_limits": {"memory_bytes": 16 * 1024**3, "disk_bytes": disk_limit_mib * 1024**2,
                                "elapsed_seconds": elapsed_limit_seconds},
              "translations": translations}
    save(output / "workload.json", result)
    load(output / "workload.json")
    return result


def load(path):
    value = json.loads(Path(path).read_text())
    identity(path)
    require(value["format"] == "selective-read-large-workload-v1" and value["comparison_revision"] == 3,
            "unknown large workload")
    require(value["source_sha256"] == {str(p.relative_to(HERE)): digest(p) for p in SOURCES}, "workload harness sources changed")
    require(value["scope"] in ("smoke", "candidate") and value["publication_ready"] is False,
            "formal workload freeze is not implemented by this slice")
    require(value["readers"] == list(READERS) and value["sessions"] == SESSIONS, "workload readers/sessions changed")
    require(len(value["cases"]) == 18 and {r["case_id"] for r in value["cases"]} == set(definitions()), "large workload must have exactly 18 cases")
    scales = value["scales"]
    require(scales == {"data": .01, "control": .01} if value["scope"] == "smoke" else
            set(scales) == {"data", "control"} and scales["data"] in SCALES[1:]
            and scales["control"] == SCALES[SCALES.index(scales["data"]) - 1], "invalid source-scale controls")
    for row in value["cases"]:
        role, fixture, name = definitions()[row["case_id"]]
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
        require(set(translation["expressions"]) == set(definitions()), "incomplete native translations")
        for case, expression in translation["expressions"].items():
            require(matrix.sha(expression["native_expression"]) == expression["sha256"], "native expression changed")
            row = next(r for r in value["cases"] if r["case_id"] == case)
            require(row["native_expression_sha256"][reader] == expression["sha256"], "native expression binding changed")
    return value


def binding(value, fixtures, case_id):
    row = next(r for r in value["cases"] if r["case_id"] == case_id)
    path = Path(fixtures) / "manifest.json"
    require(digest(path) == row["fixture_manifest_sha256"], "workload fixture identity changed")
    expected = query_fields(case_id, json.loads(path.read_text()))
    require(all(row[k] == v for k, v in expected.items()), "workload SQL, literals, scale or geometry changed")
    return row


def report(campaign, output):
    """Audit the existing campaign artifacts, including every failed reader slot."""
    import campaign as runner
    config = json.loads((campaign / "campaign.json").read_text())
    path = Path(config["workload"])
    frozen = load(path)
    comparison = identity(path)
    require(comparison_identity(config) == comparison, "campaign workload changed")
    seals = json.loads((campaign / "frozen.json").read_text())
    require(seals == {name: digest(campaign / name) for name in ("campaign.json", "inventory.json", "schedule.json")}, "frozen campaign inputs changed")
    inventory = json.loads((campaign / "inventory.json").read_text())
    slots = json.loads((campaign / "schedule.json").read_text())
    observations = [json.loads(line) for line in (campaign / "observations.jsonl").read_text().splitlines()]
    summary = json.loads((campaign / "summary.json").read_text())
    require(comparison_identity(summary) == comparison and all(comparison_identity(s) == comparison for s in slots),
            "schedule/summary workload changed")
    require(slots == runner.schedule(inventory, config["campaign_id"], comparison), "schedule differs from the frozen ordering rules")
    jobs = {j["id"]: j for j in config["jobs"]}
    require(set(jobs) == set(inventory) == set(summary["jobs"]), "campaign inventory differs from jobs")
    for job, entries in inventory.items():
        require(set(entries) == set(READERS), "all five reader statuses are required")
        case = jobs[job]["case_id"]
        require(case in definitions() and (jobs[job]["execution_mode"] == "open" and job == case or
                jobs[job]["execution_mode"] == "reuse" and frozen["sessions"].get(job) == case), "unknown workload job")
    require(len({r["run_id"] for r in observations}) == len(observations), "duplicate observation")
    for row in observations:
        require(comparison_identity(row) == comparison and comparison_identity(row["request"]) == comparison, "observation workload changed")
        job = jobs[row["job_id"]]
        expected = binding(frozen, Path(config["fixtures"]), job["case_id"])
        require(row["request"]["canonical_sql"] == expected["canonical_sql"]
                and row["request"]["execution_mode"] == job["execution_mode"], "observation query changed")
        gate = inventory[row["job_id"]][row["reader_id"]]
        runner.validate(row["observation"], row["request"], row["reader_id"], gate if row["stage"] != "gate" else None)
    require(summary["jobs"] == runner.summarize(inventory, slots, observations,
            summary["timer_resolution"]["ratio_floor_ns"], summary["integrity_passed"]), "summary differs from raw observations")
    output.mkdir()
    rows = [{"job": job, "case": next(r for r in frozen["cases"] if r["case_id"] == job["case_id"]),
             "reader_id": reader, "gate": inventory[job["id"]][reader],
             "measurements": summary["jobs"][job["id"]][reader],
             "native_phases": {"planning_ns": None, "scan_ns": None,
                               "unavailable_reason": "adapters do not expose comparable separate phase clocks"}}
            for job in config["jobs"] for reader in READERS]
    result = {"status": summary["status"], **comparison, "campaign_id": config["campaign_id"],
              "scope": frozen["scope"], "publication_ready": False, "rows": rows,
              "observations_sha256": digest(campaign / "observations.jsonl"),
              "summary_sha256": digest(campaign / "summary.json")}
    save(output / "large-workload-report.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    if sys.argv[1:2] == ["report"]:
        parser.add_argument("command")
        parser.add_argument("--campaign", type=Path, required=True)
        parser.add_argument("--output", type=Path, required=True)
        args = parser.parse_args()
        result = report(args.campaign, args.output)
        print(json.dumps({"status": result["status"], "entries": len(result["rows"])}))
        sys.exit(0 if result["status"] == "complete" else 1)
    parser.add_argument("--fixtures", type=Path, action="append", required=True)
    parser.add_argument("--control-fixtures", type=Path, action="append", required=True)
    parser.add_argument("--binary", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--disk-limit-mib", type=int, required=True)
    parser.add_argument("--elapsed-limit-seconds", type=int, required=True)
    parser.add_argument("--smoke", action="store_true", help="bounded SF0.01 contract check, never a scale-control result")
    args = parser.parse_args()
    define(args.fixtures, args.control_fixtures, args.binary, args.output,
           args.disk_limit_mib, args.elapsed_limit_seconds, args.smoke)
