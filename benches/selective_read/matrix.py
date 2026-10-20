"""Freeze, prepare and report the 30 public predicate/projection cases."""

import argparse
import hashlib
import json
from pathlib import Path
import runpy
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import PROTOCOL, digest, save
from python_common import require
import native

CATALOG = HERE / "query-matrix.json"
EXPRESSIONS = HERE / "native-expressions.jsonl"
FAMILIES = ("li", "wide")
LAYOUTS = ("clustered", "shuffled")
ORACLE_FIELDS = ("source_rows", "qualifying_rows", "output_rows", "qualifying_selectivity",
                 "predicate_steps", "active_files", "candidate_files", "matching_files")


def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def scale_key(value):
    require(type(value) in (int, float) and value in (.01, 1, 10), "unknown public scale")
    return format(value, "g")


def translations(binary, request, output, comparison_revision=2):
    """Use the actual pinned adapter in its own interpreter, without table I/O."""
    adapter = runpy.run_path(str(binary.resolve()), run_name="matrix_translation")
    build = json.loads(binary.with_name("build.json").read_text())
    require(digest(binary) == build["executable_sha256"], "stale adapter executable")
    require(adapter["runtime_metadata"](adapter["engine"]()) == build["runtime"], "stale native runtime")
    result = {"reader": build["reader_id"], "lock_sha256": build["lockfile_sha256"],
              "adapter_sha256": digest(binary), "expressions": {}}
    for key, sql in json.loads(request.read_text()).items():
        expression = (adapter["expression_identity"](sql, comparison_revision=5)
                      if comparison_revision == 5 and build["reader_id"] == "daft"
                      else adapter["expression_identity"](sql))
        result["expressions"][key] = {"native_expression": expression, "sha256": adapter["json_hash"](expression)}
    save(output, result)


def catalog():
    value = json.loads(CATALOG.read_text())
    require(value["format"] == "selective-read-query-matrix-v1" and value["comparison_revision"] == 2
            and value["protocol_sha256"] == digest(PROTOCOL), "stale query catalog")
    require(value["native_expressions_sha256"] == digest(EXPRESSIONS), "native expression catalog changed")
    expressions = [json.loads(line) for line in EXPRESSIONS.read_text().splitlines()]
    require(all(sha(row["native_expression"]) == row["sha256"] for row in expressions), "native expression hash differs")
    expected = {(reader, key) for scale in value["scales"].values() for queries in scale["queries"].values()
                for query in queries.values() for reader, key in query["native_expression_sha256"].items()}
    require({(row["reader_id"], row["sha256"]) for row in expressions} == expected, "native expression catalog incomplete")
    return value


def shape_fields(case, frozen):
    family, layout, name = case.split(".")
    shape = frozen["shapes"][family][name]
    return {"case_id": case, "family": family, "layout": layout, **shape,
            "projection_columns": len(shape["projection"]),
            "predicate_only_columns": [c for c in shape["predicate_columns"] if c not in shape["projection"]]}


def query_fields(case, frozen, manifest):
    family, layout, name = case.split(".")
    table = next(t for t in manifest["tables"] if t["id"] == f"{family}.{layout}")
    source = next(s for s in manifest["sources"] if s["scale_factor"] == table["scale_factor"])
    scale = scale_key(table["scale_factor"])
    expected = "0.01" if manifest["profile"] == "smoke" else "10" if manifest["profile"] == "report" and family == "li" else "1"
    require(scale == expected and table["snapshot_version"] == 0, "fixture scale/snapshot differs from the public matrix")
    frozen_source = frozen["scales"][scale]
    query = frozen_source["queries"][family][name]
    sql = source["wide_queries" if family == "wide" else "queries"][name]
    require(source["in_literals"] == frozen_source["in_literals"] and source["rows"] == frozen_source["source_rows"]
            and sql == query["sql"], "source literals/rows/SQL differ from checked-in catalog")
    predicate = frozen["shapes"][family][name]["predicate"]
    literals = source["in_literals"] if predicate == "eq2-in20" else source["in_literals"][:1] if predicate == "eq2-in1" else []
    return {"scale": scale, "fixture_id": table["id"], "fixture_path": table["path"], "snapshot_version": table["snapshot_version"],
            "in_literals": literals, "canonical_sql": sql, "canonical_sql_sha256": query["sql_sha256"],
            "native_expression_sha256": query["native_expression_sha256"]}


def prepare(fixtures, output, references=None):
    import oracle
    frozen = catalog()
    manifest = json.loads((fixtures / "manifest.json").read_text())
    output.mkdir()
    root = references.resolve() if references else output.resolve() / "references"
    rows = []
    for family in FAMILIES:
        for layout in LAYOUTS:
            for name in frozen["shapes"][family]:
                case = f"{family}.{layout}.{name}"
                row = shape_fields(case, frozen)
                try:
                    row.update(query_fields(case, frozen, manifest))
                    require(oracle.case_input(fixtures, case)[-1] == row["canonical_sql"], "oracle SQL differs from catalog")
                    reference = root / case
                    if references is None:
                        metadata = oracle.prepare(fixtures, case, reference)
                    else:
                        metadata = json.loads((reference / "reference.json").read_text())
                    oracle.check_reference_build(metadata)
                    require(metadata["status"] == "complete" and metadata["case_id"] == case and metadata["canonical_sql"] == row["canonical_sql"]
                            and metadata["fixture_manifest_sha256"] == digest(fixtures / "manifest.json")
                            and metadata["protocol_sha256"] == frozen["protocol_sha256"]
                            and metadata["oracle_sha256"] == digest(Path(oracle.__file__))
                            and metadata["reference_sha256"] == digest(reference / "reference.parquet"), "stale independent reference")
                    row.update(status="prepared", reference=str(reference), reference_metadata_sha256=digest(reference / "reference.json"),
                               oracle={k: metadata[k] for k in ORACLE_FIELDS})
                except (ValueError, OSError, KeyError, StopIteration) as error:
                    row.update(status="preparation_failed", failure_reason=str(error) or case)
                rows.append(row)
                print(json.dumps({"case_id": case, "status": row["status"]}), flush=True)
    result = {"format": "selective-read-prepared-matrix-v1", "status": "complete" if all(r["status"] == "prepared" for r in rows) else "incomplete",
              "catalog_sha256": digest(CATALOG), "protocol_sha256": frozen["protocol_sha256"], "profile": manifest["profile"],
              "translation_locks": frozen["translation_locks"],
              "fixture_manifest_sha256": digest(fixtures / "manifest.json"), "cases": sorted(rows, key=lambda r: r["case_id"])}
    save(output / "matrix.json", result)
    return result


def load(path, fixtures):
    value = json.loads(path.read_text())
    frozen = catalog()
    manifest = json.loads((fixtures / "manifest.json").read_text())
    require(value["format"] == "selective-read-prepared-matrix-v1" and value["catalog_sha256"] == digest(CATALOG)
            and value["protocol_sha256"] == digest(PROTOCOL)
            and value["fixture_manifest_sha256"] == digest(fixtures / "manifest.json")
            and value["profile"] == manifest["profile"] and value["translation_locks"] == frozen["translation_locks"], "prepared matrix identity changed")
    expected = {f"{family}.{layout}.{name}" for family in FAMILIES for layout in LAYOUTS for name in frozen["shapes"][family]}
    require(len(value["cases"]) == len(expected) == 30 and {r["case_id"] for r in value["cases"]} == expected, "matrix must contain exactly 30 public cases")
    for row in value["cases"]:
        require(row["status"] in ("prepared", "preparation_failed"), "unknown preparation status")
        require(all(row[k] == v for k, v in shape_fields(row["case_id"], frozen).items()), "prepared shape changed")
        if row["status"] == "prepared":
            require(all(row[k] == v for k, v in query_fields(row["case_id"], frozen, manifest).items()), "prepared query changed")
            reference = Path(row["reference"]) / "reference.json"
            require(digest(reference) == row["reference_metadata_sha256"], "reference metadata changed")
            metadata = json.loads(reference.read_text())
            import oracle
            oracle.check_reference_build(metadata)
            require(metadata["status"] == "complete" and metadata["oracle_sha256"] == digest(HERE / "oracle.py")
                    and metadata["fixture_manifest_sha256"] == value["fixture_manifest_sha256"]
                    and metadata["protocol_sha256"] == value["protocol_sha256"]
                    and all(metadata[k] == row[k] for k in ("case_id", "snapshot_version", "canonical_sql", "canonical_sql_sha256", "projection", "limit"))
                    and row["oracle"] == {k: metadata[k] for k in ORACLE_FIELDS}, "reference identity/counts changed")
        else:
            require(bool(row["failure_reason"]), "missing preparation failure reason")
    return value


def check_translation(record, case):
    if record["status"] == "success" and case["status"] == "prepared":
        identity = record["identity"]
        expected = case["native_expression_sha256"].get(identity["reader_id"])
        require(identity["native_expression_sha256"] == expected, "native expression differs from checked-in matrix")


def report(campaign, output):
    config = json.loads((campaign / "campaign.json").read_text())
    require(config.get("matrix") is not None, "campaign did not use a prepared query matrix")
    prepared = load(Path(config["matrix"]["path"]), Path(config["fixtures"]))
    return native.call("campaign-report", {"campaign": str(campaign), "output": str(output),
        "format": {"kind": "matrix", "prepared": prepared}})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("translations", help=argparse.SUPPRESS)
    for name in ("binary", "request", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--comparison-revision", type=int, choices=(2, 3, 4, 5, 6), default=2)
    p = commands.add_parser("prepare")
    for name in ("fixtures", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--references", type=Path, help="reuse CASE_ID/reference.json directories")
    p = commands.add_parser("report")
    p.add_argument("--campaign", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "translations":
        translations(args.binary, args.request, args.output, args.comparison_revision)
    elif args.command == "prepare":
        sys.exit(0 if prepare(args.fixtures, args.output, args.references)["status"] == "complete" else 1)
    else:
        print(json.dumps(report(args.campaign, args.output)))
