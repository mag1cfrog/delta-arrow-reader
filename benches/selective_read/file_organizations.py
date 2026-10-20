"""Verify repacked rows and pruning geometry, then prepare the four public references."""

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq

import oracle
import native
from oracle import digest_file, inside, load_json, require, same_rows, verify_object


CASES = tuple(f"files{files}.{query}" for files in (64, 4096) for query in ("empty", "eq2-in20"))


def batches(root, table):
    for item in table["files"]:
        path = verify_object(inside(root, table["path"]), item)
        yield from pq.ParquetFile(path).iter_batches(batch_size=oracle.BATCH_ROWS)


def geometry(table, files):
    require(table["file_count"] == len(table["files"]) == files, "wrong repacked file count")
    rows = table["rows"]
    for i, item in enumerate(table["files"]):
        first, end = i * rows // files, (i + 1) * rows // files
        require(item["source_ordinal_range"] == [first, end] and item["rows"] == item["delta_stats"]["numRecords"] == end - first,
                "wrong repacked file boundary")
        require(sum(group["rows"] for group in item["row_groups"]) == item["rows"], "wrong repacked group rows")


def pruning(reference):
    if reference["case_id"].endswith(".empty"):
        require(reference["candidate_files"] == reference["matching_files"] == [] and reference["output_rows"] == 0,
                "empty predicate must exclude every file")
    else:
        require(reference["output_rows"] > 0 and reference["matching_files"], "compound predicate must match rows")
        require(set(reference["matching_files"]) <= set(reference["candidate_files"]), "statistics exclude matching files")
        # A coarse regression guard, not a tuned minimum winning margin for any reader.
        require(0 < len(reference["candidate_files"]) <= max(1, reference["active_files"] // 100),
                "compound predicate lost its extensive file-skipping opportunity")


def prepare(parent, fixtures, output):
    parent, fixtures, output = map(lambda p: Path(p).resolve(), (parent, fixtures, output))
    manifest, original = load_json(fixtures / "manifest.json"), load_json(parent / "manifest.json")
    require(manifest["status"] == original["status"] == "complete", "incomplete fixture preparation")
    require(manifest["repacked_from"]["manifest_sha256"] == digest_file(parent / "manifest.json"), "repack parent changed")
    clustered = next(t for t in original["tables"] if t["id"] == manifest["repacked_from"]["fixture_id"])
    require(manifest["sources"] == [next(s for s in original["sources"] if s["scale_factor"] == clustered["scale_factor"])],
            "repacking changed the source rows, literals or SQL")
    require({t["id"] for t in manifest["tables"]} == {"files64", "files4096"}, "wrong file-organization table inventory")
    output.mkdir(parents=True, exist_ok=False)
    verified = []
    for table in manifest["tables"]:
        geometry(table, int(table["id"].removeprefix("files")))
        rows = same_rows(batches(parent, clustered), batches(fixtures, table))
        require(rows == table["rows"] == clustered["rows"], "repacked table lost rows")
        verified.append({"fixture_id": table["id"], "rows": rows, "file_count": table["file_count"], "parquet_bytes": table["bytes"]})
    references = []
    for case in CASES:
        print(f"preparing {case}", flush=True)
        reference = oracle.prepare(fixtures, case, output / case)
        pruning(reference)
        references.append(reference)
    for predicate in ("empty", "eq2-in20"):
        pair = [r for r in references if r["case_id"].endswith("." + predicate)]
        require(pair[0]["reference_sha256"] == pair[1]["reference_sha256"], "repacked query results differ")
    result = {"status": "complete", "profile": manifest["profile"],
              "fixture_manifest_sha256": digest_file(fixtures / "manifest.json"),
              "parent_manifest_sha256": digest_file(parent / "manifest.json"),
              "verification": "exact ordered Arrow values in every column against parent li.clustered",
              "tables": verified, "cases": [{k: r[k] for k in (
                  "case_id", "source_rows", "output_rows", "active_files", "candidate_files", "matching_files", "reference_sha256")}
                  | {"reference_metadata_sha256": digest_file(output / r["case_id"] / "reference.json")}
                  for r in references]}
    (output / "file-organizations.json").write_bytes(oracle.json_bytes(result))
    return result


def report(campaign, references, output):
    config = json.loads((campaign / "campaign.json").read_text())
    checked = json.loads((references / "file-organizations.json").read_text())
    require(checked["status"] == "complete" and checked["fixture_manifest_sha256"] == digest_file(Path(config["fixtures"]) / "manifest.json"),
            "file-organization verification differs from campaign fixtures")
    expected = {case["case_id"]: case for case in checked["cases"]}
    require(set(expected) == set(CASES), "incomplete file-organization references")
    for case, metadata in expected.items():
        require(metadata["reference_metadata_sha256"] == digest_file(references / case / "reference.json"), "reference metadata changed")
        actual = json.loads((references / case / "reference.json").read_text())
        require(all(actual[key] == value for key, value in metadata.items() if key != "reference_metadata_sha256"), "verification counts differ from reference")
        pruning(actual)
    return native.call("campaign-report", {"campaign": str(campaign), "output": str(output),
        "format": {"kind": "files", "references": str(references), "checked": checked}})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    for name in ("parent", "fixtures", "output"):
        prepare_parser.add_argument("--" + name, type=Path, required=True)
    report_parser = commands.add_parser("report")
    for name in ("campaign", "references", "output"):
        report_parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.parent, args.fixtures, args.output)
    else:
        print(json.dumps(report(args.campaign, args.references, args.output)))
