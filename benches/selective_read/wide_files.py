"""Independent checks for the wide normal/4096-file pairs and their deletion union."""

import argparse
import json
from pathlib import Path
from statistics import median

import file_organizations as organizations
import oracle
from oracle import require

LEGACY_CASES = ("wide.files4096.eq2-in20", "wide.files4096.eq2-in20.dv")


def geometry(manifest, *, large, smoke):
    pair = manifest["wide_file_pair"]
    require(pair["kind"] == ("large" if large else "legacy"), "wrong wide-file deletion contract")
    expected = {"wide.clustered", "wide.files4096", "wide.files4096.dv"} | ({"wide.clustered.dv"} if large else set())
    tables = {t["id"]: t for t in manifest["tables"]}
    require(set(tables) == expected and len(tables) == len(manifest["tables"]), "wrong wide-file inventory")
    require(len(manifest["sources"]) == 1, "one shared source required")
    source = manifest["sources"][0]
    scale = source["scale_factor"]
    require(float(scale) == .01 if smoke else scale in (10, 30, 100, 300) if large else scale == 1,
            "wrong wide-file source scale")
    normal, repacked = (tables[t] for t in ("wide.clustered", "wide.files4096"))
    names = [f["name"] for f in normal["schema"]["fields"]]
    require(names == list(oracle.ORIGINAL + oracle.PAYLOADS), "wide pair must preserve all 80 columns")
    for table in tables.values():
        require(table["scale_factor"] == scale and table["rows"] == source["rows"] and table["schema"] == normal["schema"],
                "wide organizations differ in scale, rows or schema")
        dv = table["id"].endswith(".dv")
        require(table["deletion_vectors"] is dv and type(table["snapshot_version"]) is int
                and table["snapshot_version"] == int(dv), "wrong wide-file snapshot")
    organizations.geometry(repacked, 4096)
    sizes = sorted(f["bytes"] for f in repacked["files"])
    predicates = oracle.conditions("eq2-in20", source["in_literals"])
    candidates = [f["path"] for f in repacked["files"] if oracle.candidate(f["delta_stats"], predicates)]
    require(0 < len(candidates) <= 40, "wide pair needs at least 99% independently excludable files")
    require(not large or smoke or median(sizes) >= 64 * 1024**2, "large 4096-file median is below 64 MiB")
    return {"source_scale": float(scale), "physical_rows": source["rows"], "files": 4096,
            "file_bytes_sorted": sizes, "median_file_bytes": median(sizes),
            "candidate_files": candidates, "excluded_files": sorted({f["path"] for f in repacked["files"]} - set(candidates)),
            "scope": "smoke geometry; not large-data acceptance" if smoke else "large geometry" if large else "SF1 mechanism control"}


def extra_keys(fixtures, manifest):
    """Read both organizations fully, then derive minima without Delta pruning."""
    tables = {t["id"]: t for t in manifest["tables"]}
    pair = manifest["wide_file_pair"]
    large = pair["kind"] == "large"
    geometry(manifest, large=large, smoke=manifest["profile"] == "smoke")
    normal, repacked = (tables[t] for t in ("wide.clustered", "wide.files4096"))
    rows = organizations.same_rows(organizations.batches(fixtures, normal), organizations.batches(fixtures, repacked))
    require(rows == normal["rows"] == repacked["rows"], "wide repack lost rows")
    groups = ["wide.clustered", "wide.files4096"] if large else ["wide.files4096"]
    require(pair["shared_organizations"] == groups, "wrong deletion-union organizations")
    predicates = oracle.conditions("eq2-in20", manifest["sources"][0]["in_literals"])
    extra, matching = set(), set()
    for name in groups:
        table = tables[name]
        oracle.objects(fixtures, table, None)
        for item in table["files"]:
            minimum = None
            for row in oracle.full_rows(oracle.inside(fixtures, str(Path(table["path"]) / item["path"])), oracle.ORIGINAL + oracle.PAYLOADS):
                key = tuple(row[c] for c in oracle.KEYS)
                if oracle.prefix_matches(row, predicates) == len(predicates):
                    matching.add(key)
                else:
                    minimum = key if minimum is None else min(minimum, key)
            require(minimum is not None, "file has no nonmatching logical key")
            extra.add(minimum)
    require(matching, "wide file pair has no matching row")
    extra.add(min(matching))
    require(any(key not in extra and not oracle.deleted(dict(zip(oracle.KEYS, key))) for key in matching),
            "wide file pair has no surviving matching row")
    require(pair["extra_logical_keys"] == [list(k) for k in sorted(extra)], "saved deletion union differs from independent file minima")
    for table in tables.values():
        if table["deletion_vectors"]:
            require(table["deletion_summary"]["extra_logical_keys"] == pair["extra_logical_keys"], "DV organizations use different deletion sets")
    return extra


def coverage(table, candidates, matching):
    all_files = {f["path"] for f in table["files"]}
    dv_files = {f["path"] for f in table["files"] if f.get("deletion_vector", {}).get("physical_ordinals")}
    if table["deletion_vectors"]:
        require(dv_files == all_files, "wide pair requires nonempty real DVs on every file")
    groups = {"candidate": set(candidates), "excluded": all_files - set(candidates), "matching": set(matching)}
    return {name: {"files": len(files), "dv_files": len(files & dv_files),
                   "coverage": len(files & dv_files) / len(files) if files else None}
            for name, files in groups.items()}


def prepare(fixtures, output, workload=None):
    """Prepare paired exact references before any native reader measurements."""
    import large_workloads as large
    from run import save
    manifest = oracle.load_json(fixtures / "manifest.json")
    is_large = manifest["wide_file_pair"]["kind"] == "large"
    require(is_large == (workload is not None), "large organizations require an explicit files workload")
    if is_large:
        frozen = large.load(workload)
        require(frozen.get("family") == "files", "expected file-organization workload")
        cases = list(large.definitions("files"))
    else:
        cases = LEGACY_CASES
    shape = geometry(manifest, large=is_large, smoke=manifest["profile"] == "smoke")
    output.mkdir()
    references = []
    for case in cases:
        print("preparing " + case, flush=True)
        references.append(oracle.prepare(fixtures, case, output / case, workload=workload))
    for suffix in ("", ".dv"):
        group = [r for r in references if r["case_id"].endswith(".dv") == bool(suffix)]
        require(len({r["reference_sha256"] for r in group}) == 1, "file organizations have different exact live results")
    comparison = large.identity(workload) if workload else {"comparison_revision": 2, "protocol_sha256": large.digest(large.PROTOCOL)}
    result = {"status": "complete", **comparison, "geometry": shape,
              "fixture_manifest_sha256": oracle.digest_file(fixtures / "manifest.json"),
              "cases": [{k: r[k] for k in ("case_id", "source_rows", "output_rows", "physical_qualifying_rows",
                        "deleted_qualifying_rows", "candidate_files", "matching_files", "wide_file_geometry", "reference_sha256")}
                        | {"reference_metadata_sha256": oracle.digest_file(output / r["case_id"] / "reference.json")}
                        for r in references]}
    save(output / "wide-files.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workload", type=Path)
    args = parser.parse_args()
    prepare(args.fixtures.resolve(), args.output.resolve(), args.workload)
