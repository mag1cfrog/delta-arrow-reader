"""Independent checks for the wide normal/4096-file pairs and their deletion union."""

from pathlib import Path
from decimal import Decimal

import file_organizations as organizations
import native
import oracle
from oracle import require

def geometry(manifest, *, large, smoke):
    tables = []
    columns = ("l_shipdate", "l_shipmode", "l_partkey")
    for table in manifest["tables"]:
        metadata = {k: table[k] for k in ("id", "rows", "scale_factor", "schema", "snapshot_version", "deletion_vectors")}
        if table["id"] == "wide.files4096":
            metadata.update(organizations.boundaries(table))
            for item, original in zip(metadata["files"], table["files"], strict=True):
                item.update({k: original[k] for k in ("path", "bytes")})
                item["delta_stats"].update({key: {column: original["delta_stats"][key][column] for column in columns}
                                          for key in ("minValues", "maxValues", "nullCount")})
        tables.append(metadata)
    sources = [{k: source[k] for k in ("rows", "scale_factor", "in_literals")} for source in manifest["sources"]]
    for item in tables + sources:
        if isinstance(item["scale_factor"], Decimal):
            item["scale_factor"] = float(item["scale_factor"])
    return native.call("workload", {"action": "wide-geometry", "large": large, "smoke": smoke,
        "manifest": {"wide_file_pair": {"kind": manifest["wide_file_pair"]["kind"]}, "tables": tables, "sources": sources}})


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
