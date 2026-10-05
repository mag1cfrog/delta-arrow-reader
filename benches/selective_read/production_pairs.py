"""Prepare immutable Q2/Q4 DV pairs with the existing portable DV writer."""

import argparse
import errno
import json
import os
from pathlib import Path
import shutil
import subprocess

import oracle
import production_shapes as shapes
from production_workloads import query_fields
from run import digest, fixture_tables, save


def table_objects(table):
    logs = [table["delta_log"]] if table["delta_log"] else []
    return table["files"] + logs + [f["geometry"] for f in table["files"] if "geometry" in f]


def capacity(fixtures, transfers, output, limit):
    # Retained inputs include source copies, manifests and geometry, once per inode.
    retained = {}
    for root in fixtures:
        for path in root.rglob("*"):
            if path.is_file():
                stat = path.stat()
                retained[stat.st_dev, stat.st_ino] = max(stat.st_size, stat.st_blocks * 512)
    resident = sum(retained.values())
    destination_device = output.parent.stat().st_dev
    copied = 0
    for root, table in transfers:
        for item in table_objects(table):
            path = oracle.inside(root, str(Path(table["path"]) / item["path"]))
            stat = path.stat()
            oracle.require(stat.st_size == item["bytes"], "fixture object size changed")
            if stat.st_dev != destination_device:
                copied += stat.st_size
    reserve = resident // 4 + 256 * 1024**2
    additional = copied + reserve
    oracle.require(resident + additional <= limit and additional <= shutil.disk_usage(output.parent).free,
                   "production DV pair exceeds its disk ceiling")
    return {"retained_input_bytes": resident, "copied_input_bytes": copied,
            "metadata_reserve_bytes": reserve, "conservative_bytes": resident + additional,
            "additional_disk_bytes": additional, "immutable_hardlinks": copied == 0}


def prepare(fixtures, binary, output, disk_gib, seconds):
    oracle.require(0 < disk_gib <= 192 and seconds > 0, "finite production pairing ceilings required")
    bases, parents, source, modes, source_hashes, definitions = {}, [], None, set(), set(), {}
    for root in fixtures:
        manifest = json.loads((root / "manifest.json").read_text())
        oracle.require(manifest["protocol"] == "selective-read-production-fixtures-v1", "expected original production fixtures")
        modes.add(manifest["mode"])
        source_hashes.add(manifest["source_parent_manifest_sha256"])
        parents.append({"path": str(root.resolve()), "manifest_sha256": digest(root / "manifest.json"),
                        "writer": manifest["writer"]["generator"]})
        for table in fixture_tables(manifest):
            query_fields(table["id"], manifest)
            oracle.require(table["id"] not in bases and not table["deletion_vectors"], "duplicate/non-base table")
            bases[table["id"]] = root, table
        name, definition = manifest["shape"], manifest["shape_definition"]
        oracle.require(name not in definitions or definitions[name] == definition, "paired layout definitions differ")
        definitions[name] = definition
        current = manifest["sources"][0]
        oracle.require(source is None or source[1] == current, "source inventories differ")
        source = root, current
    expected = {c for c, v in shapes.cases().items() if not v["deletion_vectors"] and v["shape"] in definitions}
    oracle.require(bases and set(bases) == expected and len(modes) == len(source_hashes) == 1,
                   "pair both layouts of each included shape from one source and scope")
    phase = capacity(fixtures, [source, *bases.values()], output, disk_gib * 1024**3)
    output.mkdir()
    save(output / "attempt.json", {"status": "preparing", "disk_limit_bytes": disk_gib * 1024**3,
                                   **phase, "parents": parents})
    copy_remaining = phase["copied_input_bytes"]
    def copy(root, table):
        nonlocal copy_remaining
        for item in table_objects(table):
            name = str(Path(table["path"]) / item["path"])
            path = oracle.verify_object(root, {**item, "path": name})
            target = output / name
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(path, target)
            except OSError as error:
                if error.errno != errno.EXDEV:
                    raise
                oracle.require(item["bytes"] <= copy_remaining, "unbudgeted cross-filesystem copy")
                copy_remaining -= item["bytes"]
                shutil.copyfile(path, target)
    copy(*source)
    tables = []
    for case, (root, table) in sorted(bases.items()):
        copy(root, table)
        shape = shapes.cases()[case]["shape"]
        tables.append(table | {"queries": {case: definitions[shape]["canonical_sql"]}})
    request = {"format": "selective-read-production-pairs-request-v1", "contract_sha256": digest(shapes.CONTRACT),
               "mode": next(iter(modes)), "tables": tables, "output_limit_bytes": disk_gib * 1024**3}
    save(output / "dv-request.json", request)
    subprocess.run([str(binary.resolve()), "production-pairs", str((output / "dv-request.json").resolve()),
                    str(output.resolve()), str(seconds)], check=True, timeout=seconds + 30)
    result = json.loads((output / "dv-result.json").read_text())
    oracle.require(result["status"] == "complete" and result["request_sha256"] == digest(output / "dv-request.json")
                   and result["generator"]["executable_sha256"] == digest(binary), "DV writer identity changed")
    manifest = {"protocol": "selective-read-production-pairs-v1", "status": "complete", "mode": modes.pop(),
                "contract_sha256": digest(shapes.CONTRACT), "source_parent_manifest_sha256": source_hashes.pop(),
                "parents": parents, "sources": [source[1]], "shape_definitions": definitions,
                "production_dv": {"base_case_ids": sorted(bases), "extra_logical_keys": result["extra_logical_keys"]},
                "writer": {"status": "complete", "generator": result["generator"], "tables": result["base_tables"] + result["tables"]},
                "dv_result_sha256": digest(output / "dv-result.json"), "driver_sha256": digest(Path(__file__)),
                "native_campaign_ready": False, "publication_ready": False}
    save(output / "manifest.json", manifest)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, action="append", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--disk-limit-gib", type=int, required=True)
    parser.add_argument("--elapsed-limit-seconds", type=int, default=1800)
    args = parser.parse_args()
    value = prepare(args.fixtures, args.binary, args.output, args.disk_limit_gib, args.elapsed_limit_seconds)
    print(json.dumps({"status": value["status"], "tables": len(value["writer"]["tables"])}))
