#!/usr/bin/env python3
"""Derive the fixed partitioned snapshot from completed SF1 wide.clustered bytes."""

import hashlib
import json
import os
import pathlib
import sys
import uuid


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def prepare(base, destination, receipt):
    base, destination, receipt = map(lambda value: pathlib.Path(value).resolve(), (base, destination, receipt))
    repository = pathlib.Path(__file__).resolve().parents[4]
    if destination.is_relative_to(repository) or destination.exists() or receipt.exists():
        raise ValueError("choose a new snapshot outside Git and a new receipt destination")
    manifest = json.loads((base / "manifest.json").read_text())
    if manifest["status"] != "complete" or not any(source["scale_factor"] == 1 for source in manifest["sources"]):
        raise ValueError("a completed SF1 source is required")
    table = next(table for table in manifest["tables"] if table.get("id") == "wide.clustered")
    if len(table["schema"]["fields"]) != 80:
        raise ValueError("expected the original 80-column wide table")
    table_root = base / table["path"]
    log = table_root / "_delta_log/00000000000000000000.json"
    if digest(log) != table["delta_log"]["sha256"]:
        raise ValueError("base Delta log checksum differs")
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    metadata = next(action["metaData"] for action in actions if "metaData" in action)
    schema = json.loads(metadata["schemaString"])
    if metadata["partitionColumns"] or len(schema["fields"]) != 80:
        raise ValueError("expected the original unpartitioned schema")
    schema["fields"].append({"name":"file_bucket", "type":"integer", "nullable":False, "metadata":{}})
    metadata["schemaString"] = json.dumps(schema, separators=(",", ":"), sort_keys=True)
    metadata["partitionColumns"] = ["file_bucket"]
    metadata["id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, "dar-execution-1380/sf1/wide.clustered/file_bucket/v1"))
    files = {file["path"]:file for file in table["files"]}
    adds = sorted((action["add"] for action in actions if "add" in action), key=lambda add:add["path"])
    if {add["path"] for add in adds} != set(files) or sum(file["bytes"] for file in files.values()) > 32 * 1024**3:
        raise ValueError("base files differ from the completed manifest or exceed preparation bound")
    destination.mkdir(parents=True)
    mapping = []
    for number, add in enumerate(adds):
        relative = pathlib.PurePosixPath(add["path"])
        if relative.is_absolute() or ".." in relative.parts or relative.suffix != ".parquet":
            raise ValueError("unexpected native-writer file path")
        source = table_root / relative
        expected = files[add["path"]]
        if source.stat().st_size != expected["bytes"] or digest(source) != expected["sha256"]:
            raise ValueError("base Parquet checksum differs: " + add["path"])
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, target)
        add["partitionValues"] = {"file_bucket":str(number)}
        mapping.append({"path":add["path"],"file_bucket":number,"rows":expected["rows"],"bytes":expected["bytes"],"sha256":expected["sha256"]})
    derived_log = destination / "_delta_log/00000000000000000000.json"
    derived_log.parent.mkdir()
    derived_log.write_text("".join(json.dumps(action, separators=(",", ":"), sort_keys=True) + "\n" for action in actions))
    result = {"status":"complete","source_scale_factor":1,"source_commit":"8954dcec63f934359c7bcdb74b5220c842d898e4","source_table":"wide.clustered","stored_columns":80,"snapshot_columns":81,"snapshot_version":0,"rows":table["rows"],"files":mapping,"data_bytes":sum(file["bytes"] for file in mapping),"base_root":str(base),"snapshot_root":str(destination),"source_manifest_sha256":digest(base/"manifest.json"),"base_delta_log_sha256":digest(log),"derived_delta_log_sha256":digest(derived_log),"generator":manifest["generator"],"derivation":"unchanged hardlinked Parquet; virtual INT file_bucket is path-sorted file ordinal; all original rows/statistics retained"}
    receipt.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key:result[key] for key in ["rows","stored_columns","snapshot_columns","data_bytes","derived_delta_log_sha256"]}))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: prepare.py BASE_SF1 NEW_SNAPSHOT NEW_RECEIPT")
    prepare(*sys.argv[1:])
