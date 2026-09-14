"""Run the vendored Rust frontend on the fixed corpus using real Delta tables."""

import argparse
import hashlib
import json
import os
import subprocess
from collections import Counter, defaultdict
from datetime import date, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from reference import ROOT, corpus_hash, encode, load_corpus, verify_seeds
from inventory import source_hash


def arrow_type(kind):
    if isinstance(kind, str):
        return {"integer": pa.int32(), "long": pa.int64(), "string": pa.string(), "date": pa.date32()}[kind]
    if kind["type"] == "struct":
        return pa.struct([pa.field(f["name"], arrow_type(f["type"]), f["nullable"]) for f in kind["fields"]])
    if kind["type"] == "array":
        return pa.list_(pa.field("element", arrow_type(kind["elementType"]), kind["containsNull"]))
    if kind["type"] == "map":
        return pa.map_(arrow_type(kind["keyType"]),
                       pa.field("value", arrow_type(kind["valueType"]), kind["valueContainsNull"]))
    raise ValueError(f"unsupported fixture type: {kind}")


def fixture_table(table):
    schema = pa.schema(arrow_type(table["schema"]))
    rows = [{field.name: date.fromisoformat(value) if value is not None and pa.types.is_date(field.type) else value
             for field, value in zip(schema, row, strict=True)} for row in table["rows"]]
    return pa.Table.from_pylist(rows, schema=schema)


def write_delta(path, name, table):
    """Use the previous probe's real-Parquet fixture layout with the frozen inputs."""
    path.mkdir(parents=True)
    data = fixture_table(table)
    partitions = ["region"] if name == "t" else []
    actions = [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
               {"metaData": {"id": f"sail-extraction-{name}", "format": {"provider": "parquet", "options": {}},
                             "schemaString": json.dumps(table["schema"]), "partitionColumns": partitions,
                             "configuration": {}, "createdTime": 1587968585495}}]
    groups = defaultdict(list)
    for index, row in enumerate(data.to_pylist()):
        groups[tuple(row[key] for key in partitions)].append(index)
    for index, (key, indices) in enumerate(groups.items()):
        batch = data.take(indices)
        file = path / f"part-{index}.parquet"
        pq.write_table(batch.drop(partitions), file)
        ids = batch["id"].to_pylist()
        stats = {"numRecords": len(indices), "minValues": {"id": min(ids)}, "maxValues": {"id": max(ids)},
                 "nullCount": {"id": batch["id"].null_count}}
        actions.append({"add": {"path": file.name, "partitionValues": dict(zip(partitions, key, strict=True)),
                                "size": file.stat().st_size, "modificationTime": 1587968586000,
                                "dataChange": True, "stats": json.dumps(stats)}})
    (path / "_delta_log").mkdir()
    (path / "_delta_log/00000000000000000000.json").write_text("\n".join(map(json.dumps, actions)) + "\n")


def spark_type(kind):
    # Compare logical types while preserving Arrow's physical representation separately.
    if pa.types.is_dictionary(kind):
        return spark_type(kind.value_type)
    if pa.types.is_struct(kind):
        return {"type": "struct", "fields": [spark_field(field) for field in kind]}
    if pa.types.is_list(kind) or pa.types.is_large_list(kind) or pa.types.is_fixed_size_list(kind):
        return {"type": "array", "elementType": spark_type(kind.value_type), "containsNull": kind.value_field.nullable}
    if pa.types.is_map(kind):
        return {"type": "map", "keyType": spark_type(kind.key_type), "valueType": spark_type(kind.item_type),
                "valueContainsNull": kind.item_field.nullable}
    if pa.types.is_decimal(kind):
        return f"decimal({kind.precision},{kind.scale})"
    if pa.types.is_timestamp(kind):
        if kind.unit != "us":
            raise ValueError(f"timestamp precision needs explicit comparison: {kind}")
        return "timestamp" if kind.tz else "timestamp_ntz"
    if pa.types.is_string(kind) or pa.types.is_large_string(kind) or pa.types.is_string_view(kind):
        return "string"
    if pa.types.is_binary(kind) or pa.types.is_large_binary(kind) or pa.types.is_binary_view(kind):
        return "binary"
    return {pa.null(): "void", pa.bool_(): "boolean", pa.int8(): "byte", pa.int16(): "short", pa.int32(): "integer",
            pa.int64(): "long", pa.float32(): "float", pa.float64(): "double", pa.date32(): "date"}[kind]


def spark_field(field):
    metadata = {key.decode(): value.decode() for key, value in (field.metadata or {}).items()}
    return {"name": field.name, "type": spark_type(field.type), "nullable": field.nullable, "metadata": metadata}


def spark_value(value, kind):
    if value is None:
        return None
    if pa.types.is_dictionary(kind):
        return spark_value(value, kind.value_type)
    if pa.types.is_struct(kind):
        return [spark_value(value[field.name], field.type) for field in kind]
    if pa.types.is_list(kind) or pa.types.is_large_list(kind) or pa.types.is_fixed_size_list(kind):
        return [spark_value(item, kind.value_type) for item in value]
    if pa.types.is_map(kind):
        entries = [[spark_value(key, kind.key_type), spark_value(item, kind.item_type)] for key, item in value]
        return {"map_entries": sorted(entries, key=lambda entry: json.dumps(entry[0], sort_keys=True))}
    if pa.types.is_timestamp(kind) and kind.tz:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return encode(value)


def observation(table):
    columns = [[spark_value(value, field.type) for value in column.to_pylist()]
               for field, column in zip(table.schema, table.columns, strict=True)]
    return {"schema": {"type": "struct", "fields": [spark_field(field) for field in table.schema]},
            "rows": [list(row) for row in zip(*columns, strict=True)] if columns else [[] for _ in range(table.num_rows)]}


def read_arrow(path):
    with pa.ipc.open_stream(path) as reader:
        return observation(reader.read_all())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True, help="New directory for fixtures and observations")
    args = parser.parse_args()
    if pa.__version__ != "25.0.1":
        raise ValueError(f"expected PyArrow 25.0.1, found {pa.__version__}")
    inputs, cases = load_corpus()
    run = args.run_dir.resolve()
    run.mkdir(parents=True)  # Refuse to reuse stale captures or fixture files.
    for name, table in inputs["tables"].items():
        write_delta(run / "delta" / name, name, table)
    files_before = {str(path.relative_to(run)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in (run / "delta").rglob("*") if path.is_file()}
    env = os.environ.copy()
    # The Rust process must not need Python discovery or a Python library path.
    env.pop("LD_LIBRARY_PATH", None)
    env["PATH"] = "/nonexistent"
    env["PYO3_PYTHON"] = "/nonexistent/no-python"
    subprocess.run([str(args.binary.resolve()), str(run)], env=env, check=True)
    files_after = {str(path.relative_to(run)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in (run / "delta").rglob("*") if path.is_file()}
    assert files_before == files_after, "query runner changed fixture files"
    raw = json.loads((run / "rust-observations.json").read_text())
    assert [item["id"] for item in raw] == [case["id"] for case in cases], "missing/reordered/unknown case"
    input_captures = {name: read_arrow(run / f"input-{name}.arrow") for name in inputs["tables"]}
    observations = []
    checks = []
    for case, result in zip(cases, raw, strict=True):
        actual = result["actual"]
        if actual["status"] == "ok":
            actual.update(read_arrow(run / f"{case['id']}.arrow"))
        observations.append({"id": case["id"], "settings": result["settings"], **actual})
        if case["family"] == "excluded":
            checks.append({"id": case["id"], "check": "rejected_before_execution", "passed": actual["status"] == "planning_error"})
        if "seed_arrow_types" in case:
            checks.append({"id": case["id"], "check": "physical_arrow_types", "passed": actual.get("arrow_types") == case["seed_arrow_types"]})
        if case["id"] == "qualified_table":
            checks.append({"id": case["id"], "check": "native_qualified_table", "passed": actual.get("rows") == case["seed_rows"]})
        if case["id"] in {"delta_filter_projection", "delta_self_join"}:
            required = 2 if case["id"] == "delta_self_join" else 1
            checks.append({"id": case["id"], "check": "delta_scan", "passed": actual.get("delta_scans") == required})
        if case["id"] == "delta_filter_projection":
            checks.append({"id": case["id"], "check": "pruning", "passed":
                           [actual.get(key) for key in ["files_planned", "files_excluded", "scan_rows"]] == [1, 1, 1]})
    capture = {"engine": "extracted", "reference": {"distribution": "vendored-sail", "version": "0.7.1"},
               "source_sha256": source_hash(), "cargo_lock_sha256": hashlib.sha256((ROOT / "Cargo.lock").read_bytes()).hexdigest(),
               "corpus_sha256": corpus_hash(inputs, cases), "input_schemas": {name: v["schema"] for name, v in input_captures.items()},
               "input_rows": {name: v["rows"] for name, v in input_captures.items()}, "observations": observations}
    (run / "capture.json").write_text(json.dumps(capture, indent=2) + "\n")
    (run / "adapter-checks.json").write_text(json.dumps(checks, indent=2) + "\n")
    print(f"Captured {len(observations)} queries: {dict(Counter(item['status'] for item in observations))}")
    verify_seeds(cases, observations)
    failures = [check for check in checks if not check["passed"]]
    assert not failures, failures
    print(f"Passed {len(checks)} adapter checks; Spark compatibility is checked separately with oracle.py")


if __name__ == "__main__":
    main()
