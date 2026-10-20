"""Generate and measure the isolated #452 Delta fixtures through the existing network proxy."""

import argparse
import json
from pathlib import Path
import random
import sys
import subprocess
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
from run import digest, save

CASES = {"long": 16384, "short": 65536, "selected-long": 8192,
    "selected-long-batch": 65536, "length-heavy": 262144,
    "mixed-bad-first": 65536, "mixed-alternating": 65536}


def value_size(case, row, column, rng):
    if case.startswith("mixed-"):
        page = row // 8192
        skewed = page < 4 if case == "mixed-bad-first" else (page + column) % 2 == 0
        return (524288 if row % 997 == 0 else row % 2) if skewed else 512 + rng.randrange(64)
    if case == "long":
        return 1536 + rng.randrange(1024)
    if case == "short":
        return 8 + rng.randrange(56)
    if case in ("selected-long", "selected-long-batch"):
        return 262144 if row % 997 == 0 else row % 2
    return rng.randrange(2)


def generate(output, case, binary, compression="NONE"):
    output.mkdir()
    rows = CASES[case]
    page_target = (8 if case.startswith("mixed-") else 4) * 1024**2
    fields = [pa.field("row_id", pa.int64(), False), pa.field("grouped", pa.int32(), False), pa.field("scattered", pa.int32(), False)]
    fields += [pa.field(f"payload_{i}", pa.string() if i % 4 < 2 else pa.binary(), i % 2 == 1) for i in range(8)]
    schema = pa.schema(fields)
    references = {shape: [] for shape in ("grouped", "scattered", "dense")}
    actions = [{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["deletionVectors"],
        "writerFeatures": ["deletionVectors", "invariants", "appendOnly"]}}, {"metaData": {
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, "delta-length-452/" + case)), "format": {"provider": "parquet", "options": {}},
        "schemaString": json.dumps({"type": "struct", "fields": [{"name": field.name, "type":
            {pa.int64(): "long", pa.int32(): "integer", pa.string(): "string", pa.binary(): "binary"}[field.type],
            "nullable": field.nullable, "metadata": {}} for field in fields]}), "partitionColumns": [], "configuration": {}}}]
    objects = []
    layouts = []
    for file in range(2):
        grouped = pa.array([int(row % 2048 < 8) for row in range(rows)], type=pa.int32())
        scattered = pa.array([int(row % 997 == 0) for row in range(rows)], type=pa.int32())
        columns = [pa.array(range(file * rows, (file + 1) * rows), type=pa.int64()), grouped, scattered]
        for i, field in enumerate(fields[3:]):
            rng = random.Random(452 + 16 * file + i)
            values = []
            for row in range(rows):
                size = value_size(case, row, i, rng)
                value = rng.randbytes(size)
                if pa.types.is_string(field.type):
                    value = value[:size // 2].hex() + ("\u00e9" if size > 2 else "")
                values.append(None if field.nullable and row % 7 == 0 else value)
            columns.append(pa.array(values, type=field.type))
        table = pa.Table.from_arrays(columns, schema=schema)
        path = output / f"part-{file}.parquet"
        pq.write_table(table, path, row_group_size=rows // 2, compression=compression,
            compression_level=3 if compression == "zstd" else None, use_dictionary=False,
            column_encoding={field.name: "DELTA_LENGTH_BYTE_ARRAY" for field in fields[3:]},
            data_page_version="1.0", data_page_size=page_target, max_rows_per_page=8192, write_page_index=True)
        layouts.append({"file": path.name, **json.loads(subprocess.check_output(
            [str(binary), "inspect", str(path)], text=True))})
        dv_id = uuid.uuid5(uuid.NAMESPACE_URL, f"delta-length-452/{case}/{file}")
        dv = output / f"deletion_vector_{dv_id}.bin"
        descriptor = json.loads(subprocess.check_output([str(binary), "dv", str(rows), str(dv_id), str(dv)], text=True))
        assert descriptor["cardinality"] == len(range(0, rows, 103))
        actions.append({"add": {"path": path.name, "size": path.stat().st_size, "partitionValues": {},
            "modificationTime": 0, "dataChange": True, "deletionVector": descriptor,
            "stats": json.dumps({"numRecords": rows, "tightBounds": False})}})
        objects.extend({"path": p.name, "bytes": p.stat().st_size, "sha256": digest(p)} for p in (path, dv))
        live = pa.array([row % 103 != 0 for row in range(rows)])
        projection = table.select(["row_id"] + [field.name for field in fields[3:]])
        for shape in references:
            selected = live if shape == "dense" else pc.and_(live, pc.equal(table[shape], 1))
            references[shape].append(projection.filter(selected))
    log = output / "_delta_log/00000000000000000000.json"
    log.parent.mkdir()
    log.write_text("".join(json.dumps(action) + "\n" for action in actions))
    objects.append({"path": str(log.relative_to(output)), "bytes": log.stat().st_size, "sha256": digest(log)})
    refs = {}
    for shape, tables in references.items():
        table = pa.concat_tables(tables)
        path = output / f"{shape}.arrow"
        with pa.ipc.new_stream(path, table.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
            writer.write_table(table)
        refs[shape] = {"path": path.name, "rows": table.num_rows, "sha256": digest(path)}
    save(output / "manifest.json", {"format": "delta-length-network-v1", "case": case, "rows_per_file": rows,
        "compression": compression, "compression_level": 3 if compression == "zstd" else None,
        "columns": 8, "seed": 452, "row_group_rows": rows // 2, "page_target_bytes": page_target, "page_row_limit": 8192,
        "pyarrow": pa.__version__, "generator_sha256": digest(Path(__file__)), "dv_writer_sha256": digest(binary),
        "objects": objects, "references": refs, "layout": layouts})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate")
    gen.add_argument("--case", choices=CASES, required=True)
    gen.add_argument("--compression", choices=("NONE", "zstd"), default="NONE")
    for name in ("output", "binary"):
        gen.add_argument("--" + name, type=lambda p: Path(p).resolve(), required=True)
    args = parser.parse_args()
    generate(args.output, args.case, args.binary, args.compression)
