"""Generate and compare numeric-page fixtures, separate from the revision-6 corpus."""

import argparse
from array import array
import json
from pathlib import Path
import random
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
from run import digest, save

TYPES = {"int32": pa.int32(), "int64": pa.int64(), "float": pa.float32(), "double": pa.float64()}
DECIMAL_TYPES = {"decimal20": pa.decimal128(20, 4), "decimal38": pa.decimal128(38, 10)}


def value_bytes(rng, dtype, rows):
    if pa.types.is_decimal(dtype):
        limit = 10**dtype.precision
        return b"".join(rng.randrange(1 - limit, limit).to_bytes(16, "little", signed=True) for _ in range(rows))
    return rng.randbytes(rows * dtype.bit_width // 8)


def generate(output, decimals=False):
    output.mkdir()
    types = DECIMAL_TYPES if decimals else TYPES
    fields = [pa.field("row_id", pa.int64(), False), pa.field("is_match", pa.int32(), False)]
    fields += [pa.field(f"{name}_{i}", dtype, i % 2 == 0) for name, dtype in types.items() for i in range(8)]
    schema = pa.schema(fields)
    files, references = [], {name: [] for name in types}
    for file in range(2):
        rows = 524288
        ids = range(file * rows, (file + 1) * rows)
        matches = pa.array([i % 8191 == 0 for i in ids])
        columns = [pa.array(array("q", ids)), pc.cast(matches, pa.int32())]
        for index, field in enumerate(fields[2:]):
            rng = random.Random(450 + file * 32 + index)
            valid = pa.py_buffer(rng.randbytes(rows // 8)) if field.nullable else None
            values = pa.py_buffer(value_bytes(rng, field.type, rows))
            columns.append(pa.Array.from_buffers(field.type, rows, [valid, values]))
        table = pa.Table.from_arrays(columns, schema=schema)
        path = output / f"part-{file}.parquet"
        pq.write_table(table, path, row_group_size=rows, compression="NONE", use_dictionary=False,
                       data_page_version="1.0", data_page_size=4 * 1024**2, max_rows_per_page=rows, write_page_index=True)
        files.append({"path": path.name, "bytes": path.stat().st_size, "sha256": digest(path)})
        for name in types:
            references[name].append(table.select(["row_id"] + [f"{name}_{i}" for i in range(8)]).filter(matches))
    log = output / "_delta_log"
    log.mkdir()
    delta_types = {pa.int32(): "integer", pa.int64(): "long", pa.float32(): "float", pa.float64(): "double"}
    delta_types.update({dtype: f"decimal({dtype.precision},{dtype.scale})" for dtype in DECIMAL_TYPES.values()})
    actions = [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}, {"metaData": {
        "id": "decimal-pages-451" if decimals else "numeric-pages-450", "format": {"provider": "parquet", "options": {}},
        "schemaString": json.dumps({"type": "struct", "fields": [{"name": f.name, "type": delta_types[f.type],
            "nullable": f.nullable, "metadata": {}} for f in fields]}), "partitionColumns": [], "configuration": {}}}]
    actions += [{"add": {"path": f["path"], "size": f["bytes"], "partitionValues": {}, "modificationTime": 0,
                         "dataChange": True}} for f in files]
    log_path = log / "00000000000000000000.json"
    log_path.write_text("".join(json.dumps(a) + "\n" for a in actions))
    refs = {}
    for name, tables in references.items():
        table = pa.concat_tables(tables)
        path = output / f"{name}.arrow"
        with pa.ipc.new_stream(path, table.schema) as writer:
            writer.write_table(table)
        refs[name] = {"path": path.name, "sha256": digest(path), "rows": table.num_rows}
    save(output / "manifest.json", {"format": "numeric-page-fixture-v1", "generator_sha256": digest(Path(__file__)),
         "pyarrow": pa.__version__, "files": files, "log_sha256": digest(log_path), "references": refs})


def check(actual, expected):
    actual = actual.sort_by("row_id").combine_chunks()
    expected = expected.sort_by("row_id").combine_chunks()
    assert actual.column_names == expected.column_names and actual.num_rows == expected.num_rows
    for left, right in zip(actual.columns, expected.columns, strict=True):
        left, right = left.chunk(0), right.chunk(0)
        assert left.type == right.type and left.is_null().equals(right.is_null())
        left, right = left.filter(left.is_valid()), right.filter(right.is_valid())
        if pa.types.is_floating(left.type):
            bits = pa.int32() if left.type == pa.float32() else pa.int64()
            left, right = left.view(bits), right.view(bits)
        assert left.equals(right), "values differ (floats compared by bits)"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate_args = commands.add_parser("generate")
    generate_args.add_argument("--output", type=Path, required=True)
    generate_args.add_argument("--decimals", action="store_true", help="generate fixed-length decimal columns instead of the original numeric fixture")
    args = parser.parse_args()
    generate(args.output, args.decimals)
