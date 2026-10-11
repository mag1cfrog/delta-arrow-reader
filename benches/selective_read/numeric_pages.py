"""Generate and compare numeric-page fixtures, separate from the revision-6 corpus."""

import argparse
from array import array
import json
import os
from pathlib import Path
import random
import signal
import statistics
import subprocess

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import network
import storage
from storage import digest, save

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


def run_reader(command, *, env=None, stdout=None, stderr=None, timeout=180):
    process = subprocess.Popen(command, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
    try:
        code = process.wait(timeout=timeout)
        if code:
            raise subprocess.CalledProcessError(code, command)
    finally:
        # Killing only the launcher leaves its timed reader using shared resources.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)


def measure(args):
    os.sched_setaffinity(0, storage.state(args.state)["cpus"]["observer"])
    manifest = json.loads((args.fixture / "manifest.json").read_text())
    assert manifest["format"] == "numeric-page-fixture-v1"
    assert digest(args.fixture / "_delta_log/00000000000000000000.json") == manifest["log_sha256"]
    for file in manifest["files"]:
        assert digest(args.fixture / file["path"]) == file["sha256"]
    ref = manifest["references"][args.type]
    assert digest(args.fixture / ref["path"]) == ref["sha256"]
    if args.selection == "dense":
        expected = pq.read_table([args.fixture / f["path"] for f in manifest["files"]],
            columns=["row_id"] + [f"{args.type}_{i}" for i in range(8)])
    else:
        expected = pa.ipc.open_stream(args.fixture / ref["path"]).read_all()
    args.output.mkdir()
    env = storage.reader_environment(args.state)
    env.pop("RUST_LOG", None)
    env.pop("NUMERIC_PAGE_VALIDATE", None)
    builds = {name: digest(binary) for name, binary in (("baseline", args.baseline), ("candidate", args.candidate))}
    save(args.output / "experiment.json", {"fixture_sha256": digest(args.fixture / "manifest.json"),
        "binaries": builds, "type": args.type, "selection": args.selection, "table_uri": args.table,
        "network": None if args.table.startswith("file:") else network.config(args.state),
        "driver_sha256": digest(Path(__file__)),
        "primary_metric": "first fully consumed query after initialization", "pairs": 3})
    samples = []
    with storage.exclusive(args.state):
        storage.verify_server(args.state)
        for phase, repetition in [("validation", 0)] + [("timing", n) for n in range(3)]:
            order = ("baseline", "candidate") if repetition % 2 == 0 else ("candidate", "baseline")
            for name in order:
                slot = args.output / f"{name}-{phase}-{repetition}"
                slot.mkdir()
                current_env = env | ({"NUMERIC_PAGE_VALIDATE": "1",
                    "RUST_LOG": "delta_arrow_reader::diagnostics::intra_page=debug"} if phase == "validation" else {})
                network.control(args.state, reset=True, traced=phase == "validation")
                command = storage.reader_prefix(args.state) + ["/usr/bin/time", "-f", "%U %S %M", "-o", str(slot / "resources.txt"),
                    str(getattr(args, name)), args.table, args.type, args.selection, str(slot / "reader")]
                with (slot / "stdout.json").open("x") as stdout, (slot / "stderr.log").open("x") as stderr:
                    run_reader(command, env=current_env, stdout=stdout, stderr=stderr)
                capture = network.finish(args.state, slot)
                record = json.loads((slot / "reader/record.json").read_text())
                assert record["settings"]["warmup"] == "Automatic"
                assert "experimental_intra_page_reads: true" in record["settings"]["scan_options"]
                assert all(q["rows"] == expected.num_rows for q in record["queries"])
                if phase == "validation":
                    for query in range(2):
                        check(pa.ipc.open_stream(slot / f"reader/{query}.arrow").read_all(), expected)
                    save(slot / "correctness.json", {"status": "passed", "comparison": "independent Arrow reference, exact bits"})
                sample = {"build": name, "phase": phase, "repetition": repetition, "record": record, "network": capture}
                save(slot / "sample.json", sample)
                samples.append(sample)
                print(name, phase, repetition, [round(q["seconds"], 3) for q in record["queries"]], flush=True)
    summary = {name: statistics.median(s["record"]["queries"][0]["seconds"] for s in samples
              if s["build"] == name and s["phase"] == "timing") for name in builds}
    save(args.output / "summary.json", {"samples": samples, "first_query_medians_seconds": summary})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate_args = commands.add_parser("generate")
    generate_args.add_argument("--output", type=Path, required=True)
    generate_args.add_argument("--decimals", action="store_true", help="generate fixed-length decimal columns instead of the original numeric fixture")
    run_args = commands.add_parser("run")
    for name in ("fixture", "state", "baseline", "candidate", "output"):
        run_args.add_argument("--" + name, type=lambda p: Path(p).resolve(), required=True)
    run_args.add_argument("--table", required=True)
    run_args.add_argument("--type", choices=TYPES | DECIMAL_TYPES, required=True)
    run_args.add_argument("--selection", choices=("sparse", "dense"), default="sparse")
    args = parser.parse_args()
    generate(args.output, args.decimals) if args.command == "generate" else measure(args)
