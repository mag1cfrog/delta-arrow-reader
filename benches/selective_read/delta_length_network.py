"""Generate and measure the isolated #452 Delta fixtures through the existing network proxy."""

import argparse
import json
import os
from pathlib import Path
import random
import signal
import statistics
import subprocess
import time
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import network
import storage
from storage import digest, save

CASES = {"long": 16384, "short": 65536, "selected-long": 8192, "length-heavy": 262144}


def generate(output, case, binary, compression="NONE"):
    output.mkdir()
    rows = CASES[case]
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
                size = (1536 + rng.randrange(1024) if case == "long" else 8 + rng.randrange(56) if case == "short"
                    else (262144 if row % 997 == 0 else row % 2) if case == "selected-long" else rng.randrange(2))
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
            data_page_version="1.0", data_page_size=4 * 1024**2, max_rows_per_page=8192, write_page_index=True)
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
        "columns": 8, "seed": 452, "row_group_rows": rows // 2, "page_target_bytes": 4 * 1024**2, "page_row_limit": 8192,
        "pyarrow": pa.__version__, "generator_sha256": digest(Path(__file__)), "dv_writer_sha256": digest(binary),
        "objects": objects, "references": refs, "layout": layouts})


def invoke(args, mode, phase, repetition, arrays):
    slot = args.output / f"{mode}-{phase}-{repetition}-{arrays}"
    slot.mkdir()
    command = storage.reader_prefix(args.state) + ["/usr/bin/time", "-f", "%U %S %M", "-o", str(slot / "resources.txt"),
        str(args.binary), args.table, mode, args.selection, arrays, phase,
        str(args.output / "cost-profile.json"), str(slot / "reader")]
    network.control(args.state, reset=True, traced=phase == "validate")
    with (slot / "stdout.log").open("x") as stdout, (slot / "stderr.log").open("x") as stderr:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=stdout, stderr=stderr,
            env=storage.reader_environment(args.state), start_new_session=True)
        try:
            deadline = time.monotonic() + 180
            while not (slot / "reader/initialized.json").exists():
                if process.poll() is not None:
                    raise RuntimeError(f"initialization failed: {slot}")
                if time.monotonic() >= deadline:
                    raise TimeoutError("initialization deadline")
                time.sleep(.01)
            init = slot / "initialization"
            init.mkdir()
            network.finish(args.state, init)
            network.control(args.state, reset=True, traced=phase == "validate")
            process.stdin.write(b"\n")
            process.stdin.close()
            if process.wait(timeout=180):
                raise RuntimeError(f"query failed: {slot}")
        except BaseException as error:
            save(slot / "failure.json", {"error": repr(error), "returncode": process.poll()})
            raise
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
    capture = network.finish(args.state, slot)
    record = json.loads((slot / "reader/record.json").read_text())
    if phase == "validate":
        actual = pa.ipc.open_stream(slot / "reader/result.arrow").read_all().cast(args.expected.schema)
        # Both paths preserve file/row order; compare exact values and nulls without sorting large payloads.
        assert actual.equals(args.expected), f"query result differs: {slot}"
    assert record["output_rows"] == args.expected.num_rows
    sample = {"mode": mode, "phase": phase, "repetition": repetition, "arrays": arrays,
        "record": record, "query_network": capture, "process_resources": (slot / "resources.txt").read_text().strip()}
    save(slot / "sample.json", sample)
    print(mode, phase, repetition, arrays, round(record["query_seconds"], 3), flush=True)
    return sample


def measure(args):
    args.output.mkdir()
    manifest = json.loads((args.fixture / "manifest.json").read_text())
    assert manifest["format"] == "delta-length-network-v1"
    assert pa.__version__ == manifest["pyarrow"], "oracle Arrow version changed"
    prefix = digest(args.fixture / "manifest.json")
    assert args.table.rstrip("/") == f"s3://{storage.BUCKET}/{prefix}", "table must match the verified fixture"
    assert network.config(args.state) is not None, "a recorded network profile is required"
    profile = network.config(args.state)["profile"]
    assert profile["mbps"] > 0, "cost experiment requires a bounded bandwidth profile"
    save(args.output / "cost-profile.json", {"latency_ms": profile["latency_ms"] + profile["jitter_ms"],
        "mbps": profile["mbps"], "request_overhead_bytes": 0,
        "basis": "controlled proxy settings; upper jitter bound; optimistic zero request-processing cost, not automatic calibration"})
    for item in manifest["objects"]:
        assert digest(args.fixture / item["path"]) == item["sha256"]
    reference = manifest["references"][args.selection]
    assert digest(args.fixture / reference["path"]) == reference["sha256"]
    args.expected = pa.ipc.open_stream(args.fixture / reference["path"]).read_all()
    os.sched_setaffinity(0, storage.state(args.state)["cpus"]["observer"])
    save(args.output / "experiment.json", {"fixture_sha256": digest(args.fixture / "manifest.json"),
        "binary_sha256": digest(args.binary), "driver_sha256": digest(Path(__file__)), "network": network.config(args.state),
        "source_sha256": {name: digest(Path(__file__).parent / name) for name in (
            "src/bin/delta-length-query.rs", "src/delta_length_reader.rs", "src/delta_length.rs", "Cargo.lock", "storage.py",
            "../../src/reader/backend/direct_parquet/range_planning.rs")},
        "cost_profile_sha256": digest(args.output / "cost-profile.json"),
        "selection": args.selection, "pairs": 3, "primary_metric": "first fully consumed query including planning, predicate and DV",
        "acceptance": "all three pairs improve beyond the larger within-mode range; investigate >5% median or consistent smaller regressions",
        "policy": "uncompressed flat V1; >=1 MiB pages; <=1/8 selected rows; >=64 bytes/row; discovery <=10% modeled ordinary batch cost; >=25% byte saving and >10% modeled remaining-read saving after discovery"})
    try:
        with storage.exclusive(args.state):
            storage.verify_server(args.state)
            samples = [invoke(args, mode, "validate", 0, arrays) for arrays in ("arrays", "views") for mode in ("ordinary", "experimental")]
            for repetition in range(3):
                order = ("ordinary", "experimental") if repetition % 2 == 0 else ("experimental", "ordinary")
                samples.extend(invoke(args, mode, "timing", repetition, "arrays") for mode in order)
    except BaseException as error:
        save(args.output / "failure.json", {"error": repr(error)})
        raise
    medians = {mode: statistics.median(s["record"]["query_seconds"] for s in samples
        if s["phase"] == "timing" and s["mode"] == mode) for mode in ("ordinary", "experimental")}
    save(args.output / "summary.json", {"samples": samples, "medians_seconds": medians})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate")
    gen.add_argument("--case", choices=CASES, required=True)
    gen.add_argument("--compression", choices=("NONE", "zstd"), default="NONE")
    for name in ("output", "binary"):
        gen.add_argument("--" + name, type=lambda p: Path(p).resolve(), required=True)
    run = commands.add_parser("run")
    for name in ("output", "binary", "state", "fixture"):
        run.add_argument("--" + name, type=lambda p: Path(p).resolve(), required=True)
    run.add_argument("--table", required=True)
    run.add_argument("--selection", choices=("grouped", "scattered", "dense"), required=True)
    args = parser.parse_args()
    generate(args.output, args.case, args.binary, args.compression) if args.command == "generate" else measure(args)
