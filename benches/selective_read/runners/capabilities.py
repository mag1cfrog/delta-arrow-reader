"""Shared bounded Delta snapshot/DV checks, independent of the reader engine."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import oracle
import run

CORPUS = Path(__file__).resolve().parents[3] / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus"


def check_cli(check, description):
    parser = argparse.ArgumentParser(description=description)
    for name in ("binary", "fixtures", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    check(args.binary, args.fixtures, args.output)


def stream_fixture(output):
    table = output / "no-dv"
    schema = pq.ParquetFile(next(table.glob("*.parquet"))).schema_arrow
    stream_table = output / "stream-table"
    (stream_table / "_delta_log").mkdir(parents=True)
    actions = [json.loads(line) for line in (table / "_delta_log/00000000000000000000.json").read_text().splitlines()]
    actions = [a for a in actions if "protocol" in a or "metaData" in a]
    for index in range(32):
        path = stream_table / f"{index:03d}.parquet"
        data = pa.Table.from_pydict({"id": range(8192), "value": [1] * 8192,
                                    "label": ["1" if index < 31 else "bad"] * 8192}, schema=schema)
        pq.write_table(data, path)
        actions.append({"add": {"path": path.name, "size": path.stat().st_size, "partitionValues": {},
                                "modificationTime": 0, "dataChange": True}})
    (stream_table / "_delta_log/00000000000000000000.json").write_text("".join(json.dumps(a) + "\n" for a in actions))


def corrupt_export(source, output):
    with pa.ipc.open_stream(source) as stream:
        values = stream.read_all()
    rows = values.to_pylist()
    rows[0]["l_orderkey"] += 1
    with output.open("xb") as sink, pa.ipc.new_stream(sink, values.schema) as writer:
        writer.write_table(pa.Table.from_pylist(rows, schema=values.schema))


def probe(binary, payload, output, env=None):
    """Export a bounded capability fixture; it is not a public-case certificate."""
    output.mkdir()
    run.save(output / "request.json", payload)
    with (output / "stdout.jsonl").open("x") as stdout, (output / "stderr.log").open("x") as stderr:
        process = subprocess.run([str(binary.resolve()), str((output / "request.json").resolve()),
                                  str((output / "reader").resolve())], stdout=stdout, stderr=stderr, env=env)
    record_path = output / "reader/record.json"
    record = json.loads(record_path.read_text()) if record_path.exists() else {
        "status": "invalid_input", "failure_reason": (output / "stderr.log").read_text()}
    assert (process.returncode == 0) == (record["status"] == "success"), record
    return record


def exported(record, output, expected):
    assert record["status"] == "success", record
    for query in record["queries"]:
        with pa.ipc.open_stream(output / "reader" / query["result"]) as stream:
            batches = list(stream)  # These fixtures have at most twelve rows.
            assert all(b.num_rows <= 8192 for b in batches)
            actual = pa.Table.from_batches(batches, schema=stream.schema)
        assert [(f.name, oracle.logical_type(f.type)) for f in actual.schema] == [
            (f.name, oracle.logical_type(f.type)) for f in expected.schema]
        assert actual.sort_by("id").to_pylist() == expected.sort_by("id").to_pylist()
        assert query["output_rows"] == expected.num_rows and query["completion_ns"] is None


def export_corpus(record, output, deleted, ids, semantics=None):
    original = CORPUS / "deletion_vectors"
    source = pq.ParquetFile(next((original / "table").glob("*.parquet"))).read()
    with pa.ipc.open_file(original / "expected.arrow") as saved:
        live = saved.read_all()
    assert source.num_rows == 12 and live.num_rows == 9
    assert set(source["id"].to_pylist()) - set(live["id"].to_pylist()) == {2, 5, 12}
    expected = live if deleted else source
    if ids is not None:
        expected = pa.Table.from_pylist([row for row in expected.to_pylist() if row["id"] in ids], schema=expected.schema)
    if semantics == "nulls":
        expected = pa.Table.from_pylist([r for r in source.to_pylist() if r["value"] is None or r["label"] is None],
                                       schema=source.schema).select(["label", "id", "value"])
    elif semantics == "duplicate":
        expected = source.slice(1, 1)
    else:
        assert semantics is None
    exported(record, output, expected)
    return expected.num_rows


def delta_capabilities(binary, fixtures, output):
    import native
    return native.call("delta-capabilities", {"binary": str(binary), "fixtures": str(fixtures),
        "output": str(output), "python": sys.executable, "harness": str(Path(__file__).resolve().parents[1])})
