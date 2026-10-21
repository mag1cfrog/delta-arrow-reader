"""Independent Arrow values and fixtures for native reader capability checks."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


CORPUS = Path(__file__).resolve().parents[3] / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus"


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


def logical_type(kind):
    if pa.types.is_dictionary(kind):
        kind = kind.value_type
    return pa.string() if (pa.types.is_string(kind) or pa.types.is_large_string(kind)
                           or pa.types.is_string_view(kind)) else kind


def exported(record, output, expected):
    assert record["status"] == "success", record
    for query in record["queries"]:
        with pa.ipc.open_stream(output / "reader" / query["result"]) as stream:
            batches = list(stream)  # These fixtures have at most twelve rows.
            assert all(b.num_rows <= 8192 for b in batches)
            actual = pa.Table.from_batches(batches, schema=stream.schema)
        assert [(f.name, logical_type(f.type)) for f in actual.schema] == [
            (f.name, logical_type(f.type)) for f in expected.schema]
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
