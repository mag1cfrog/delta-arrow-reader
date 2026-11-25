"""Check a full stock CLI export against all decoded SF1 Parquet rows."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
from build import REPO, digest


def parquet_batches(snapshot, files, arrow, parquet):
    for file in files:
        source = parquet.ParquetFile(snapshot / file["path"])
        assert source.metadata.num_rows == file["rows"]
        seen = 0
        for batch in source.iter_batches(batch_size=8192):
            buckets = arrow.repeat(arrow.scalar(file["file_bucket"], type=arrow.int32()), batch.num_rows)
            batch = batch.append_column(arrow.field("file_bucket", arrow.int32(), nullable=False), buckets)
            seen += batch.num_rows
            yield batch
        assert seen == file["rows"]


def compare(stream, expected, arrow):
    def nonempty(batches):
        return (batch for batch in batches if batch.num_rows)

    left, right = nonempty(expected), nonempty(stream)
    a, b = next(left, None), next(right, None)
    exact_types, rows = None, 0
    while a is not None and b is not None:
        if exact_types is None:
            assert a.schema.names == b.schema.names
            exact_types = {}
            for stored, executed in zip(a.schema, b.schema, strict=True):
                view = stored.type == arrow.string() and executed.type == arrow.string_view()
                assert stored.type == executed.type or view, f"unexpected executed type for {stored.name}"
                exact_types[stored.name] = {"parquet": str(stored.type), "stock_cli": str(executed.type)}
        count = min(a.num_rows, b.num_rows)
        for i, (x, y) in enumerate(zip(a.columns, b.columns, strict=True)):
            x, y = x.slice(0, count), y.slice(0, count)
            if x.type != y.type:
                y = y.cast(x.type)
            assert x.equals(y), f"row values differ from stock export at row {rows}, column {i}"
        rows += count
        a = next(left, None) if a.num_rows == count else a.slice(count)
        b = next(right, None) if b.num_rows == count else b.slice(count)
    assert a is None and b is None and exact_types
    return {"rows": rows, "decoded_column_types": exact_types}


def main():
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    cli = argparse.ArgumentParser(description=__doc__)
    for name in ("fixture", "stock_cli", "output"):
        cli.add_argument(name, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    assert all(not path.is_relative_to(REPO) for path in (args.fixture, args.output))
    args.output.mkdir()
    path = args.fixture / "fixture-proof.json"
    proof = json.loads(path.read_text())
    snapshot = args.fixture / "delta-snapshot"
    dar = args.stock_cli / "dar"
    bound = json.loads((args.stock_cli / "receipt.json").read_text())
    assert digest(dar) == bound["binary_sha256"]
    files = proof["checks"]["files"]
    assert all(digest(snapshot / f["path"]) == f["macos"] for f in files)
    table = args.output / "tables.json"
    table.write_text(json.dumps({"format_version": 1, "tables": [{"alias": "sf1", "location": str(snapshot),
                                                                  "table_version": "0"}]}) + "\n")
    sql = args.output / "export.sql"
    sql.write_text("SELECT * FROM sf1 ORDER BY file_bucket, l_shipdate, l_shipmode, l_partkey, l_orderkey, l_linenumber\n")
    output = args.output / "sf1.arrow"
    cmd = [dar, "--diagnostics=json", "query", "--tables-file", table, "--sql-file", sql, "--format", "arrow",
           "--output", output, "--memory-limit-bytes", str(4 * 1024**3),
           "--max-output-bytes", str(32 * 1024**3), "--max-rows", str(proof["rows"]), "--deadline-ms", "1800000"]
    clean = {"PATH": os.defpath, "TZ": "UTC"}
    result = subprocess.run(cmd, env=clean, capture_output=True, timeout=1860)
    assert result.returncode == 0 and output.is_file(), "stock CLI failed to export the complete fixture"
    with arrow.memory_map(str(output), "r") as source:
        reader = arrow.ipc.open_stream(source)
        export = compare(reader, parquet_batches(snapshot, files, arrow, parquet), arrow)
    assert export["rows"] == proof["rows"] and len(export["decoded_column_types"]) == proof["snapshot_columns"]
    proof["stock_cli_export"] = {**export, "export_sha256": digest(output), "stock_cli": bound["binary_sha256"],
                                  "stock_cli_source": bound["source"], "sql": sql.read_text().strip(),
                                  "arrow_version": arrow.__version__}
    # The CLI comparison independently confirms local decoding. The Linux file hashes
    # are still needed to establish that the same Parquet values generated on that host.
    proof["content_matches"] = all(f["match"] for f in files)
    path.write_text(json.dumps(proof, indent=2) + "\n")
    print(f"Complete stock CLI export matched {export['rows']} rows and {len(export['decoded_column_types'])} types")


if __name__ == "__main__":
    main()
