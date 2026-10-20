"""Plan Q2/Q4-derived queries and file membership from the real SF10 source.

This is untimed input preparation. It writes no Delta fixture and cannot produce
a campaign manifest or certify Parquet page geometry or reader performance.
"""

from datetime import date
import json
import sys
from pathlib import Path
import tempfile

import duckdb
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "runners")]
from oracle import WIDE69
import native

CONTRACT = Path(__file__).resolve().parents[2] / "docs/content/benchmarks/selective-read-production-shapes.md"
SHAPES = {
    "q2": {"files": 130, "stripes": 5, "extra_numeric_columns": 336, "projection": WIDE69},
    "q4": {"files": 60, "stripes": 6, "extra_numeric_columns": 10,
           "projection": WIDE69 + ("l_suppkey", "l_quantity")},
}
INPUT_COLUMNS = ["l_orderkey", "l_linenumber", "l_shipdate", "l_shipmode"]


def definitions(file_target_mib=512, page_rows=20000, page_bytes=1048576, write_batch_rows=1024):
    return native.call("workload", {"action": "shapes", "file_target_mib": file_target_mib,
        "page_rows": page_rows, "page_bytes": page_bytes, "write_batch_rows": write_batch_rows})


def cases():
    return native.call("workload", {"action": "cases"})


def source_counts(paths):
    """Count each predicate stage with Arrow, independently of the layout SQL."""
    counts = [0, 0, 0, 0]
    for path in paths:
        for batch in pq.ParquetFile(path).iter_batches(columns=INPUT_COLUMNS):
            masks = [pc.equal(batch.column("l_shipdate"), date(1995, 3, 15))]
            masks.append(pc.and_(masks[-1], pc.equal(batch.column("l_shipmode"), "AIR")))
            masks.append(pc.and_(masks[-1], pc.equal(batch.column("l_linenumber"), 1)))
            counts[0] += batch.num_rows
            for index, mask in enumerate(masks, 1):
                counts[index] += pc.sum(pc.cast(mask, "int64")).as_py()
    return counts


def analyze(output):
    request = json.load(sys.stdin)
    counts = source_counts(request["paths"])
    with tempfile.TemporaryDirectory(prefix="shape-sort-", dir=output) as temporary:
        with duckdb.connect(config={"memory_limit": "2GiB", "threads": "4",
                                    "temp_directory": temporary, "max_temp_directory_size": "16GiB"}) as connection:
            connection.read_parquet(request["paths"]).project(", ".join(INPUT_COLUMNS)).create_view("source")
            statistics = {name: connection.execute(sql).to_arrow_table().to_pylist()
                          for name, sql in request["queries"].items()}
    print(json.dumps({"counts": counts, "statistics": statistics, "duckdb": duckdb.__version__}, default=str))


if __name__ == "__main__":
    if sys.argv[1:2] != ["--analyze"]:
        raise SystemExit("use xtask selective-read plan-production")
    analyze(Path(sys.argv[2]))
