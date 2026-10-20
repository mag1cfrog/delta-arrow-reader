"""Plan Q2/Q4-derived queries and file membership from the real SF10 source.

This is untimed input preparation. It writes no Delta fixture and cannot produce
a campaign manifest or certify Parquet page geometry or reader performance.
"""

from datetime import date
import json
import os
import sys
from pathlib import Path
import tempfile

import duckdb
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "runners")]
from oracle import WIDE69, require
import native

CONTRACT = Path(__file__).resolve().parents[2] / "docs/content/benchmarks/selective-read-production-shapes.md"
PREDICATE = "l_shipdate = DATE '1995-03-15' AND l_shipmode = 'AIR' AND l_linenumber IN (1)"
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


def file_geometry(connection, shape):
    """Predict file membership only; actual writer metadata must check it again."""
    files, stripes = shape["files"], shape["stripes"]
    require(type(files) is int and type(stripes) is int and files >= stripes > 0, "invalid file/stripe counts")
    sql = f"""
        WITH striped AS (
            SELECT *, l_orderkey % {stripes} AS stripe FROM source
        ), ordered AS (
            SELECT *, row_number() OVER (
                PARTITION BY stripe ORDER BY l_shipdate, l_shipmode, l_linenumber, l_orderkey
            ) - 1 AS ordinal, count(*) OVER (PARTITION BY stripe) AS stripe_rows
            FROM striped
        ), assigned AS (
            SELECT *, (ordinal * ({files // stripes} + CAST(stripe < {files % stripes} AS BIGINT)))
                // stripe_rows AS file_index FROM ordered
        )
        SELECT stripe, file_index, count(*) AS rows,
            min(l_shipdate) AS date_min, max(l_shipdate) AS date_max,
            min(l_shipmode) AS mode_min, max(l_shipmode) AS mode_max,
            min(l_linenumber) AS line_min, max(l_linenumber) AS line_max,
            count(*) FILTER (WHERE {PREDICATE}) AS matching_rows
        FROM assigned GROUP BY stripe, file_index ORDER BY stripe, file_index
    """
    rows = connection.execute(sql).fetchall()
    require(len(rows) == files, "source cannot populate the declared file count")
    result = []
    for stripe, index, count, dmin, dmax, mmin, mmax, lmin, lmax, matched in rows:
        candidate = dmin <= date(1995, 3, 15) <= dmax and mmin <= "AIR" <= mmax and lmin <= 1 <= lmax
        require(not matched or candidate, "file bounds exclude qualifying rows")
        result.append({"stripe": stripe, "file_index": index, "rows": count,
                       "candidate": candidate, "matching_rows": matched})
    return result


def analyze(output):
    request = json.load(sys.stdin)
    counts = source_counts(request["paths"])
    with tempfile.TemporaryDirectory(prefix="shape-sort-", dir=output) as temporary:
        with duckdb.connect(config={"memory_limit": "2GiB", "threads": "4",
                                    "temp_directory": temporary, "max_temp_directory_size": "16GiB"}) as connection:
            connection.read_parquet(request["paths"]).project(", ".join(INPUT_COLUMNS)).create_view("source")
            geometry = {name: file_geometry(connection, shape) for name, shape in request["shapes"].items()}
    print(json.dumps({"counts": counts, "geometry": geometry, "duckdb": duckdb.__version__}))


if __name__ == "__main__":
    if sys.argv[1:2] == ["--analyze"]:
        analyze(Path(sys.argv[2]))
    else:
        binary = str(native.executable())
        os.execv(binary, [binary, "selective-read", "plan-production", *sys.argv[1:],
                         "--python", sys.executable, "--harness", str(HERE)])
