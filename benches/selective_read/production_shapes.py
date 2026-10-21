"""Plan Q2/Q4-derived queries and file membership from the real SF10 source.

This is untimed input preparation. It writes no Delta fixture and cannot produce
a campaign manifest or certify Parquet page geometry or reader performance.
"""

import json
import sys
from pathlib import Path
import tempfile

import duckdb

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "runners")]
import native

INPUT_COLUMNS = ["l_orderkey", "l_linenumber", "l_shipdate", "l_shipmode"]


def source_counts(paths):
    return native.call("oracle", {"action": "source-counts", "paths": [str(p) for p in paths],
                                  "python": sys.executable, "harness": str(HERE)})


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
