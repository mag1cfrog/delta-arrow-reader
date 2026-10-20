"""Pinned DuckDB-to-Arrow producer for native production fixture preparation."""

import json
import os
from pathlib import Path
import resource
import sys

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "runners")]


def stream(output):
    import duckdb
    import pyarrow as pa
    from oracle import ORIGINAL, require

    require(duckdb.__version__ == "1.5.5" and pa.__version__ == "25.0.1",
            "use the pinned oracle Python environment")
    request = json.loads((output / "producer-request.json").read_text())
    with duckdb.connect(config={"memory_limit": "2GiB", "threads": "4",
                                "temp_directory": str(output / "spill"),
                                "max_temp_directory_size": "16GiB"}) as connection:
        connection.read_parquet(request["paths"]).create_view("source")
        query = ("SELECT " + ", ".join(ORIGINAL) + " FROM source ORDER BY l_orderkey % "
                 + str(int(request["stripes"])) + ", l_shipdate, l_shipmode, l_linenumber, l_orderkey")
        batches = connection.execute(query).to_arrow_reader(8192)
        with pa.ipc.new_stream(sys.stdout.buffer, batches.schema) as writer:
            for batch in batches:
                writer.write_batch(batch)
    sys.stdout.buffer.flush()
    with (output / "producer-result.json").open("x") as result:
        json.dump({"version": duckdb.__version__, "pyarrow": pa.__version__, "python": sys.version,
                   "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}, result)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--stream"]:
        stream(Path(sys.argv[2]))
    else:
        import native
        binary = str(native.executable())
        os.execv(binary, [binary, "selective-read", "production-fixtures", *sys.argv[1:],
                         "--python", sys.executable, "--harness", str(HERE)])
