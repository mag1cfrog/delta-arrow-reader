"""Pinned DuckDB-to-Arrow producer for native production fixture preparation."""

import json
from pathlib import Path
import resource
import sys


def stream(output):
    import duckdb
    import pyarrow as pa

    if duckdb.__version__ != "1.5.5" or pa.__version__ != "25.0.1":
        raise ValueError("use the pinned oracle Python environment")
    request = json.loads((output / "producer-request.json").read_text())
    with duckdb.connect(config={"memory_limit": "2GiB", "threads": "4",
                                "temp_directory": str(output / "spill"),
                                "max_temp_directory_size": "16GiB"}) as connection:
        connection.read_parquet(request["paths"]).create_view("source")
        batches = connection.execute(request["query"]).to_arrow_reader(8192)
        with pa.ipc.new_stream(sys.stdout.buffer, batches.schema) as writer:
            for batch in batches:
                writer.write_batch(batch)
    sys.stdout.buffer.flush()
    with (output / "producer-result.json").open("x") as result:
        json.dump({"version": duckdb.__version__, "pyarrow": pa.__version__, "python": sys.version,
                   "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}, result)


if __name__ == "__main__":
    if sys.argv[1:2] != ["--stream"]:
        raise SystemExit("use xtask selective-read production-fixtures")
    stream(Path(sys.argv[2]))
