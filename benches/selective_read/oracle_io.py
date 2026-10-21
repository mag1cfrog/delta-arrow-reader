"""Unfiltered C++ Parquet reads for the independent Rust benchmark oracle."""

import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq


if __name__ == "__main__":
    if pa.__version__ != "25.0.1":
        raise ValueError("unsupported oracle PyArrow runtime")
    for line in sys.stdin:
        request = json.loads(line)
        columns = request["columns"] or None
        writer = None
        try:
            for path in request["paths"]:
                with pq.ParquetFile(path) as source:
                    schema = pa.schema([source.schema_arrow.field(name) for name in columns]) if columns else source.schema_arrow
                    if writer is None:
                        writer = pa.ipc.new_stream(sys.stdout.buffer, schema)
                    for batch in source.iter_batches(batch_size=8192, columns=columns):
                        writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()
        sys.stdout.buffer.flush()
