"""Unfiltered C++ Parquet reads for the independent Rust benchmark oracle."""

import sys

import pyarrow as pa
import pyarrow.parquet as pq


if __name__ == "__main__":
    if pa.__version__ != "25.0.1":
        raise ValueError("unsupported oracle PyArrow runtime")
    with pq.ParquetFile(sys.argv[1]) as source:
        columns = sys.argv[2:] or None
        schema = pa.schema([source.schema_arrow.field(name) for name in columns]) if columns else source.schema_arrow
        with pa.ipc.new_stream(sys.stdout.buffer, schema) as writer:
            for batch in source.iter_batches(batch_size=8192, columns=columns):
                writer.write_batch(batch)
