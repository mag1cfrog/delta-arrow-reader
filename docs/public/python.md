# Read a Delta table with Python

Read a local Delta table as PyArrow record batches, select columns and rows,
and close the reader when you stop early.

## Before you start

[Install the Python package](installation.md#python), then start a Python session
from the repository root. Run the local examples below in order. They use the
sample table checked into the repository, which has 360 rows. To use your own
table, replace `location` with its path and select columns from its schema.

## Stream batches

```python
from pathlib import Path

from delta_arrow_reader import DeltaTable

location = Path("tests/reader/fixtures/external_writer/corpus/partitioned/table")
table = DeltaTable(location)
print(f"Snapshot: {table.version}")
print(table.schema)

rows = 0
with table.to_reader(columns=["id", "region"], limit=200) as reader:
    for batch in reader:
        print(f"Batch rows: {batch.num_rows}")
        rows += batch.num_rows
print(f"Read {rows} rows")
```

The sample prints snapshot version `0` and finishes with `Read 200 rows`.
Each iteration yields a `pyarrow.RecordBatch` containing the selected columns
in the requested order. Omit `columns` to read every column, and omit `limit`
to read every row. Planning reads Delta metadata; Parquet reads begin when you
request the first batch.

The table stays on the snapshot it loaded, even if another process writes new
commits. Each reader created from it reads that same snapshot. To load the latest
snapshot, call `refresh()`:

```python
latest = table.refresh()
print(f"Original: {table.version}, refreshed: {latest.version}")
```

Refresh returns a new table. The original table and its readers keep their
snapshot, even if refresh fails. If no new commits exist, the returned table has
the same version. To select a specific version, use
`DeltaTable(location, version=0)`.

## Use the Arrow stream interface and stop early

`to_reader()` returns a `pyarrow.RecordBatchReader`. You can also construct one
through PyArrow's public stream interface:

```python
import pyarrow as pa

with pa.RecordBatchReader.from_stream(table.scan(columns=["id"])) as reader:
    for batch in reader:
        print(batch.num_rows)
        break
```

The `with` block closes the reader even when the loop ends early or raises an
exception. Closing stops new scan work; synchronous Kernel work already running
may still finish. A batch you keep remains valid after the reader closes.

Each `scan()` result can be exported once. After export, the consumer owns its
cleanup. Closing the original stream object does not close that consumer.

## Collect a result in memory

Call `read_all()` when you want a `pyarrow.Table` containing all remaining rows:

```python
with table.to_reader(columns=["id"], limit=10) as reader:
    result = reader.read_all()
print(result.to_pydict())
```

This result contains ten rows. Without a limit, `read_all()` holds the full
remaining result in memory, so use it when that result fits.

## Read from S3

Replace `<bucket>`, `<table-prefix>`, and `<region>` below. Set
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in your environment, and set
`AWS_SESSION_TOKEN` when using temporary credentials.

```python
import os

from delta_arrow_reader import DeltaTable

options = {
    "aws_region": "<region>",
    "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
    "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
}
if token := os.environ.get("AWS_SESSION_TOKEN"):
    options["aws_session_token"] = token

remote = DeltaTable("s3://<bucket>/<table-prefix>", storage_options=options)
with remote.to_reader(limit=10) as reader:
    for batch in reader:
        print(batch)
```

`storage_options` accepts string keys and values. The
[Python API details](installation.md#python) cover argument validation, stream
ownership, errors, and Ctrl+C behavior.
