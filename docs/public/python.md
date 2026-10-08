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

## Prepare metadata for repeated scans

Use `warmup="query_planning"` to load and retain active-file metadata during table
construction. Later scans reuse it, and refresh updates it for the new snapshot
or reuses it when the version is unchanged. Warmup does not read Parquet data.

```python
prepared = DeltaTable(location, warmup="query_planning")
```

The default, `warmup="none"`, leaves this work to individual scans. With warmup
enabled, unsupported table protocols fail during construction rather than at
scan planning. Other strings raise `ValueError`; non-string values raise
`TypeError`.

## Set the scan partition target

Both `scan()` and `to_reader()` accept `target_partitions` to override automatic
scan partition planning. The default, `None`, keeps automatic planning.

```python
with table.to_reader(columns=["id"], limit=10, target_partitions=2) as reader:
    print(reader.read_all().num_rows)
```

A supplied value must be an integer from `1` through `2 * sys.maxsize + 1`.
Booleans and other types raise `TypeError`, zero and negative values raise
`ValueError`, and values above the maximum raise `OverflowError`.

## Choose a Parquet reader

`ScanExecutionOptions` is an immutable configuration object. Its
`parquet_backend` defaults to `"direct"`, which reads Parquet files through the
asynchronous reader. Use `"delta_kernel"` to delegate reads to Delta Kernel:

```python
from delta_arrow_reader import ScanExecutionOptions

kernel_options = ScanExecutionOptions(parquet_backend="delta_kernel")
configured = DeltaTable(location, execution_options=kernel_options)
with configured.to_reader(columns=["id"], limit=10) as reader:
    print(reader.read_all().num_rows)

with configured.to_reader(execution_options=ScanExecutionOptions(), limit=10) as reader:
    print(reader.read_all().num_rows)
```

The second reader uses the default `"direct"` backend. Both `scan()` and
`to_reader()` accept `execution_options`. An override replaces the complete
configuration for that scan. Omitting it or passing `None` uses the table's
settings. Later scans and refreshed tables keep the table's configuration.

Pass a `ScanExecutionOptions` object, not a dictionary. Other types raise
`TypeError`. For `parquet_backend`, unsupported strings raise `ValueError` and
non-string values raise `TypeError`.

## Limit concurrent file reads per partition

Set `max_concurrent_file_reads_per_partition` to limit how many files each scan
partition can read at once. It applies to both Parquet backends and defaults to
the Rust reader's value of `3`. This is an upper bound; actual concurrency may be
lower.

```python
serial_reads = ScanExecutionOptions(max_concurrent_file_reads_per_partition=1)
with table.to_reader(target_partitions=1, execution_options=serial_reads) as reader:
    print(reader.read_all().num_rows)
```

This example uses one partition and reads one file at a time. The setting is a
positive integer no greater than `sys.maxsize >> 2`, the core's concurrency
capacity. `None`, booleans, and other non-integer values raise `TypeError`.
Zero, negative values, and values above the core limit that still fit `usize`
raise `ValueError`. Positive integers larger than `2 * sys.maxsize + 1` raise
`OverflowError`.

## Limit concurrent file reads across a scan

Set `max_concurrent_file_reads_per_scan` to cap concurrent file reads across all
partitions in one scan. Both the scan and per-partition limits apply:

```python
scan_limit = ScanExecutionOptions(max_concurrent_file_reads_per_scan=2)
with table.to_reader(target_partitions=4, execution_options=scan_limit) as reader:
    print(reader.read_all().num_rows)
```

At most two files are read concurrently across these partitions. Omit the option
or pass `None` to derive the limit from the partition target multiplied by the
per-partition limit, capped at the core's concurrency capacity. `None` is the
Rust default. A supplied integer has the same bounds and error behavior as
`max_concurrent_file_reads_per_partition`.

## Set the output batch buffer

`output_buffer_batches_per_partition` sets how many batches each partition can
queue for the consumer. The Rust default is `1`. A larger buffer lets the
producer read further ahead while the consumer processes earlier batches:

```python
buffered = ScanExecutionOptions(output_buffer_batches_per_partition=2)
with table.to_reader(execution_options=buffered) as reader:
    print(reader.read_all().num_rows)
```

The value counts queued batches, not rows or bytes. Batches being prepared,
backend buffers, and batches you retain also use memory. The option accepts the
same positive integer range and raises the same errors as
`max_concurrent_file_reads_per_partition`; `None` is not accepted.

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
