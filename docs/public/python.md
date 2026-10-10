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

## Filter rows

Both `scan()` and `to_reader()` accept `filters`. Use `==`, `!=`, `<`, `<=`, `>`,
or `>=` to compare Boolean, signed integer, floating-point, string, binary,
decimal, date, and timestamp columns. Use `"is"` with `None` to select null
values, or `"is not"` with `None` to select non-null values:

```python
with table.to_reader(
    columns=["id"], filters=[("id", ">=", 100), ("region", "==", "east")], limit=10,
) as reader:
    print(reader.read_all().to_pydict())
```

A list of `(column, operator, value)` tuples combines conditions with AND.
A list of lists combines AND groups with OR. For example,
`[[("label", "is", None)], [("region", "is", None)]]` selects rows where either
column is null. Omitting `filters`, passing `None`, or passing `[]` disables
filtering. An empty inner AND group is true, so `[[]]` selects every row.

Filters use top-level logical column names, including columns omitted from the
output. They apply before `limit`, and deleted rows remain excluded. Malformed
groups and non-string column names or operators raise `TypeError`. Unknown
operators, comparisons with `None`, and null tests with a value other than
`None` raise `ValueError`. Invalid column references raise a redacted
`DeltaReaderError`.

Comparison values must match the column type: `bool` for Boolean columns,
and `int` for signed 8-, 16-, 32-, or 64-bit integer columns. Booleans are not
accepted as integers. Wrong or unsupported value types raise `TypeError`;
integers outside the column's range raise `OverflowError`.

Float32 and Float64 columns require Python `float` values. Integers, booleans,
and other types raise `TypeError`. NaN, infinity, and values that become infinite
when converted to Float32 raise `ValueError`. Float32 rounds to 32-bit precision,
including underflow to signed zero. Comparisons use native Arrow ordering, which
distinguishes `-0.0` from `0.0` and orders `-0.0` first.

Utf8 and LargeUtf8 columns require `str`; Binary, LargeBinary, and FixedSizeBinary
columns require `bytes`. Empty values and embedded NULs are supported. Strings
that cannot be encoded as UTF-8 and fixed-size binary values with the wrong
length raise `ValueError`. Other types, including `bytearray` and `memoryview`,
raise `TypeError`. Values are copied before scan planning.

Decimal128 columns require finite `decimal.Decimal` values. Conversion is exact
and independent of the decimal context; it does not round. For a `decimal(5,2)`
column, `Decimal("1.2300")` is accepted as `1.23`, while `Decimal("1.234")` and
`Decimal("1000")` raise `ValueError` for scale and precision violations. NaN and
infinity also raise `ValueError`. An unscaled integer outside the signed 128-bit
range raises `OverflowError`. Integers, floats, strings, and other value types
raise `TypeError`.

Date32 columns require `datetime.date` values. Dates are converted to signed
days since `1970-01-01`, including dates before that day. `datetime.datetime`
values, with or without a timezone, and other types raise `TypeError`. For
example, use `date(1969, 12, 31)` after `from datetime import date`.

Timestamp columns without a timezone (`timestamp_ntz`) require naive
`datetime.datetime` values, for which `utcoffset()` is `None`. Values are
converted to exact signed microseconds since `1970-01-01 00:00:00`, without
using the process timezone or floating-point arithmetic. Aware values and
invalid offsets raise `ValueError`; other value types raise `TypeError`.

Timestamp columns with a timezone (`timestamp`) require aware
`datetime.datetime` values. Conversion uses the value's UTC offset, including
`fold` for repeated local times, and preserves exact microseconds. Different
offsets can represent the same instant. The filter keeps the column's timezone
metadata. Naive values and invalid offsets raise `ValueError`; other value
types raise `TypeError`. For example, use
`datetime(2024, 1, 1, tzinfo=timezone.utc)` after
`from datetime import datetime, timezone`.

Timestamp values must be instances of the standard library's `datetime.datetime`
class itself. Subclass instances, including `pandas.Timestamp` values and
`pandas.NaT`, raise `TypeError` because they can carry extra precision or special
values. Custom timezones are supported: `utcoffset()` is called once and must
return a standard `datetime.timedelta` instance or `None`. Offset subclasses
raise `ValueError`.

## Prepare a table for repeated scans

The default, `warmup="automatic"`, prepares supported S3 tables and profiles
their network before querying. It uses a five-second sampling limit after
metadata loading and schedules at most 24 MiB across 3,084 requests, excluding
store retries. Failed or incomplete network samples are discarded. Other stores
remain lazy. See [network warmup](scan-planning.md#choose-a-warmup-mode).

Use `warmup="query_planning"` to load and retain active-file metadata during table
construction. Later scans reuse it, and refresh updates it for the new snapshot
or reuses it when the version is unchanged. This mode does not read Parquet data.

```python
prepared = DeltaTable(location, warmup="query_planning")
```

Use `warmup="none"` to skip preparation and leave metadata work to individual
scans. It also keeps partial-page reads inactive because no small-request cost
is measured. With metadata warmup enabled, unsupported table protocols fail during
construction rather than at scan planning. Other strings raise `ValueError`;
non-string values raise `TypeError`.

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

## Set file prefetch depth

`prefetch_files_per_partition` controls how many future file streams each
partition prepares when using the `"direct"` backend. The Rust default is `2`.
Set it to `0` to disable prefetch of future files:

```python
no_prefetch = ScanExecutionOptions(prefetch_files_per_partition=0)
with table.to_reader(execution_options=no_prefetch) as reader:
    print(reader.read_all().num_rows)
```

The scan and per-partition file-read limits still apply. The `"delta_kernel"`
backend ignores this setting. A supplied value must be an integer from `0`
through `2 * sys.maxsize + 1`. `None`, booleans, and other types raise `TypeError`,
negative values raise `ValueError`, and values above the maximum raise
`OverflowError`.

## Set the Parquet metadata size hint

`parquet_metadata_size_hint_bytes` controls how many bytes the `"direct"` backend
initially requests from the end of each Parquet file to load its metadata.
Omitting it keeps the Rust default of `65536` bytes. Pass `None` to disable the
hint:

```python
no_metadata_hint = ScanExecutionOptions(parquet_metadata_size_hint_bytes=None)
with table.to_reader(execution_options=no_metadata_hint) as reader:
    print(reader.read_all().num_rows)
```

The reader fetches more bytes if the initial request does not contain all the
metadata. It always requests at least the 8-byte Parquet footer and never requests
beyond the file. The `"delta_kernel"` backend ignores this hint.

A supplied integer must be from `1` through `2 * sys.maxsize + 1`. Booleans and
other types raise `TypeError`, zero and negative values raise `ValueError`, and
values above the maximum raise `OverflowError`.

## Buffer small Parquet files

`parquet_full_file_read_threshold_bytes` lets the `"direct"` backend fetch files
at or below the threshold once and serve subsequent Parquet range reads from
memory. The Rust default, `None`, disables full-file buffering:

```python
small_files = ScanExecutionOptions(parquet_full_file_read_threshold_bytes=1024 * 1024)
with table.to_reader(execution_options=small_files) as reader:
    print(reader.read_all().num_rows)
```

This example buffers files up to and including 1 MiB. Each qualifying file is
held in memory during reading. Larger files use ordinary range reads. The
`"delta_kernel"` backend ignores this setting.

Pass `None` to disable buffering. A supplied integer has the same positive
range and error behavior as `parquet_metadata_size_hint_bytes`.

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
