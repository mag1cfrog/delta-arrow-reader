# Read a table with Python

Open a Delta table, read a few rows, then choose columns or query it with SQL.
Results use PyArrow, which is installed with the reader.

## Before you start

[Install the Python package](installation.md#python) and have an existing Delta
table available. Use the table's directory, which contains `_delta_log`, rather
than a single Parquet file.

For a table in cloud storage, follow the [S3](configuration.md#read-from-s3) or
[Azure](configuration.md#read-from-azure) connection example, then use the same
reading methods below.

<details markdown="1">
<summary>Use the repository's sample table</summary>

If you have a [repository checkout](https://github.com/mag1cfrog/delta-arrow-reader),
start Python from its root directory and use this path in the next example:

```text
crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table
```

The sample has 360 rows and the columns `id`, `value`, `label`, and `region`.
The column-selection, filter, and SQL examples below work with it as written.

</details>

## Read your first rows

Replace `/path/to/table` with your table's path:

```python
from delta_arrow_reader import DeltaTable

table = DeltaTable("/path/to/table")
with table.to_reader(limit=10) as reader:
    result = reader.read_all()

print(result.to_pydict())
```

This prints up to ten rows as a dictionary of columns. `result` is a
`pyarrow.Table` containing all columns. The `with` block closes the reader when
you finish or if an error occurs; the result remains usable afterward.

`read_all()` collects the result in memory. The limit keeps this first result
small. Rows have no guaranteed order, so a later read may return different rows.

## Choose columns

See which columns your table has:

```python
print(table.schema)
```

Then request just the columns you need. Replace `id` and `region` with names
from your table:

```python
with table.to_reader(columns=["id", "region"], limit=10) as reader:
    print(reader.read_all().to_pydict())
```

The result contains those columns in the order you requested.

## Filter rows

For a table with an integer `id` column and a text `region` column:

```python
with table.to_reader(
    columns=["id", "region"],
    filters=[("id", ">=", 100), ("region", "==", "east")],
    limit=10,
) as reader:
    print(reader.read_all().to_pydict())
```

Both conditions must match: `id` is at least `100` and `region` is `"east"`.
The limit applies after filtering. Filter values must match their column types.
<details markdown="1">
<summary>Combine conditions with OR or check for nulls</summary>

A list of lists combines groups with OR. This selects rows where `region` is
`"east"` or `id` is less than `10`:

```python
with table.to_reader(
    filters=[[("region", "==", "east")], [("id", "<", 10)]],
    limit=10,
) as reader:
    print(reader.read_all().to_pydict())
```

Within each inner list, all conditions must match. To check for nulls, use
`("region", "is", None)` or `("region", "is not", None)`. Comparisons such as
`("region", "==", None)` are invalid.

The [to_reader() reference](python-api/table.md#delta_arrow_reader.DeltaTable.to_reader)
lists all supported operators and value types.

</details>

## Use Pandas or Polars

Install the library you use with `python -m pip install pandas` or
`python -m pip install polars`. Neither is required for PyArrow reads.
Read a small result with the [table you opened earlier](#read-your-first-rows):

```python
with table.to_reader(limit=10) as reader:
    result = reader.read_all()
```

For a Pandas DataFrame:

```python
df = result.to_pandas()
print(df.head())
```

For a Polars DataFrame:

```python
import polars as pl

df = pl.from_arrow(result)
print(df.head())
```

These conversions keep the result in memory and may allocate additional memory.
Choose columns and filters in `to_reader()` to reduce the data read from storage;
filtering the DataFrame afterward does not reduce that read.
For larger results, [process one batch at a time](#process-a-larger-result-in-batches).
See the [PyArrow](https://arrow.apache.org/docs/python/pandas.html) and
[Polars](https://docs.pola.rs/api/python/stable/reference/api/polars.from_arrow.html)
documentation for conversion options and data-type behavior.

## Query with SQL

If you prefer SQL, register the [table you opened earlier](#read-your-first-rows)
with a `Session`. The name `events` is how your query refers to it:

```python
from delta_arrow_reader import Session

session = Session()
session.register_table("events", table)

with session.sql("SELECT * FROM events LIMIT 10") as reader:
    print(reader.read_all().to_pydict())
```

SQL returns a PyArrow reader too. The package includes DataFusion to run the
queries locally; you do not need Spark, a Databricks cluster, or the separate
Python `datafusion` package. `events` is a name inside this session, not a
catalog lookup.

Once that works, change the query to filter or summarize your data. For the
`id` and `region` columns above:

```python
with session.sql("""
    SELECT region, COUNT(*) AS events
    FROM events
    WHERE id >= 100
    GROUP BY region
    ORDER BY region
""") as reader:
    print(reader.read_all().to_pydict())
```

Each call accepts one read-only query. Register more tables to join them.
Sorting, grouping, and joining can use substantial memory even when you read
the output in batches. The [Session reference](python-api/session.md) covers
registration rules and errors.

## Process a larger result in batches

When the result may be too large to keep in memory, iterate over the reader
instead of calling `read_all()`:

```python
with table.to_reader() as reader:
    for batch in reader:
        print(f"Read {batch.num_rows} rows")
        # Process this batch before reading the next one.
```

Each batch is a `pyarrow.RecordBatch`, a group of rows. This example has no limit,
so it reads the whole table. You can use the same loop with columns, filters, or
a SQL reader. The `with` block closes the reader even if you `break` early.

## Read again or use newer data

Reuse `table` for later queries. It stays on the version it opened, even when
another process adds data. To load the latest version:

```python
latest = table.refresh()
print(f"Original version: {table.version}, latest: {latest.version}")
```

The original table and existing readers keep their version. For SQL, register
`latest` under a new name or in a new session. Refreshing a table does not update
an existing registration.

<details markdown="1">
<summary>Read an older version (time travel)</summary>

Pass a version number when opening a table:

```python
older = DeltaTable("/path/to/table", version=0)
print(older.version)
```

Replace `0` with the version you need and include the same `storage_options`
as for other reads of that location. The version's log and data files must
still be available. This selects a version by number; the Python constructor
does not accept a timestamp.

</details>

## Where to go next

- [Handle errors](python-api/errors.md): catch failures when opening a table or reading batches.
- [Enable logging](python-api/logging.md): show reader progress or get more detail when investigating a problem.
- [Configuration](configuration.md): connect to cloud storage, reduce opening work, or limit parallel reads.
- [Python API reference](python-api/index.md#api-reference): look up methods, parameters, defaults, and errors.
