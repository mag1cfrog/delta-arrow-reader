# Read a table with Python

Open a Delta table, read a few rows, then choose columns or query it with SQL.
Results use PyArrow, which is installed with the reader.

## Before you start

[Install the Python package](installation.md#python) and have an existing Delta
table available. Use the table's directory, which contains `_delta_log`, rather
than a single Parquet file.

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
For OR conditions, null checks, and other types, see
[the filtering rules](python-api/table.md#delta_arrow_reader.DeltaTable.to_reader).

## Query with SQL

If you prefer SQL, register the same table with a `Session`. The name `events`
is how your query refers to it:

```python
from delta_arrow_reader import Session

session = Session()
session.register_table("events", table)

with session.sql("SELECT * FROM events LIMIT 10") as reader:
    print(reader.read_all().to_pydict())
```

SQL returns a PyArrow reader too. The package includes DataFusion to run the
queries; you do not need to install the separate Python `datafusion` package.

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

## Where to go next

- [Configuration](configuration.md): read from S3, choose preparation before queries, or limit resource use.
- [Python API reference](python-api/index.md): look up methods, parameters, defaults, and errors.
- [Arrow streams](python-api/streams-and-errors.md): pass a scan to another Arrow consumer.
