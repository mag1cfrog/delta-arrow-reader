# Delta Arrow Reader for Python

Read Delta Lake tables as PyArrow record batches with the Rust Delta Arrow Reader.
Use filters directly or query registered tables with SQL.

Install in an activated Python virtual environment:

```console
python -m pip install --only-binary=:all: delta-arrow-reader
```

Wheels require CPython 3.10 or newer and target Linux x86_64 with glibc 2.28 or
newer, Windows x86_64, and macOS arm64 with macOS 12.0 or newer. They include the
Python extension and require PyArrow 18 or newer. See the
[installation guide](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python)
for tested versions and platform limits. Installing a wheel needs no Rust toolchain.

For command-line use, install the separate `delta-arrow-reader-cli` package.
It does not install the Python API or PyArrow:

```console
uvx --no-build --from delta-arrow-reader-cli delta-arrow-reader --help
```

```python
from delta_arrow_reader import DeltaTable

table = DeltaTable("/path/to/delta-table")
with table.to_reader() as reader:
    for batch in reader:
        print(batch)
```

See the [Python quickstart](https://mag1cfrog.github.io/delta-arrow-reader/python/)
for column selection, filters, and snapshot refresh. For SQL, use the included
Rust DataFusion engine:

```python
from delta_arrow_reader import Session

session = Session()
session.register_table("events", table)
with session.sql("SELECT COUNT(*) AS rows FROM events") as reader:
    print(reader.read_all().to_pydict())
```

SQL supports read-only queries over registered snapshots, including joins and
aggregations. It does not require the separate `datafusion` Python package.
See the [SQL guide](https://mag1cfrog.github.io/delta-arrow-reader/python/#query-with-sql)
for execution and memory behavior. The
[contributor guide](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/contributing.md)
covers local builds and tests.
