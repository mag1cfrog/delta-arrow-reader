# Read-only metadata references

`requests.json` and `spark-reference.jsonl` contain 64 Spark 4.2.0 observations
in both ANSI modes. The source manifest supplies an empty table with explicit
column metadata and separate catalog setup. DESCRIBE and SHOW COLUMNS compare
field order, column names, complete values and executed types. The schema covers
nested arrays, structs and maps, decimals, both timestamp kinds, quoted field
names and comments.

SHOW controls distinguish permanent tables, ordinary views and temporary
providers, selected and qualified schemas, and the default catalog. Patterns
cover case, surrounding whitespace, complete-name alternatives, `*`, ordinary
regex syntax, escaped dots, missing matches and invalid alternatives. Empty
catalogs, schemas and zero-column providers execute separate typed-result
controls in Rust. Native Unicode case folding differs from Spark's default
ASCII folding for some non-ASCII patterns. Java-only regex constructs such as
lookbehind are not supported by the native matcher. These separate limits are
recorded in [known differences](https://github.com/mag1cfrog/delta-arrow-reader/issues/1248).

EXPLAIN observations retain Spark's raw plan text and executed STRING schema.
Rust checks the one-row result and type, then requires its own DataFusion logical
and physical plan text. It does not compare the two engines' plan strings.

The admission tests use a recording real Delta provider and object store.
DESCRIBE and SHOW produce no source scan or storage requests. EXPLAIN may create
a source scan plan, but reads no data objects and never collects the explained
query. Catalog, function, session and fixture snapshots remain unchanged.
Host scalar and table-function canaries remain uncalled in metadata query
bodies; ordinary queries keep their existing host-function behavior.

DESCRIBE reports provider columns once and omits Spark's extra partition
information rows. Metadata operates within the registered providers' Rust
contracts; it is not a sandbox for arbitrary provider implementations.

Capture with Python 3.12, Java 21, UTC and PySpark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, using a fresh output path:

```sh
TZ=UTC python3.12 capture_spark.py /path/to/new/spark-reference.jsonl
```

The shared helper verifies the runtime and records an owned JVM's cleanup. The
capture driver creates a fresh warehouse beside the output, outside the repo.
Only Spark reference setup creates catalog objects; candidate tests register
providers through the host API.
