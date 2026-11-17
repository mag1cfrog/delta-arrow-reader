# Read-only admission references

`requests.json` supplies the same three source rows to Spark 4.2.0 and to a real
Delta provider in `read_only_admission.rs`. `spark-reference.jsonl` records values
and executed result types in both ANSI modes for SELECT, WITH, VALUES, TABLE,
qualified references, parenthesized queries, higher-order functions and text
containing command words. Nullability and metadata are outside these comparisons.

The qualified-name reference reads a Spark global temporary view backed by those
same inputs. The candidate uses registered Delta providers, including ordinary
qualified names, quoted names with file extensions, literal and computed
`IDENTIFIER` names, and a native host table function. These are equivalent table
bindings, rather than a claim about Spark catalog persistence. The native host
scalar functions use the explicit `id + 1` reference. Disabled substitution uses
an equivalent `concat('$', '{payload}')` reference to preserve literal text.

The rejection test observes real provider calls, local-store reads and writes,
fixture bytes, catalog registrations and session configuration. Its observers
are calibrated with actual file operations and a native Delta query. A host table
function that reads a fixture Parquet file verifies that forbidden static and
literal path references are rejected before that read, even when they occur
later in the query. Computed forbidden names are checked after expression evaluation, when an
earlier host table function may already have performed I/O. The
[crate README](../../../README.md) and
[known differences](https://github.com/mag1cfrog/delta-arrow-reader/issues/1248)
describe this boundary.

From this directory, regenerate the independent Spark results with Python 3.12,
Java 21 and PySpark 4.2.0 at revision
`32f7299601108917fb01920a54e084595b7b3bf8`. The
[shared capture helper](../capture_spark_requests.py) checks those versions and
UTC settings. Choose a new output file in an existing directory:

```sh
TZ=UTC python3.12 capture_spark.py /path/to/new/spark-reference.jsonl
```

The helper uses the recorded in-memory inputs. It does not require Spark Delta
support.
