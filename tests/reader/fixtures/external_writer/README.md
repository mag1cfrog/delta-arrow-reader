# External writer corpus

These three Delta tables were written by Apache Spark 4.0.1 and Delta Lake
4.0.0. Spark then read each latest snapshot, ordered the rows by `id`, and
exported its schema and complete result to `expected.arrow` with PyArrow
20.0.0. The reader and its synthetic fixture helpers take no part in generation.
The data is original synthetic data under the repository's [Apache-2.0
license](../../../../LICENSE).

| Table | Snapshot | Live / physical rows | Parquet layout | Coverage |
| --- | ---: | ---: | --- | --- |
| `partitioned` | 0 | 360 / 360 | 3 files, 2 row groups each | Null, east, and west partitions; nullable integer and string values |
| `nested_mapping` | 1 | 6 / 6 | 1 file, 1 row group | Name mapping, physical field IDs, a renamed nested field, null structs, null children, and an empty string |
| `deletion_vectors` | 1 | 9 / 12 | 1 file, 1 row group | Spark DELETE marks IDs 2, 5, and 12 in an external deletion vector |

The corpus budget is **256 KiB**, including the manifest and Arrow oracles.
The checked-in corpus occupies **41,631 bytes** across 15 files. Generator
source and this README are outside that data budget.

[manifest.json](corpus/manifest.json) records tool/runtime versions, the
generation command, Delta protocols and features, schemas, snapshot versions,
row counts, row-group counts, deletion-vector descriptors, and file sizes and
SHA-256 hashes. Each `expected_schema_and_rows` entry points to the Arrow IPC
file containing that table's complete Spark oracle.

## Run the tests

From the repository root:

```sh
cargo test --locked --test reader external_writer
cargo test --locked --features datafusion --test reader external_writer
```

Normal tests read only these local files. They need no Python, Java, Spark,
cloud credentials, or downloads beyond the usual Rust dependencies. The corpus
is under `tests/reader`, which is excluded from the published crate.

The tests check complete schemas and values against Spark, including nested
nulls and deleted-row exclusion. They compare filtered scans with Arrow
kernels applied to a verified full scan, with both visible and hidden filter
columns. Comparisons cover all six integer operators, null checks, all/no
matches, a deleted-row match, and nullable partition predicates. Results are
ordered by the unique `id`, so scan order is not part of the assertion.

Direct and Kernel run with cold and eagerly warmed metadata. DataFusion runs
both backends with string views enabled and disabled, batch size 7, and file
repartitioning enabled. String views and partition dictionaries are normalized
only after checking logical names, types, and nullability. The tests also check
actual Parquet row groups, mapped physical names/IDs, and retained DV rows.

## Regenerate

Use a Java 21 runtime and `uv`. From the repository root, choose a new output
directory; the generator refuses to overwrite an existing one:

```sh
JAVA_HOME=/path/to/java-21 SPARK_LOCAL_IP=127.0.0.1 \
  uv run --python 3.13.12 --script \
  tests/reader/fixtures/external_writer/generate.py \
  /tmp/new-external-writer-corpus
```

The script pins Spark, Delta, PyArrow, and pandas. The recorded run used
CPython 3.13.12 and Temurin 21.0.12.1+1. First use downloads the Python packages
and Delta's Maven artifacts. [Delta's compatibility table](https://docs.delta.io/releases/)
lists the Spark/Delta version pairing.

Read the regenerated Arrow oracles with `pyarrow.ipc.open_file(path).read_all()`
and compare their schemas and rows with the committed oracles before replacing
`corpus`. Re-run both test commands and update the recorded size. File names,
table/column UUIDs, and commit timestamps may differ between runs; byte-for-byte
Parquet and log identity is not required. Hadoop checksum sidecars are removed;
Parquet files, Delta JSON actions, and DV payloads are kept as written by Spark.

This is a small interoperability corpus, not exhaustive Delta protocol
certification. Production fixes and upstream Kernel limitations belong in
separate issues.
