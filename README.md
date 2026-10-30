# Delta Arrow Reader

<h3 align="center">
  <strong>Delta Lake in. Arrow batches out. No Spark required.</strong>
</h3>

<p align="center">
  <a href="https://mag1cfrog.github.io/delta-arrow-reader/"><img alt="User Guide" src="https://img.shields.io/badge/User_Guide-e5e7eb?style=flat-square"></a>
  <a href="https://mag1cfrog.github.io/delta-arrow-reader/python-api/"><img alt="Python API" src="https://img.shields.io/badge/Python_API-e5e7eb?style=flat-square"></a>
  <a href="https://docs.rs/delta-arrow-reader"><img alt="Rust API" src="https://img.shields.io/badge/Rust_API-e5e7eb?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://pypi.org/project/delta-arrow-reader/"><img alt="PyPI" src="https://img.shields.io/pypi/v/delta-arrow-reader.svg?style=flat-square&amp;color=007ec6&amp;label=PyPI"></a>
  <a href="https://crates.io/crates/delta-arrow-reader"><img alt="crates.io" src="https://img.shields.io/crates/v/delta-arrow-reader.svg?style=flat-square&amp;color=ea7233"></a>
  <a href="https://github.com/mag1cfrog/delta-arrow-reader/blob/main/LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/Apache--2.0-e5e7eb?style=flat-square"></a>
  <a href="https://github.com/mag1cfrog/delta-arrow-reader/actions/workflows/docs.yml?query=branch%3Amain%20event%3Apush"><img alt="Documentation build and deployment" src="https://img.shields.io/github/actions/workflow/status/mag1cfrog/delta-arrow-reader/docs.yml?branch=main&amp;event=push&amp;style=flat-square&amp;label=docs"></a>
</p>

Delta Arrow Reader reads Delta Lake tables from your terminal, Python, or Rust.

> **Bring your Databricks login.**
>
> Use your existing Databricks CLI profile to read Unity Catalog tables and run
> SQL locally. No separate cloud credentials to configure, no cluster or SQL
> warehouse to start.
>
> See the [Unity Catalog guide](https://mag1cfrog.github.io/delta-arrow-reader/unity-catalog/)
> for access requirements and supported tables.

- **Get results faster.** Spend less time downloading and processing data your
  query doesn't need.
- **Use less memory.** Process results as they arrive, without keeping the whole
  result in RAM.
- **Skip the cluster setup.** Read and filter Delta tables in your terminal or
  application, without running Spark.

**Faster than delta-rs, DuckDB, Polars, and single-machine Spark** in all eight
of our [filtered-read benchmarks](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/),
using TPC-H-derived tables with about 60 million rows each. Results compare median
times for the first complete query after table initialization, excluding startup.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/852250d4dd9edd82f8295f75673e17b9a002b068/docs/public/assets/selective-read-readme-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/852250d4dd9edd82f8295f75673e17b9a002b068/docs/public/assets/selective-read-readme-light.svg">
  <img alt="Five readers query Delta Lake tables with about 60 million rows derived from TPC-H. The four cases use deletion vectors, with matching rows grouped together or spread out in 416-column and 90-column tables. Bars show median seconds for the first complete query after initialization, excluding startup and table initialization. Linear scales start at zero; shorter is faster. The 416-column panels span 0-80 seconds; the 90-column panels span 0-200 seconds." src="https://raw.githubusercontent.com/mag1cfrog/delta-arrow-reader/852250d4dd9edd82f8295f75673e17b9a002b068/docs/public/assets/selective-read-readme-light.svg" width="1000">
</picture>

<sub>Showing four cases using deletion vectors to mark deleted rows. <a href="https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/">Full results</a> include all eight cases, with and without deletion vectors.</sub>

## Get started

Start with the [command line](#cli), [Python](#python), or [Rust](#rust).

### CLI

Preview rows as JSON, run SQL to filter, group, or join tables, and save results
as Arrow or JSON. Queries run locally.

Choose one option with [uv](https://docs.astral.sh/uv/getting-started/installation/)
or [pipx](https://pipx.pypa.io/stable/how-to/install-pipx.html):

| Use | Command |
| --- | --- |
| Try it with uv, without installing DAR first | `uvx --no-build --from delta-arrow-reader dar --help` |
| Install with uv | `uv tool install --no-build delta-arrow-reader` |
| Install with pipx | `pipx install delta-arrow-reader` |

After installing, use `dar` directly. For a one-off run with uvx, prefix the
examples below with `uvx --no-build --from delta-arrow-reader`.

Use a working Databricks CLI profile to preview a Unity Catalog table:

```bash
dar preview uc://main.sales.orders --databricks-profile work --limit 3
```

Replace the table name and `work` with your own. The `databricks` command must be
on your `PATH`. Unity Catalog must allow external data access, and your identity
needs the [required table and schema permissions](https://mag1cfrog.github.io/delta-arrow-reader/reference/unity-catalog/#table-access),
including `EXTERNAL USE SCHEMA`. For S3 tables, also pass a
[region options file](https://mag1cfrog.github.io/delta-arrow-reader/reference/unity-catalog/#s3-region)
with `--storage-options-file storage.json`.

For a local Delta table, supply its directory:

```bash
dar preview /path/to/delta-table --limit 3
```

Continue with the [CLI quickstart](https://mag1cfrog.github.io/delta-arrow-reader/cli-quickstart/)
or [SQL examples](https://mag1cfrog.github.io/delta-arrow-reader/cli-workflows/).
See [installation](https://mag1cfrog.github.io/delta-arrow-reader/installation/#command-line)
for supported platforms, standalone `dar` binaries, and containers.

### Python

Install from PyPI (Python 3.10 or later):

```bash
pip install delta-arrow-reader-py
```

Read tables as PyArrow batches or query them with SQL. Start with the
[Python quickstart](https://mag1cfrog.github.io/delta-arrow-reader/python/)
or browse the [Python API reference](https://mag1cfrog.github.io/delta-arrow-reader/python-api/).
See [installation](https://mag1cfrog.github.io/delta-arrow-reader/installation/#python)
for supported platforms.

### Rust

Install the reader and Tokio (Rust 1.94 or later):

```bash
cargo add delta-arrow-reader
cargo add tokio --features macros,rt-multi-thread
```

Read up to 100 rows from an existing Delta table:

```rust,no_run
use delta_arrow_reader::DeltaTableBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = DeltaTableBuilder::new("/path/to/delta-table")
        .load_table()
        .await?;
    let scan = table.scan().with_limit(100).build().await?;
    let mut batches = scan.into_stream();

    while let Some(batch) = batches.next_batch().await? {
        println!("Read {} rows", batch.num_rows());
    }
    Ok(())
}
```

Save this as `src/main.rs`, replace the table path, and run `cargo run`.

[Read a table](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
or [query with SQL](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/).
See the [Rust API reference](https://docs.rs/delta-arrow-reader) for types and methods.

For development and benchmark setup, see the
[repository documentation](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/README.md).
