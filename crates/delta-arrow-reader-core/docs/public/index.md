---
title: Delta Arrow Reader
description: Read Delta Lake tables with Python, Rust, or the command line.
---

# Delta Arrow Reader

Delta Arrow Reader lets you read Delta Lake tables from Python, Rust, or the
command line. Select the rows and columns you need, then process them in batches
instead of loading the whole result into memory. Each batch is a group of rows
in Apache Arrow format, ready to use in your application.

The reader works with existing Delta tables. It does not write tables or manage
transactions. Queries run on your machine or in your application's environment,
without a Spark or Databricks cluster. Cloud reads use storage credentials,
which the CLI can obtain through [Unity Catalog](unity-catalog.md) using your
existing Databricks profile.

## Start here

Choose how you want to read your table. Each quickstart links to the installation
steps for that interface:

- [Python](python.md): read rows with PyArrow or query them with SQL.
- [Command line](cli-quickstart.md): inspect a table or save rows to a file.
- [Unity Catalog](unity-catalog.md): use a Databricks profile to read tables and run SQL locally.
- [Rust](streaming-reader.md): process Arrow batches in your application.
- [DataFusion in Rust](datafusion.md): register a table and query it with SQL.

## After your first read

- Troubleshoot a failed read in [Python](python-api/errors.md),
  [the command line](cli.md#errors-and-process-status), or [Rust](diagnostics.md).
- Connect to [S3 or Azure](configuration.md), or adjust opening time and resource use.
- Learn [how the reader works](architecture.md).
- Review the [performance reports](benchmarks.md) and their test conditions.
- Look up a type or method in the [Python API reference](python-api/index.md#api-reference) or
  [Rust API reference](https://docs.rs/delta-arrow-reader).
