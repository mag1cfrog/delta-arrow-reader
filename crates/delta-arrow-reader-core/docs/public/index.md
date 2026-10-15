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
transactions.

## Start here

Choose how you want to read your table. Each quickstart links to the installation
steps for that interface:

- [Python](python.md): read rows with PyArrow or query them with SQL.
- [Command line](cli-quickstart.md): inspect a table or save rows to a file.
- [Rust](streaming-reader.md): process Arrow batches in your application.
- [DataFusion in Rust](datafusion.md): register a table and query it with SQL.

## After your first read

- Choose [initialization and resource settings](configuration.md) for your application.
- Learn [how the reader works](architecture.md).
- Review the [performance reports](benchmarks.md) and their test conditions.
- Look up a type or method in the [Python API reference](python-api/index.md) or
  [Rust API reference](https://docs.rs/delta-arrow-reader).
