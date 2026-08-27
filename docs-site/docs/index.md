---
title: Delta Arrow Reader
description: Read Delta Lake tables as streaming Apache Arrow batches or DataFusion tables.
---

# Delta Arrow Reader

Delta Arrow Reader lets Rust applications read Delta Lake tables as a stream
of Apache Arrow record batches. Because rows arrive in batches, your
application can start working with them without first loading the whole result
into memory.

You can use the stream directly or register the table with DataFusion and query
it with SQL. The crate focuses only on reading; it does not write Delta tables
or manage transactions.

## Start here

First, [install the crate](installation.md). Then choose the quickstart that
fits your application:

1. [Read a table as a stream](streaming-reader.md) if you want to work with an Arrow
   batch stream.
2. [Query a table with DataFusion](datafusion.md) if you want to use SQL.

## After your first read

- Learn [how the reader works](architecture.md).
- Review the [reader benchmarks](benchmarks.md) and their test conditions.
- Look up a type or method in the
  [Rust API reference](https://docs.rs/delta-arrow-reader).
