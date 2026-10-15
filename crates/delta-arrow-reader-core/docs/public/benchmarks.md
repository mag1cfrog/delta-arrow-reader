---
title: Performance
description: Compare readers on selective queries, then explore which parts of a read account for the savings.
---

# Performance

Start with the [wide-table comparison](benchmarks/selective-read-results.md)
to compare Delta Arrow Reader with delta-rs, DuckDB, Polars, and single-machine
Spark. DAR was fastest in all eight tested cases. Each query returns about
900 rows and 70 columns from a table of about 60 million rows derived from TPC-H.

The report uses the same input and resource limits for each reader. It includes
queries with matching rows grouped together or spread out, with and without
deletion vectors (rows marked as deleted). These are custom selective queries,
not standard TPC-H queries or a TPC-H score.

## Choose a report

| What you want to know | Read |
| --- | --- |
| How do the five readers compare on the same queries? | [Selective reads on wide tables](benchmarks/selective-read-results.md) |
| What does opening a table cost, and what happens on a second query? | [Repeated queries](benchmarks/eager-metadata.md) |
| Does reading selected values within pages help on real S3? | [Partial-page results](partial-page-reads.md#benchmark-results) |

Compare results within the same test environment. The wide-table comparison
uses MinIO with controlled latency and bandwidth; the real S3 test uses a
connection from Los Angeles to Oregon. Their absolute times are not directly
comparable. Opening time and query time are reported separately.

## Understand individual optimizations

These smaller experiments isolate one part of reading. Their millisecond
timings explain a mechanism, rather than predict a full application query.

| Experiment | Question |
| --- | --- |
| [Predicate decoding](benchmarks/row-filter.md) | How much work does a filter save by decoding only the columns it needs? |
| [Page-index reads](benchmarks/page-index.md) | When can selected rows leave whole pages unread? |
| [Range planning](benchmarks/range-planning.md) | When is downloading extra bytes faster than making more requests? |

Each report includes its test conditions and commands. For the five-reader
comparison's fixture generation and pinned builds, use the
[reproduction instructions](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/README.md).
