---
title: Performance
description: Results for selective Delta queries, repeated queries, filtering, page-index reads, and range planning.
---

# Performance

Start with [selective reads on wide tables](benchmarks/selective-read-results.md)
for a comparison with delta-rs, DuckDB, Polars, and single-machine Spark. The
benchmark returns about 900 rows and 70 columns from public, TPC-H-derived
tables with about 60 million rows. It tests matching rows stored close together
or spread out, and deletion vectors, which mark deleted rows without rewriting
the data files.

The report includes all eight cases, timing distributions, downloaded bytes,
request counts, and test conditions. Compare readers within the same case:
layout, cache state, and storage latency affect the result.

## More reports

The repeated-query report looks at reusing an open table. The other experiments
isolate parts of Delta Arrow Reader's read path using small generated tables.

| Report | What it measures |
| --- | --- |
| [Repeated queries](benchmarks/eager-metadata.md) | The first query, including table initialization, compared with a second query on the same open table. |
| [Predicate decoding](benchmarks/row-filter.md) | Decoding just the columns needed by a `WHERE` filter. |
| [Page-index reads](benchmarks/page-index.md) | Skipping parts of a file when matching rows are stored close together. |
| [Range planning](benchmarks/range-planning.md) | Trading fewer requests for more downloaded bytes under controlled latency and throughput. |

For fixture generation, reader builds, and measurement commands, see the
[benchmark instructions in the repository](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/benches/selective_read/README.md).
