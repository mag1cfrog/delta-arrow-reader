# Repeated queries

Keeping a table open lets a reader reuse metadata and its measured connection
profile. The public benchmark tests this pattern by opening a table once and
running the same query twice, without caching the query result.

## Results

These are Delta Arrow Reader's current results from the
[wide-table comparison](selective-read-results.md), measured at merged commit
`cd3064e` with the default automatic warmup and partial-page policy. The queries
return about 900 rows and 70 columns from tables containing about 60 million
rows. Grouped matches sit close together; scattered matches are spread out.
Deletion vectors (DVs) mark deleted rows without rewriting the data files.

The reader had four physical CPU cores and an 8 GiB memory limit. Storage used
an emulated connection with 200 ms request latency (+/-20 ms jitter) and a
shared 150 Mbps bandwidth limit. Times are medians of five independent
processes, in seconds. Engine startup is excluded.

| Table / matches | Initialization | First query | Second query |
| --- | ---: | ---: | ---: |
| 416 columns, grouped | 4.024 | 1.730 | 1.739 |
| 416 columns, scattered | 4.094 | 6.254 | 6.247 |
| 416 columns, grouped + DV | 4.429 | 2.001 | 1.997 |
| 416 columns, scattered + DV | 4.480 | 6.512 | 6.455 |
| 90 columns, grouped | 3.711 | 1.689 | 1.704 |
| 90 columns, scattered | 4.101 | 9.975 | 9.956 |
| 90 columns, grouped + DV | 3.804 | 1.979 | 2.060 |
| 90 columns, scattered + DV | 3.705 | 10.274 | 10.243 |

The README chart uses the first-query column, excluding initialization.
The second query shares the initialized table but still plans and reads its
full result. These paired queries give five independent samples per query
position. The [full comparison](selective-read-results.md#initialization-and-repeated-queries)
includes quartiles, initialization-plus-first-query timings, and results for
all five readers.

## Interpreting reuse savings

These runs use `WarmupMode::Automatic`. For their S3 tables, DAR prepares
reusable metadata and samples the connection while loading the table. Both
queries share that initialized provider. The measurements combine several
effects: avoiding repeated initialization, reusing metadata and the connection
profile, and other cache effects. They do not isolate any one of these or
compare lazy and eager metadata loading.

Preparing a table takes time and retains state in memory. Consider
initialization and all queries together when choosing a mode for your
application. The [metadata lifecycle guide](../delta-metadata-lifecycle.md#which-mode-should-you-use)
explains the tradeoff. For a controlled lazy/eager experiment, see the
[repository benchmark instructions](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/eager-metadata.md).
