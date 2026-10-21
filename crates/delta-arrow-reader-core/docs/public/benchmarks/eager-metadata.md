# Repeated queries

Keeping a table open avoids repeating its initialization. In these runs, DAR's
first and second queries took similar time: both reused the prepared table,
and both still planned and read the full result. Query results were not cached.

Use this report to distinguish the one-time opening cost from query time. It
measures a table opened once and queried twice, rather than isolating the saving
from one particular cache.

## Results

Times are medians of five independent processes, in seconds. Startup is excluded.
These are the same wide-table cases as the [five-reader comparison](selective-read-results.md):
about 900 returned rows from about 60 million, with matches grouped together
or spread out. DV means rows are marked as deleted without rewriting the files.
Storage used MinIO with simulated latency and a shared bandwidth limit, as in
that comparison.

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

<details markdown="1">
<summary>Build, workload, and controlled network conditions</summary>

These are Delta Arrow Reader's published results from the
[wide-table comparison](selective-read-results.md), measured at merged commit
`cd3064e` with the default automatic warmup and partial-page policy. The queries
return about 900 rows and 70 columns from tables containing about 60 million
rows.

The reader had four physical CPU cores and an 8 GiB memory limit. Storage used
an emulated connection with 200 ms request latency (+/-20 ms jitter) and a
shared 150 Mbps bandwidth limit. Times are medians of five independent
processes, in seconds. Engine startup is excluded.

</details>

## Interpreting reuse savings

These runs use automatic warmup (`warmup="automatic"` in Python,
`WarmupMode::Automatic` in Rust). For their S3 tables, DAR prepares
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
