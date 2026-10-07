# Repeated queries

Keeping a table open lets a reader reuse metadata, such as the list of files
belonging to that table version. The public benchmark tests this pattern by
opening a table once and running the same query twice, without caching the
query result.

## Results

These are Delta Arrow Reader's results from the
[wide-table comparison](selective-read-results.md). The queries return about
900 rows and 70 columns from tables containing about 60 million rows. Grouped
matches sit close together; scattered matches are spread out. Deletion vectors
(DVs) mark deleted rows without rewriting the data files.

The reader had four physical CPU cores and an 8 GiB memory limit. Storage used
an emulated connection with 200 ms request latency (+/-20 ms jitter) and a
shared 150 Mbps bandwidth limit. Times are medians of five independent runs,
in seconds. Process startup is excluded.

| Table / matches | Initialization + first query | Second query |
| --- | ---: | ---: |
| 416 columns, grouped | 5.595 | 3.768 |
| 416 columns, scattered | 11.388 | 10.125 |
| 416 columns, grouped + DV | 6.326 | 4.067 |
| 416 columns, scattered + DV | 11.977 | 10.350 |
| 90 columns, grouped | 7.036 | 4.300 |
| 90 columns, scattered | 22.954 | 22.063 |
| 90 columns, grouped + DV | 7.307 | 4.508 |
| 90 columns, scattered + DV | 23.280 | 22.349 |

The first timing column includes table initialization and the first complete
query. The second query reuses the initialized table but still plans and reads
its full result. The two queries in each run are paired observations, not ten
independent samples. The [full comparison](selective-read-results.md#repeat-a-query)
includes quartiles, first-query timings, and results for all five readers.

The second query had a lower median time in all eight DAR cases. For the
416-column table with grouped matches and no DVs, the time fell from 5.595 to
3.768 seconds. For the 90-column table with scattered matches and no DVs, it
fell from 22.954 to 22.063 seconds. The benefit varies with the query and table.

## Interpreting reuse savings

DAR uses `WarmupMode::QueryPlanning` for these runs, preparing reusable Delta
metadata while loading the table. Both queries use that mode. The comparison
measures the combined effect of avoiding initialization and reusing the source;
it does not isolate metadata caching from other cache effects or compare lazy
and eager metadata loading.

Preparing metadata takes time and retains it in memory. Consider initialization
and all queries together when choosing a mode for your application. The
[metadata lifecycle guide](../delta-metadata-lifecycle.md#which-mode-should-you-use)
explains the tradeoff. For a controlled lazy/eager experiment,
see the [repository benchmark instructions](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/eager-metadata.md).
