# Delta metadata lifecycle

Every Delta query needs to know which Parquet files belong to the loaded table
version. It can then narrow that list using the query predicate, partition
values, and file statistics. The Delta log or checkpoint supplies the file
list. The list and the information used to prune it are the table's scan
metadata.

`WarmupMode` controls how much reusable metadata `load_table` prepares. The
default `None` mode waits until each scan is built. `QueryPlanning` prepares the
reusable Delta metadata while the table loads. The experimental
`ParquetMetadata` mode also loads parsed footers and offset indexes for every
active file.

Each query still gets its own file, row-group, and page selection. The caches
do not reuse one query's selection for another query or change query results.

## What happens before rows are read

A query against remote storage can involve three rounds of I/O:

1. The reader uses the Delta log or checkpoint to find the active files. Delta
   Kernel applies the query predicate to their partition values and file
   statistics, removing files that cannot match.
2. The reader fetches the remaining Parquet footers. Row-group statistics may
   narrow the read again.
3. The reader fetches the selected Parquet row groups and produces Arrow
   batches.

The first round mixes work that can be reused with work that belongs to one
query. Replaying the Delta history and reconciling the active files can be
reused for a loaded table version. Applying a predicate cannot.
`WarmupMode::QueryPlanning` moves only that reusable work to table
initialization. `WarmupMode::ParquetMetadata` also moves the reusable part of
the second round while leaving row-group and page selection with each query.

This difference matters most for a selective query. Statistics may reduce a
large table to a few Parquet row groups, making the data read small, while
Delta replay remains a fixed planning cost. On remote storage, that planning
cost can take longer than reading the result. Reusing active-file metadata
removes the repeated replay without changing the later pruning or Parquet I/O.

## No warmup

`DeltaTableBuilder::load_table()` uses `WarmupMode::None` by default:

```text
table load -> table version and schema
query 1 -> Delta replay -> query pruning -> Parquet footer/data
query 2 -> Delta replay -> query pruning -> Parquet footer/data
query 3 -> Delta replay -> query pruning -> Parquet footer/data
```

Loading the table selects a version and reads its schema. The reader waits
until a scan is built to assemble the active-file metadata and ask Delta
Kernel to prune it for that scan.

This keeps table loading quick and avoids retaining an active-file cache. The
tradeoff is that every scan against the loaded table repeats the Delta replay.
No warmup usually fits tables that are queried once, queried occasionally, or
loaded in a process with tight memory limits.

## Query-planning warmup

`DeltaTableBuilder::with_warmup(WarmupMode::QueryPlanning)` prepares the same
reusable metadata before `load_table()` returns:

```text
table initialization -> Delta replay -> metadata cache
                                           |
query 1 -----------------------------------+-> pruning -> Parquet footer/data
query 2 -----------------------------------+-> pruning -> Parquet footer/data
query 3 -----------------------------------+-> pruning -> Parquet footer/data
```

The method returns after the cache is ready, so table initialization takes
longer. The cache contains the reconciled Delta `add` metadata for the current
active files, including available file statistics. It does not contain the raw
commit history.

When a scan is built, the reader gives this metadata back to Delta Kernel
through `Scan::scan_metadata_from`. Delta Kernel applies that scan's predicate
and statistics pruning. Different queries can therefore select different
files while sharing the result of Delta replay.

The cache belongs to one loaded table. If the same location is loaded twice,
the two table objects have separate caches. Both also stay fixed at the exact
table version they loaded.

## Parquet metadata warmup

`DeltaTableBuilder::with_warmup(WarmupMode::ParquetMetadata { .. })` first
performs query-planning warmup. It then fetches and parses the Parquet footer
and optional offset indexes for every unique active file:

```text
table initialization -> Delta replay -> active-file cache -> Parquet metadata cache
                                                                    |
query 1 -> pruning -------------------------------------------------+-> Parquet data
query 2 -> pruning -------------------------------------------------+-> Parquet data
query 3 -> pruning -------------------------------------------------+-> Parquet data
```

The experimental Cargo feature is disabled by default, and this warmup mode
requires explicit file-count and estimated-memory limits. It supports only the
direct Parquet backend. The returned table and its clones share one immutable
cache through both the streaming API and DataFusion.

Warmup does not read Parquet data pages. A query still decides which
files, row groups, pages, and columns it needs. See
[Warm Parquet metadata for repeated queries](https://mag1cfrog.github.io/delta-arrow-reader/prepared-parquet-metadata/)
for setup, limits, failure behavior, and report fields.

## Which mode should you use?

This choice applies to the loaded table, whether you query it through the
streaming API or DataFusion SQL. Both APIs support all three modes.

| Consideration | No warmup | Query planning | Parquet metadata |
| --- | --- | --- | --- |
| Typical use | One query, occasional queries, or tight memory limits | Repeated queries where Delta replay is costly | Repeated remote queries where footer reads are also costly |
| Table loading | Returns without building reusable scan caches | Waits for Delta replay and cache creation | Also fetches and parses metadata for every active Parquet file |
| Memory while loaded | No reusable active-file cache | Keeps active-file metadata and statistics | Also keeps parsed Parquet footers and offset indexes |
| Each later scan | Replays Delta metadata, then prunes | Reuses Delta metadata, then prunes | Reuses both caches, then performs query-specific pruning |
| Seeing a newer version | Load a new table | Load a new table | Load and warm a new table |
| Parquet data reads | Per query | Per query | Per query |

Use no warmup unless you know the table will serve repeated queries and the
saved planning time justifies slower initialization and higher memory use. Add
Parquet metadata warmup only when measurements show that repeated footer
reads matter. No single query count is a reliable break-even point. The result
depends on the table history, checkpoint shape, file count, available
statistics, storage latency, query selectivity, and memory pressure.

## Results from a real S3 workload

On August 27, 2026, a real-S3 benchmark loaded two production-shaped tables
and ran six queries:

| Measurement | No warmup | Query-planning warmup |
| --- | ---: | ---: |
| Table initialization | 1.196 s | 4.089 s |
| HIP physical planning | 2.097 s | 5.1 ms |
| Schedule physical planning | 756 ms | 1.1 ms |
| Complete six-query session | 50.205 s | 42.451 s |
| Resident memory after initialization | 38.5 MiB | 68.7 MiB |
| Full-session peak resident memory | 233.9 MiB | 248.9 MiB |

The warmed run spent more time and memory during initialization. After that,
physical planning fell from seconds to milliseconds, and the six-query
session finished sooner. Both runs returned the same results and performed the
same Parquet I/O.

These numbers come from one workload. They are not a performance guarantee or
a general break-even point. The [benchmark methodology, environment, and limitations](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/eager-metadata/#representative-real-s3-result)
describe how the measurements were collected and what can affect them.
This case study compares no warmup with query-planning warmup; it does not
include experimental Parquet metadata warmup.

## Version and refresh behavior

The cache lasts as long as its loaded table and represents one exact Delta
version. Commits written later do not change what existing queries see. To use
a newer version, load the table again and replace the old table or DataFusion
registration.

The in-memory warmup modes do not provide:

- incremental refresh;
- background polling;
- TTL or eviction;
- persistence across processes;
- sharing between independently loaded tables;
- Parquet data-page or decoded-batch caching; or
- automatic selection of which tables to warm.

## Related guides

- [Read a table with the streaming API](https://mag1cfrog.github.io/delta-arrow-reader/streaming-reader/)
- [Register and query a table with DataFusion](https://mag1cfrog.github.io/delta-arrow-reader/datafusion/)
- [Understand how scans are planned](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/)
- [Review the benchmark methodology](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/eager-metadata/)
- [Warm Parquet metadata for repeated queries](https://mag1cfrog.github.io/delta-arrow-reader/prepared-parquet-metadata/)
- [Open the warmup Rust API](https://docs.rs/delta-arrow-reader/latest/delta_arrow_reader/struct.DeltaTableBuilder.html#method.with_warmup)
