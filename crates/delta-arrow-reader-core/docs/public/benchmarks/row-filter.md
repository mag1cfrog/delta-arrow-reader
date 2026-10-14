# Predicate decoding

To apply a `WHERE` condition, or predicate, a reader must decode the columns
used by that condition. This benchmark compares decoding just those three
columns with also decoding 64 columns that the filter does not use. Both cases
apply the same filter and return the same rows.

## Result

This local run took place on August 28, 2026, on an AMD Ryzen 7 8845HS with
8 cores, 16 threads, 27.95 GiB RAM, and NVMe storage, running Fedora 43 with
Linux 6.19.14. Each value is the median of five repetitions.

| Filter input | Columns decoded | Decode time | Peak memory | Matching rows |
| --- | ---: | ---: | ---: | ---: |
| Narrow | 3 | 0.425 ms | 9.020 MiB | 16 |
| Wide | 67 | 32.428 ms | 29.902 MiB | 16 |

Decoding only the three filter columns reduced median predicate decoding time
by 98.7%, from 32.428 ms to 0.425 ms. Median peak memory fell by 69.8%, from
29.902 MiB to 9.020 MiB. Peak memory is the process's resident set size (RSS),
the memory it holds in RAM. One MiB is 1,048,576 bytes.

Both cases returned the same 16 row IDs with a checksum of 122,880.

## Method

The synthetic Parquet file contains 16,384 rows, split into four row groups
(chunks of 4,096 rows). It has an integer `row_id`, three string columns used
by the filter, and 64 unrelated string payload columns. The filter matches one
row in every 1,024. The query's selected output columns, or output projection,
contain only `row_id`.

The narrow case gives the predicate its three referenced columns. The wide case
gives it those columns and all 64 payload columns. Everything else, including
the Parquet file, predicate, output projection, and expected rows, is identical.

Each measurement runs in a fresh child process so Linux peak resident memory is
comparable between cases. Case order reverses on alternating repetitions, and
there is no separate warmup run. The timer covers Parquet reader construction,
where the synchronous row-filter predicate is decoded and evaluated. The
benchmark records peak RSS at the end of that step, then reads and validates the
output rows.

This benchmark isolates predicate decoding and evaluation. It does not measure
an end-to-end Delta query, output-column decoding, or data-page I/O after rows
have been selected.

The wide projection is an artificial control within Delta Arrow Reader. For
a comparison with other readers, see [selective reads on wide tables](selective-read-results.md).

## Run the benchmark

Run five repetitions and save the raw CSV output:

```bash
mkdir -p target
cargo bench --locked --bench row_filter -- --repetitions 5 \
  > target/row-filter.csv
```

Use `--temp-dir PATH` to choose where the synthetic Parquet file is created.
Add `--retain-fixture` to keep it after the run for inspection. Without that
flag, the benchmark removes the fixture when it exits.

The CSV contains one row per case and repetition. It reports the predicate
projection and column count, qualifying row count, decoding time, peak RSS, and
row-ID checksum. Compare the narrow and wide rows within the same run. The
savings depend on the number, type, and width of unrelated columns. For the
effect on data-page reads after row selection, see the
[page-index benchmark](page-index.md).
