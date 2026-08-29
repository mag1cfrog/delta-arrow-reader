# Parquet row-filter predicate decoding

A Parquet row filter must decode the columns referenced by its predicate before
it can decide which rows to keep. This benchmark compares decoding only those
three columns with decoding the same columns plus 64 unrelated payload columns.
Both cases apply the same predicate and return the same rows.

## Result

This five-repetition local run took place on August 28, 2026, using the same
machine as the [reader comparison](../benchmarks.md#environment). Each value is
the median across the five repetitions.

| Predicate projection | Predicate columns | Decode time | Peak RSS | Qualifying rows |
| --- | ---: | ---: | ---: | ---: |
| Narrow | 3 | 0.425 ms | 9.020 MiB | 16 |
| Wide | 67 | 32.428 ms | 29.902 MiB | 16 |

The wide case is the comparison baseline. It makes all 67 string columns
available to the predicate even though the predicate references only three.
Restricting the projection to those three columns reduced median predicate
decoding time by 98.7%, from 32.428 ms to 0.425 ms. Median peak RSS fell by
69.8%, from 29.902 MiB to 9.020 MiB. Put another way, the wide case took 76.3
times as long and used 3.32 times the peak memory of the narrow case.

Both cases returned the same 16 row IDs with a checksum of 122,880.

## Method

The synthetic Parquet file contains four row groups of 4,096 rows, for 16,384
rows in total. It has an integer `row_id`, three string columns used by the
predicate, and 64 unrelated string payload columns. The predicate matches one
row in every 1,024, and the output projection contains only `row_id`.

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

## Run the benchmark

Run five repetitions and save the raw CSV output:

```bash
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
