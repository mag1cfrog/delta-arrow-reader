# Parquet page-index range reads

The page-index benchmark measures two row-filter layouts. Localized matches are
contained in one data page per row group. Scattered matches touch every data
page. Each layout is written once with offset indexes and once without them, so
the benchmark can compare the same query and logical data in both files.

## Result

This five-repetition local run took place on August 28, 2026, using the same
machine as the [reader comparison](../benchmarks.md#environment). Each value is
the median across the five repetitions.

| Match layout | Offset index | First batch | Total | Bytes received | Range GETs |
| --- | --- | ---: | ---: | ---: | ---: |
| Localized | Present | 0.742 ms | 1.364 ms | 1.920 MiB | 35 |
| Localized | Absent | 14.067 ms | 28.791 ms | 54.057 MiB | 5 |
| Scattered | Present | 15.060 ms | 30.795 ms | 54.057 MiB | 5 |
| Scattered | Absent | 14.400 ms | 29.697 ms | 54.057 MiB | 5 |

All four cases reported zero full GETs.

For localized matches, the offset index reduced bytes received by 96.4% and
reduced median total time from 28.791 ms to 1.364 ms. It also increased the
number of range requests from 5 to 35 because the reader fetched selected page
ranges instead of complete column chunks.

Scattered matches covered every data page, so both files required 54.057 MiB
and five range requests. The indexed case took 3.7% longer in this run. This is
the case where loading the index did not produce a narrower data read.

## Method

Each fixture contains two row groups of 4,096 rows. A data page contains 128
rows, and the query projects 16 nullable string payload columns while filtering
on a separate string column. Both layouts return 64 rows. The localized layout
places all 32 matches for a row group in its first page. The scattered layout
places one match in each of the row group's 32 pages.

The indexed Parquet file is 56,710,718 bytes. The unindexed file is 56,692,272
bytes. Dictionary encoding is disabled so each payload column chunk stays
larger than the object store's 1 MiB coalescing threshold. This keeps selected
page ranges separate instead of merging them into a complete column-chunk read.

The benchmark uses the public streaming API with the `Direct` backend and one
scan partition. It loads each Delta table and builds each scan before starting
the timer. Time to first batch starts when the stream is first polled. Total
time ends after the stream is exhausted and its results have been checked. The
data-file metrics count bytes and GETs issued by the direct Parquet reader; they
do not include Delta log reads.

Every output value and null position contributes to a result fingerprint. The
indexed and unindexed localized runs both produced
`fnv1a64:f727bcfaa4e3933f`. Both scattered runs produced
`fnv1a64:ce7e5b1c0cc9b9bf`. The benchmark stops with an error if either pair
returns different rows, values, ordering, or null placement.

The case order is reversed on alternating repetitions. The benchmark does not
perform a separate warmup run. File-system cache state, storage latency, and
hardware affect the timing, so use the byte and request counts alongside the
latency measurements.

## Run the benchmark

Run five repetitions and save the raw CSV output:

```bash
cargo bench --locked --bench page_index -- --repetitions 5 \
  > target/page-index.csv
```

Use `--temp-dir PATH` to choose where the synthetic Delta tables are created.
Add `--retain-fixtures` to keep them after the run for inspection. Without that
flag, the benchmark removes the fixtures when it exits.

The CSV contains one row per case and repetition. It reports the fixture size,
qualifying row count, result fingerprint, first-batch time, total time, range
and full GET counts, and bytes received. Compare the indexed and unindexed rows
within the same match layout. A selective row filter can save data-page reads
only when its selected rows leave some pages untouched. Storage request latency
also matters because reading fewer bytes may require more range requests.
