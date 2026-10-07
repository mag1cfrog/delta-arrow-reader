# Page-index reads

Parquet stores each column in small chunks called pages. An offset index maps
pages to their locations in a file, so the reader can fetch just the pages that
contain selected rows. This can reduce downloads when a query returns only a
small part of a table.

This benchmark tests grouped matches, which fit on one page in each row group
(a larger chunk of rows), and scattered matches, which touch every page. Each
layout is written once with offset indexes and once without them. Both files
contain the same data and return the same result.

## Result

This local run took place on August 28, 2026, on an AMD Ryzen 7 8845HS with
8 cores, 16 threads, 27.95 GiB RAM, and NVMe storage, running Fedora 43 with
Linux 6.19.14. Each value is the median of five repetitions.

These timings include result validation and hashing. The current
harness validates once before timing, as described below, so new latency
measurements use a different boundary.

| Match layout | Offset index | First batch | Total | Bytes received | Range requests |
| --- | --- | ---: | ---: | ---: | ---: |
| Grouped | Present | 0.742 ms | 1.364 ms | 1.920 MiB | 35 |
| Grouped | Absent | 14.067 ms | 28.791 ms | 54.057 MiB | 5 |
| Scattered | Present | 15.060 ms | 30.795 ms | 54.057 MiB | 5 |
| Scattered | Absent | 14.400 ms | 29.697 ms | 54.057 MiB | 5 |

First batch measures the wait for the first rows; total time covers the full
result. A range request reads part of a file. No case requested a whole file.
One MiB is 1,048,576 bytes.

For grouped matches, the offset index reduced bytes received by 96.4% and
reduced median total time from 28.791 ms to 1.364 ms. It also increased the
number of range requests from 5 to 35 because the reader fetched selected page
ranges instead of complete column chunks.

Scattered matches covered every data page, so both files required 54.057 MiB
and five range requests. The indexed case took 3.7% longer in this run. This is
the case where loading the index did not produce a narrower data read.

## Method

Each fixture contains two row groups of 4,096 rows. A data page contains 128
rows, and the query projects 16 nullable string payload columns while filtering
on a separate string column. Both layouts return 64 rows. The grouped layout
places all 32 matches for a row group in its first page. The scattered layout
places one match in each of the row group's 32 pages.

The indexed Parquet file is 56,710,718 bytes. The unindexed file is 56,692,272
bytes. Dictionary encoding is disabled so each payload column chunk stays
larger than the object store's 1 MiB coalescing threshold. This keeps selected
page ranges separate instead of merging them into a complete column-chunk read.

The benchmark uses the public streaming API with the `Direct` backend and one
scan partition. It loads each Delta table and builds each scan before starting
the timer. Time to first batch starts when the stream is first polled. Total
time ends after the stream is exhausted. Timed consumption counts and releases
batches; a separate untimed pass checks every value and computes the fingerprint
before any repetitions. The data-file metrics count bytes and GETs issued by
the direct Parquet reader; they do not include Delta log reads.

Every output value and null position in the validation pass contributes to a
result fingerprint. The indexed and unindexed grouped runs both produced
`fnv1a64:f727bcfaa4e3933f`. Both scattered runs produced
`fnv1a64:ce7e5b1c0cc9b9bf`. The benchmark stops with an error if either pair
returns different rows, values, ordering, or null placement.

The case order is reversed on alternating repetitions. The validation pass warms
each fixture; there is no additional warmup. File-system cache state, storage
latency, and hardware affect the timing, so use the byte and request counts
alongside the latency measurements.

This compares two Delta Arrow Reader configurations. For a comparison with
other readers, see [selective reads on wide tables](selective-read-results.md).

## Run the benchmark

Run five repetitions and save the raw CSV output:

```bash
mkdir -p target
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
