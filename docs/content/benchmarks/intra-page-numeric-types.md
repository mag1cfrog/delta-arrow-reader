# Numeric partial-page reads

[Issue #450](https://github.com/mag1cfrog/delta-arrow-reader/issues/450) adds flat
PLAIN INT32, INT64, FLOAT and DOUBLE pages, including required columns, to the
existing partial-read path. These measurements compare DAR before and after the
change. They use the first fully consumed query after table initialization.

The [samples](intra-page-numeric-types-samples.csv) retain initialization, first
and second query times, requests, bytes, CPU and peak RSS for each run. The
[build and fixture identities](intra-page-numeric-types-results.json) identify
the executable and source hashes. Raw observations, validation exports, earlier
attempts and reproduction scripts are retained under the recorded artifact root.

## Retained README cases

Both builds read the original TPC-H-derived SF10 snapshots, queries and reference
results. All four cases use deletion vectors. The restored 416-column spread-out
table matches every original Parquet hash and retains its original Delta logs
and deletion vectors.

| Case | Base query, seconds | Candidate query, seconds | Time change |
| --- | ---: | ---: | ---: |
| 416 columns, grouped matches | 1.963 | 2.017 | +2.8% |
| 416 columns, spread-out matches | 6.425 | 6.470 | +0.7% |
| 90 columns, grouped matches | 2.149 | 2.127 | -1.1% |
| 90 columns, spread-out matches | 10.229 | 10.255 | +0.3% |

The grouped cases include two additional alternating pairs after the initial
three pairs showed second-query variation. All five pairs remain in the sample
file. The 416-column second-query medians are 1.976 and 2.005 seconds; the
90-column medians are 2.121 and 2.158 seconds. Separate diagnostic runs issued
identical Parquet ranges and bytes for both queries in each layout. DV request
counts varied slightly. The 416-column whole-process CPU medians were 1.367 and
1.373 seconds.

The 416-column first-query difference is still +2.8%, about 55 ms. A declared
five-pair A/A control then ran the same base executable on both sides: its
medians were 2.031 and 1.962 seconds, a 68 ms difference without any code change.
Those samples appear separately as `q2-grouped-aa`, with builds `baseline-a`
and `baseline-b`. They do not replace any A/B samples.

Together with matching diagnostic reads and nearly unchanged CPU usage, this
did not identify a repeatable material regression. The measurements cannot
reliably resolve differences of a few percent in this case. The +2.8% observation
is retained; the 5% investigation trigger is not treated as an allowed slowdown.

## Added type cases

The separate numeric fixture has 1,048,576 rows in two Parquet files, totaling
165,724,621 bytes. Each file contains one row group. Each query projects eight
columns of the selected type, alternating required and nullable, plus a row ID.
A separate predicate selects 129 rows. Values are deterministic random bits;
Arrow references are generated directly from the input, independently of DAR.

Pages are uncompressed PLAIN V1 with offset indexes, up to 524,288 rows and a
4 MiB page target. This layout makes individual values addressable and leaves
enough data to skip. Smaller-page pilot results are retained too: eligibility
does not guarantee a benefit for every page size.

| Type | Base query, seconds | Candidate query, seconds | Base query MB | Candidate query MB |
| --- | ---: | ---: | ---: | ---: |
| INT32 | 2.626 | 1.534 | 38.41 | 9.99 |
| INT64 | 4.212 | 1.571 | 63.62 | 10.47 |
| FLOAT | 2.638 | 1.563 | 38.42 | 10.16 |
| DOUBLE | 3.982 | 1.578 | 63.56 | 10.60 |

Bytes here cover Parquet reads during the first query. Partial reads trade more
requests for fewer bytes; the per-run request counts are in the sample file.

## Controls and regressions found

| Control | Base query, seconds | Candidate query, seconds |
| --- | ---: | ---: |
| Dense, INT32 | 2.243 | 2.232 |
| Dense, INT64 | 3.596 | 3.603 |
| Snappy, INT32 | 2.221 | 2.217 |
| Snappy, INT64 | 3.574 | 3.571 |
| Local, INT32 | 0.017 | 0.016 |
| Local, INT64 | 0.024 | 0.024 |
| 2 ms, 2 Gbit/s, INT32 | 0.555 | 0.244 |
| 2 ms, 2 Gbit/s, INT64 | 0.343 | 0.305 |

Local, dense and Snappy controls retain ordinary I/O. Local timings are short
fallback checks, not throughput claims. The faster network can still favor
partial reads for these large, sparse pages; it uses the same automatic policy.
The local INT64 control also has five pairs: its initial second-query difference
was about 1.5 ms. With both additional pairs retained, second-query medians are
20.644 and 20.636 ms, and first-query medians differ by about 1 ms. Each query
reads the same 63,586,144 bytes in six range GETs in both builds.

Two avoidable costs found during development were fixed before this comparison:

- Large nullable uncompressed pages can require another read for null information.
  Including that dependent round in preflight prevents a wasted probe followed
  by fallback. The retained 131K-row-page control checks this case.
  Its first-query medians are 4.152 and 4.150 seconds.
- A required Zstd page smaller than its PLAIN values cannot contain an all-raw
  frame. Rejecting it from metadata avoids useless probes of compressed columns
  in the retained 416-column grouped query.

## Method and limits

The base is `7b10a504230a7bb683b0478e4f92b2d9612b67f8`, preserved before this series.
Both builds use Rust 1.98.1, the same lockfile and release flags, eight reader
logical CPUs and an 8 GiB process memory limit. Partial reads and initialization
profiling use their public defaults. The main proxy adds 200 ms +/-20 ms jitter
and paces a shared 150 Mbit/s. The fast control uses 2 ms +/-1 ms and 2 Gbit/s.

Each case starts with three alternating pairs in fresh processes, after separate
exact-result validation. OS and MinIO caches are reused. Timing runs disable
request tracing. Initialization and the second query are reported separately.
The grouped cases and local INT64 control use the five pairs described above;
none of the initial samples were discarded.
CPU and peak RSS cover the whole process. README-case HTTP counters cover the
whole session; numeric-case counters cover the first query's Parquet reads.

These are manual regression checks on one machine and a controlled HTTP proxy.
They do not establish performance on every object store. The numeric tests also
compare floating-point bits, including NaN payloads and signed zero. The full
library and reader suite passed 579 tests, with six pre-existing ignores; strict
Clippy and the four focused Python tests passed. No benchmark CI job was added.
