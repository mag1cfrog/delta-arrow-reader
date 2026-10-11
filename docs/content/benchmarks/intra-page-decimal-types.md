# Fixed-length decimal partial reads

[Issue #451](https://github.com/mag1cfrog/delta-arrow-reader/issues/451) extends
partial reads to PLAIN FIXED_LEN_BYTE_ARRAY pages. The reader uses the width in
the Parquet schema; the normal decoder still handles signed decimal values,
precision, scale and existing type conversions.

The base/candidate comparison uses `304acea` and the final change, with public
defaults. Times are medians of the first fully consumed query after initialization,
including planning. The [samples](intra-page-decimal-types-samples.csv) retain
initialization, second queries, I/O and process resources for every validation
and timing run. The `campaign` column separates the earlier implementation, the
final comparison and the A/A control. [Build and fixture identities](intra-page-decimal-types-results.json)
identify each measured build; full observations and reproduction scripts remain at
the recorded artifact root.

## Added decimal cases

The independent fixture contains 1,048,576 rows in two Parquet files, totaling
170,941,918 bytes. Each query projects eight decimal columns, alternating required
and nullable, plus an INT64 row ID. A separate predicate selects 129 rows.
Signed values are deterministic and fit the declared precision. Arrow references
come directly from the generated input.

Pages are uncompressed PLAIN V1, with offset indexes, one row group per file,
a 4 MiB page target and a 524,288-row limit. Generate the fixture with
`numeric_pages.py generate --decimals --output PATH`. The original numeric fixture
still reproduces exactly, including its Parquet, Delta log and reference hashes.

| Decimal | File width | Base query, s | Candidate query, s | Base query MB | Candidate query MB |
| --- | ---: | ---: | ---: | ---: | ---: |
| decimal(20, 4) | 9 bytes | 4.341 | 1.605 | 69.85 | 10.54 |
| decimal(38, 10) | 16 bytes | 6.675 | 1.652 | 113.92 | 10.98 |

Query bytes count Parquet bodies received during the first query. Partial reads
use more requests to transfer fewer bytes; the sample file records both.

## Retained README cases

Both builds read the original TPC-H-derived SF10 snapshots, queries and reference
results. All four cases use deletion vectors. No table layout or query changed.

| Case | Base query, s | Candidate query, s | Time change |
| --- | ---: | ---: | ---: |
| 416 columns, grouped matches | 2.030 | 2.029 | -0.1% |
| 416 columns, spread-out matches | 6.441 | 6.450 | +0.1% |
| 90 columns, grouped matches | 2.154 | 2.119 | -1.6% |
| 90 columns, spread-out matches | 10.224 | 10.217 | -0.1% |

The initial implementation showed a roughly 40 ms second-query slowdown in the
416-column grouped case. After the probe fix, the final comparison did not
repeat it. All initial samples are retained separately; their planned extra
pairs were superseded by the comparison of the final build.

## Fallback and fast-network controls

Snappy and Zstd fixtures re-encode the same decimal input. The Zstd fixture uses
large raw blocks; the unit tests separately cover fallback for compressed blocks.

| Control | Base query, s | Candidate query, s |
| --- | ---: | ---: |
| Dense, precision 20 | 3.974 | 3.974 |
| Dense, precision 38 | 6.302 | 6.267 |
| Snappy, precision 20 | 3.905 | 3.907 |
| Snappy, precision 38 | 6.291 | 6.289 |
| Large raw Zstd pages, precision 20 | 3.741 | 3.741 |
| Large raw Zstd pages, precision 38 | 6.088 | 6.086 |
| Local, precision 20 | 0.026 | 0.024 |
| Local, precision 38 | 0.037 | 0.038 |
| 1 ms, 1 Gbit/s, precision 20 | 1.103 | 0.343 |
| 1 ms, 1 Gbit/s, precision 38 | 1.813 | 0.326 |
| Raw Zstd, 1 ms, 1 Gbit/s, precision 20 | 1.015 | 1.015 |
| Raw Zstd, 1 ms, 1 Gbit/s, precision 38 | 1.778 | 1.778 |

Local, dense, Snappy and these large Zstd pages retain ordinary I/O. Local times
are short fallback checks, not throughput claims. The fast network still favors
partial reads for sparse uncompressed pages under the same automatic policy.

The Zstd control caught a regression before the final comparison. The initial
cost estimate charged one extra header round even when a page required many
dependent reads. It probed several blocks and then fell back. Precision-20
queries grew from 3.742 to 5.036 seconds; precision-38 queries grew from 6.090
to 8.492 seconds. Preflight now charges the minimum dependent rounds implied
by page size, and rejects plans that already exceed the probe limit. The final
Zstd controls above verify that this avoids wasted probes. A focused test also
preserves profitable raw-block partial reads under low latency.

## Follow-up checks

Six cases include two declared extra pairs: 90-column spread-out, both large
Zstd decimals, precision-20 Snappy, precision-38 dense and precision-38 local.
Their tables use all five pairs. The extra runs investigated higher peak RSS or
consistent small timing differences; no earlier sample was replaced.

Precision-38 dense queries read the same 109,725,520 bytes in six range GETs.
The candidate's first query was about 35 ms faster and its second about 35 ms
slower, leaving the combined times nearly unchanged. Local precision-38 first
queries differ by about 1.1 ms; second queries and whole-process CPU time match
closely. Snappy precision-20 medians differ by about 2 ms. These controls keep
the ordinary read path.

Peak RSS needed a separate check. The A/A control ran the identical preserved
base executable on both sides, with three pairs per case. The sample file labels
these runs `same-binary-control`, with builds `baseline-a` and `baseline-b`.
They are separate from the base/candidate comparison.

| Case | Base peak RSS median, MiB | Candidate peak RSS median, MiB | Same-binary A/A range, MiB |
| --- | ---: | ---: | ---: |
| 90 columns, spread-out | 452.5 | 488.9 | 440.1-500.6 |
| Large Zstd, precision 20 | 181.0 | 201.5 | 161.8-213.6 |

The higher candidate RSS medians remain visible above. The same program's RSS
range is wider than either observed median difference. The A/A 90-column
second-query medians differ by 80 ms with identical code.
These ranges describe observed variation, not confidence
intervals or permission to accept a regression.

The 90-column table has no fixed-length byte columns. The decimal fallback
controls have identical query bytes and requests, and review found no larger
retained buffers in the new preflight. CPU totals remain comparable. Together,
these checks did not identify a repeatable material regression. They do not
establish exact timing or memory equivalence.

## Method and limits

Each case starts with three alternating pairs in fresh processes, after separate
exact-result validation. Table initialization is outside the primary query time;
planning and complete result consumption are inside it. Second queries are
recorded separately. Timing runs disable diagnostic tracing. The main proxy
paces a shared 150 Mbit/s with 200 ms +/-20 ms latency; the fast control uses
1 ms and 1 Gbit/s. Both builds use Rust 1.98.1, the same lockfile and release
settings, eight reader logical CPUs and an 8 GiB process memory limit.

OS and MinIO caches are reused. HTTP counters cover initialization and both
queries; decimal-case Parquet counters cover the first query. CPU and peak RSS
cover the whole process. One initial MinIO bucket-creation attempt returned 503
before any measurement; its failure record is retained. Subsequent starts use
a bounded preparation-only retry.

These are manual regression checks on one machine and a controlled S3 proxy.
They do not update the published five-reader comparison or establish native-S3
performance. Temporary MinIO copies and services are removed after testing.
The extension keeps the existing page, encoding, compression, checksum and
resource limits. It adds no public Delta binary mapping or variable-length reads.
