# Restricted #1516 baseline on macOS

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The restricted preflight matched 22 cases against native in both
ANSI modes. The two approximate-percentile windows have the largest
paired excess, 183.864 to 188.355 ns per input row. The two aggregate
aliases, ARRAY_INSERT and aggregate TRY_AVG also exceed their floors.
These six cases set the order for attribution. The baseline changes
no product source.

The [committed protocol](../macos_rounds/1516-restricted-baseline.md)
pins staging `e9f5ce76` and the unchanged #1516 restrictions. These use
937,500 rows when the filter drops missing inputs and one million
otherwise. The same restricted batches feed both frontends. The
preflight verifies complete decoded rows, executed Arrow types and
source-row guards. The unrestricted #1560 comparisons keep their
recorded semantic exclusions.

**Apple Silicon, native macOS.** Costs in ns per actual input row
above projection. Two-value columns give ANSI off / on. The gap is
paired within blocks. Within-floor gaps are not resolvable on Apple
Silicon under this schedule.

| Case | Input restriction | Native | Spark | Paired gap | A/A floor | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| APPROX_PERCENTILE (aggregate) | Non-NULL values are 0.5 | 3.604 | 8.379 / 8.374 | +4.784 / +4.800 | 0.082 | slower |
| PERCENTILE_APPROX (aggregate) | Non-NULL values are 0.5 | 3.604 | 8.353 / 8.365 | +4.736 / +4.773 | 0.117 | slower |
| REGR_AVGX (aggregate) | Non-NULL x values are 8 | 5.678 | 5.509 / 5.512 | -0.172 / -0.170 | 0.178 | within floor |
| REGR_AVGY (aggregate) | Non-NULL y values are 0.5 | 5.675 | 5.516 / 5.504 | -0.174 / -0.090 | 0.296 | within floor |
| REGR_R2 (aggregate) | Non-NULL y equals x | 5.806 | 5.510 / 5.499 | -0.186 / -0.307 | 0.173 | faster |
| REGR_SXY (aggregate) | Non-NULL y values are 0.5 | 5.716 | 5.507 / 5.499 | -0.210 / -0.227 | 0.163 | faster |
| TRY_AVG (aggregate) | Non-NULL values are 0.5 | 0.698 | 1.370 / 1.370 | +0.671 / +0.671 | 0.034 | slower |
| ARRAY_APPEND (scalar) | Non-NULL arrays and elements | 21.712 | 21.476 / 21.513 | -0.241 / -0.190 | 0.487 | within floor |
| ARRAY_INSERT (scalar) | Non-NULL arrays and elements | 72.011 | 75.569 / 75.577 | +3.599 / +3.536 | 1.633 | slower |
| ARRAY_PREPEND (scalar) | Non-NULL arrays and elements | 21.734 | 21.600 / 21.512 | -0.064 / -0.163 | 0.718 | within floor |
| CARDINALITY (scalar) | Non-NULL arrays | 44.283 | 44.299 / 44.228 | -0.019 / -0.041 | 1.284 | within floor |
| CONCAT (scalar) | Both strings present | 14.212 | 14.512 / 15.060 | -0.238 / +1.264 | 3.215 | within floor |
| SIZE (scalar) | Non-NULL arrays | 44.426 | 44.150 / 44.287 | -0.287 / -0.054 | 1.355 | within floor |
| EXPM1 (scalar) | Non-NULL values are 0.5 | 2.418 | 2.485 / 2.483 | +0.069 / +0.068 | 0.071 | within floor |
| BASE64 (scalar) | Six-byte strings as binary | 26.996 | 23.735 / 23.818 | -3.377 / -3.137 | 0.740 | faster |
| APPROX_PERCENTILE (window) | Non-NULL values are 0.5 | 497.729 | 681.420 / 681.593 | +186.668 / +183.864 | 14.593 | slower |
| ARRAY_AGG (window) | Non-NULL values | 739.120 | 612.929 / 613.219 | -125.856 / -125.093 | 18.637 | faster |
| COLLECT_LIST (window) | Non-NULL values | 739.238 | 613.194 / 613.667 | -126.395 / -127.460 | 18.178 | faster |
| COLLECT_SET (window) | Non-NULL values | 941.023 | 941.361 / 940.436 | +1.050 / +1.600 | 30.371 | within floor |
| PERCENTILE_APPROX (window) | Non-NULL values are 0.5 | 496.930 | 682.788 / 683.275 | +187.330 / +188.355 | 17.107 | slower |
| REGR_AVGX (window) | Non-NULL x values are 8 | 374.761 | 376.360 / 375.966 | +1.491 / +0.065 | 2.595 | within floor |
| TRY_AVG (window) | Non-NULL values are 0.5 | 264.776 | 218.189 / 218.407 | -45.189 / -46.475 | 3.033 | faster |

## Prior judgment and source review

- REGEXP_REPLACE retains its [accepted Mac judgment](https://github.com/mag1cfrog/delta-arrow-reader/pull/1554).
  The Spark modes were faster than native. The positive native flag
  was a build-layout shift. Linux x86 confirmation after #1551 is
  pending. The maintainer excluded this case from the new run.
- ARRAY_COMPACT still differs on the declared restricted input.
  At id 1 the native NULL removal yields NULL, and Spark yields
  `[4, 3]` in both modes. The executed types agree. Spark reads
  visible child nulls and only compacts child buffers if needed.
  The work depends on the array contents.
  [Kernel](../../vendor/sail/crates/sail-function/src/scalar/array/spark_array_compact.rs)
- ARRAYS_ZIP declares nested fields `arr` and `arr` in Spark.
  Native declares `1` and `2`, and uses different nested
  nullability. Filtering rows cannot align those types. Spark binds
  output names during planning and reads the input lengths, offsets
  and child values during execution.
  [Kernel](../../vendor/sail/crates/sail-function/src/scalar/array/arrays_zip.rs)
- DATE_FORMAT has the fixed Spark pattern `yyyy-MM-dd`. Native
  interprets it as Chrono text, so this statement pair has no useful
  common input. Spark prepares the pattern per batch. Its timestamp
  array formatter parses the same UTC metadata and resolves a zone
  abbreviation per non-NULL row. Those repeated operations are a
  source lead for attribution after the paired gaps. The calendar
  fields depend on the row.
  [Spark formatter](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_date_format.rs)
- BITMAP_AND_AGG has no native #1391 reference. The window
  accumulator starts with a 4 KiB AND identity, processes input
  bytes and clears the suffix beyond a short input. It copies each
  output bitmap. The byte work and suffix depend on the frame.
  [Accumulator](../../vendor/sail/crates/sail-function/src/aggregate/bitmap_and_agg.rs)
- BITMAP_CONSTRUCT_AGG has no native #1391 reference. Each position
  selects a bit in a 4 KiB bitmap and the output exposes a fresh
  bitmap for each frame. Position calculations depend on the input.
  [Accumulator](../../vendor/sail/crates/sail-function/src/aggregate/bitmap_construct_agg.rs)
- BITMAP_OR_AGG has no native #1391 reference. The accumulator
  applies input bytes to a 4 KiB zero-initialized state and copies
  that state into each output. The three bitmap statements have no
  constant SQL argument. Their sliding accumulators do not retract
  and can rebuild state on each moving frame.
  [Accumulator](../../vendor/sail/crates/sail-function/src/aggregate/bitmap_or_agg.rs)

## Baseline decisions

The ARRAY_APPEND ANSI delta is within the A/A floor on its
restricted shape, as are the SIZE and CARDINALITY native gaps.
COLLECT_SET and REGR_AVGX windows are within their floors. REGR_AVGY
and REGR_AVGX aggregates are within their floors. REGR_SXY and
REGR_R2 aggregates are faster than native in both modes.
Aggregate TRY_AVG retains a gap of 0.671 ns/row against 0.034 ns/row.
Its source uses the Spark ordered sum. Attribution must preserve its
floating-point order.

All 990 processes completed, including both native A/A arms in each
block. The session retained 8,910 samples. The preflight includes
all 29 cases. Formatting, strict root-package Clippy and the full
Spark workspace suite passed on the same product and test inputs
at `7f38c538`.

- [Restricted equivalence receipt](functions-1516-macos-restricted-preflight.json)
- [Full paired measurements](functions-1516-macos-restricted-comparisons.json)
- [Compressed raw samples and executable receipt](functions-1516-macos-restricted-samples.jsonl.gz)
