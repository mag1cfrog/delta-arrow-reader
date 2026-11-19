# STRING cast attribution before changes (#1385)

The isolated DATE shape exceeds the completion line in both ANSI modes. Its
Spark completion overhead is 365.295 ms with ANSI off and 374.169 ms with ANSI on.
The isolated DECIMAL shape is below the line in both modes. This supports the
planned DATE change; baseline evidence does not satisfy the DECIMAL change gate.

PR 1 changes only the measurement harness and evidence. Product source is pinned
at `176a5761abe19eeb6a2058c9008d97ce1647b7ef`; the
[protocol](casts-1385-protocol.md) and harness were committed as
`03313bab7739e0945be5d2e872ad52a7591b8483` before the one authorized timing run.
The exact committed-source release rebuild matched the successful preflight
binary byte-for-byte. This report contains before measurements only; no cast
implementation or after measurement is included.

## Before query completion

Each median is the median of three process medians, each based on three retained
iterations after a separate warmup. Minimum, maximum and IQR describe the three
process medians. Completion includes planning through full stream drain. A Spark
row is above the line only if it is strictly >1.25 times native AND >50 ms slower.
Ratios and decisions use unrounded nanoseconds.

| Shape | Path | Median ms | Min ms | Max ms | IQR ms | Spark/native | Difference ms | Above line |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Combined | Native | 688.293 | 679.723 | 690.011 | 5.144 | - | - | - |
| Combined | Spark ANSI off | 1035.473 | 1027.769 | 1041.012 | 6.621 | 1.504x | +347.180 | yes |
| Combined | Spark ANSI on | 1035.847 | 1033.328 | 1037.487 | 2.080 | 1.505x | +347.554 | yes |
| DATE only | Native | 180.170 | 178.855 | 180.867 | 1.006 | - | - | - |
| DATE only | Spark ANSI off | 545.464 | 544.968 | 561.804 | 8.418 | 3.028x | +365.295 | yes |
| DATE only | Spark ANSI on | 554.339 | 552.363 | 554.876 | 1.256 | 3.077x | +374.169 | yes |
| DECIMAL only | Native | 533.725 | 530.746 | 540.535 | 4.895 | - | - | - |
| DECIMAL only | Spark ANSI off | 514.605 | 504.382 | 516.339 | 5.978 | 0.964x | -19.121 | no |
| DECIMAL only | Spark ANSI on | 510.977 | 509.756 | 511.609 | 0.927 | 0.957x | -22.749 | no |

The same-against-same noise floor is the absolute difference between the
medians of the native A and B process arms:

| Shape | Native A ms | Native B ms | Noise floor ms |
| --- | ---: | ---: | ---: |
| Combined | 685.647 | 684.830 | 0.817 |
| DATE only | 179.529 | 179.308 | 0.221 |
| DECIMAL only | 538.313 | 541.468 | 3.155 |

The combined shape also exceeds the completion line in both ANSI modes.
The DATE result identifies the measured isolated excess, but its overhead cannot
be subtracted from or added to the combined result. Isolations project different
source and output columns, and the common view still performs native
source-to-text casts during execution. This is query-level attribution on one
SF1 snapshot, not an exact division of the old combined 440 ms gap or a kernel
profile. No PR 1 profiles were selected.

These are the matched current before numbers for later #1385 stages. The
[historical #1380 report](execution-1380.md) recorded combined Spark medians of
1,129.3 ms with ANSI off and 1,131.4 ms with ANSI on. PR 1 makes no product
change, so the lower current figures do not demonstrate an optimization.
Native A/B noise describes repeatability within this run, not variability
between measurement campaigns. The historical difference was not investigated
or retimed; later changes must use this new matched before baseline.

## Correctness and source I/O

All nine untimed exports returned 6,001,215 rows. All six full native/Spark
comparisons agreed on every decoded value, NULL, multiplicity and executed SQL
primitive type, using the original normalization rules. Keys are Int64
`l_orderkey` and Int32 `l_linenumber`; cast outputs are Date32 `ship_date` and/or
Decimal128(15,2) `price`. Both keys remain in every shape.

Every native and Spark configuration read the same six Parquet objects for its
shape, with the following exact delivered byte totals:

| Shape | Source files | Delivered bytes in every configuration |
| --- | ---: | ---: |
| Combined | 6 | 57,151,761 |
| DATE only | 6 | 25,585,934 |
| DECIMAL only | 6 | 57,127,165 |

All 135 retained timing iterations matched their exact preflight row count,
source path set and byte totals. Source counters were final and every recorded
range-byte sum equaled the direct received-byte counter. Every timed plan
returned two output partitions. The combined preflight also matches the original
#1380 combined receipt on rows, SQL types, source files and bytes in all three
configurations. The retained fixture checksums passed without regeneration.

## Execution and memory context

Planning and execution-only completion are separate observed medians; their
medians must not be added to reconstruct query completion. Process peak RSS
covers setup, warmup and all three iterations of that one query/configuration.
Pool reservation peaks are zero for all rows; this does not mean the queries
allocate no memory. The pool does not cover all reader and kernel allocations.

| Shape | Path | Planning ms | Execution completion ms | Process peak RSS MiB |
| --- | --- | ---: | ---: | ---: |
| Combined | Native | 1.393 | 686.728 | 102.45 |
| Combined | Spark ANSI off | 2.828 | 1032.729 | 113.03 |
| Combined | Spark ANSI on | 2.919 | 1032.351 | 114.40 |
| DATE only | Native | 1.452 | 178.747 | 92.16 |
| DATE only | Spark ANSI off | 2.653 | 542.790 | 101.45 |
| DATE only | Spark ANSI on | 2.221 | 552.153 | 101.18 |
| DECIMAL only | Native | 1.473 | 531.658 | 103.41 |
| DECIMAL only | Spark ANSI off | 2.508 | 512.233 | 109.39 |
| DECIMAL only | Spark ANSI on | 2.432 | 508.544 | 108.98 |

There are no pool-threshold cases. The
[results CSV](casts-1385-results.csv) retains all 153 metric rows, including
both first-batch intervals, setup RSS, pool peaks, source GET/range/task counts,
output partitions and each metric's spread. The
[noise CSV](casts-1385-noise.csv) retains all 51 same-against-same metric rows.

## Plans, method and next decision

The [plan archive](casts-1385-plans.tar.gz) contains nine raw physical plans
before the untimed export sort and six native-to-Spark diffs. Spark uses
`spark_date` and/or `spark_cast_string_to_decimal` with its metadata wrappers
and an additional alias projection. The native reference uses Arrow casts over
matching source-to-text casts; all plans retain `DeltaScanExec` with two partitions.

The run used Rust 1.96.0 release with debug information, two runtime workers,
batch size 8,192, CPU affinity `2,3,4,5`, and the unchanged #1380 session setup
and retained SF1 fixture. It ran on the Ryzen 7 8845HS Linux worker with the
recorded `powersave` governors. All 18 native noise processes preceded the 27
main processes. There were 45 separate warmups, 135 retained iterations, no
concurrency cases, no diagnostic profiles and no repeated schedule.

The next product slice is the one reviewed DATE change. DECIMAL
`completion_flag` is false in both baseline ANSI modes, so PR 3 is not justified
by its isolated before result. It becomes eligible only if the combined shape
remains above the completion line after the DATE change. Pool findings do not
authorize that decision. Later stages must retain these baseline assets and
label carried measurements by their actual source stage.

Source inspection of the excluded
[STRING-to-TIMESTAMP path](../../vendor/sail/crates/sail-function/src/scalar/datetime/spark_timestamp.rs)
and [parser construction](../../vendor/sail/crates/sail-sql-analyzer/src/parser.rs)
shows the same pattern: the row path calls `parse_simple!`, which constructs its
Chumsky parser at each call. Its source was unchanged, and it was not timed.

## Evidence and validation

- [Frozen queries](../casts/queries.json), [schedule](../casts/schedule.json),
  [bound receipt](../casts/preflight-receipt.json), and
  [full compressed preflight](casts-1385-preflight.json.gz).
- [Compressed raw samples](casts-1385-samples.jsonl.gz),
  [metadata and committed-source build receipt](casts-1385-metadata.json),
  [summary flags](casts-1385-summary.json), and
  [validation and raw audit](casts-1385-validation.json).

Fmt and strict root-package all-target Clippy passed. The full Spark workspace
passed 3,162 tests with zero failures and three ignored tests. All 3,165 named
outcomes across 229 result groups match the accepted #1380 inventory. Tests and
preflight were not repeated after the committed-source rebuild because its
binary matched exactly. All 16 original #1380 query/schedule/receipt/report
assets and all eight frozen #1385 files remain byte-identical. The baseline
binary is preserved outside Git as a read-only independent copy; metadata
records both its identity and the distinct original cache executable path.
