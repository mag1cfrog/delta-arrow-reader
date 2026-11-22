# Work remaining after pending RANGE search reuse (#1099)

At 4,096 rows in one-row batches, Spark now makes 4,096 upper-bound
comparisons before end of input. Native makes 8,390,656 in the same
probe. Both Spark ANSI modes still copy 8,390,655 row equivalents
into each of two buffers and materialize 8,390,656 argument array
rows. Native has the same copy and argument counts.

- `PartitionSearcher::update_partition_batch` maintains a partition
  window buffer and the executor input buffer. It appends to the
  partition through `PartitionBatchState::extend`. Each buffer uses
  `concat_batches` for each subsequent batch, copying its accumulated
  prefix along with the new rows.
- `evaluate_pending_frames` in the Spark evaluator and
  `AggregateWindowExpr::get_result_column` on the native path both
  call `evaluate_args` on the full buffered prefix. For `COUNT(*)`,
  each call evaluates one INT64 literal and expands it to an array
  the length of that prefix. The array row count measures materialized
  entries across calls.

## Counts before EOF

The copy counts apply **per buffer**. All three configurations agreed
on every copy and argument count below. The first batch starts each
buffer, and each append expands it. If b is the batch size and K is
the number of batches, argument array rows total b * K * (K + 1) / 2.
Each buffer copies that total minus b row equivalents.

| Input rows | Batch rows | Copy calls per buffer | Copied rows per buffer | Argument calls | Argument array rows |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 1 | 1,023 | 524,799 | 1,024 | 524,800 |
| 1,024 | 32 | 31 | 16,864 | 32 | 16,896 |
| 1,024 | 1,024 | 0 | 0 | 1 | 1,024 |
| 2,048 | 1 | 2,047 | 2,098,175 | 2,048 | 2,098,176 |
| 2,048 | 32 | 63 | 66,528 | 64 | 66,560 |
| 2,048 | 1,024 | 1 | 2,048 | 2 | 3,072 |
| 4,096 | 1 | 4,095 | 8,390,655 | 4,096 | 8,390,656 |
| 4,096 | 32 | 127 | 264,160 | 128 | 264,192 |
| 4,096 | 1,024 | 3 | 9,216 | 4 | 10,240 |

Constant peers remain buffered until the frame completes. The five-row
progressive control still emits the four completed peers before input
EOF. All 63 paired traces agreed. The 42 Spark traces matched the
independent Spark capture across three batch sizes in both ANSI modes.
The full [paired count records](pending-range-1099-remaining-samples.jsonl.gz)
follow the [frozen protocol](pending-range-1099-remaining-protocol.md).

## Rebased verification

The merged product tree matches the release tested at `69731862`.
Its five normalized native instruction bodies, state sizes and field
offsets match staging at `737b7122`. The release also matched three
million-row native results. The [comparison and build identities](pending-range-1099-remaining-rebase.json)
record these checks. Linux formatting and root Clippy passed, and
3,294 workspace tests passed with 3 ignored.

The timed isolation campaign recorded 492.518 to 13.316 ms in Spark
ANSI off and 491.883 to 13.368 ms in ANSI on for the 4,096-row case.
The native cumulative control had a 4.671 ms paired increase. Its
native instructions and layout match. The maintainer classified this
as a build-layout shift. [Timing report and raw samples](pending-range-1099-isolation.md)
[Maintainer decision](https://github.com/mag1cfrog/delta-arrow-reader/issues/1099#issuecomment-7085154960)
