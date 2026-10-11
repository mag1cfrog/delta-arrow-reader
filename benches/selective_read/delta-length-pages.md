# Delta-length decoder experiment

This is the local correctness step of [#452](https://github.com/mag1cfrog/delta-arrow-reader/issues/452).
It checks whether a reader can discover variable-length values, fetch selected
payload ranges, and leave the normal Parquet decoder in charge of decoding.
It does not change the production reader or establish a performance benefit.

## Run the proof

From `benches/selective_read`, with Rust 1.98.1:

```sh
cargo +1.98.1 test --release --locked --features delta-length-experiment --bin delta-length-pages
cargo +1.98.1 run --release --locked --features delta-length-experiment --bin delta-length-pages -- /absolute/path/to/new-output
```

The output directory must not exist. The program writes deterministic input
Parquet files, copies with unselected value bytes cleared, and `results.json`.
The cleared copies are only valid for the recorded row selection. They must not
be used as ordinary tables: other rows no longer contain their original values.

Five inputs cover short values, long values, a few selected long values carrying
most of the payload, length-heavy pages, and all-null nullable columns. Each has
required and nullable UTF-8 and binary columns, including empty values. Grouped,
scattered and dense selections cross page and row-group boundaries; an empty
selection checks that decoding can return no rows. A deleted-row
mask removes physical row positions before selection; this does not yet test
Delta deletion-vector loading or predicate execution.

The writer emits uncompressed V1 DELTA_LENGTH_BYTE_ARRAY pages with offset
indexes. The verifier checks the actual page type, encoding and compression.
Each complete and partially cleared file is decoded into ordinary arrays and
the Utf8View/BinaryView array types used by DataFusion. Expected values come from
the original input batch, before Parquet encoding. The program checks values,
nulls and row IDs, then records the fixture, source and lockfile hashes.

## What the byte counts mean

The prototype fetches a growing prefix containing null definitions and encoded
lengths. It then fetches selected payload ranges not already in that prefix.
`results.json` retains each page's structural bytes, prefix bytes, selected
payload bytes, discovery rounds and payload request count.

These are page-body counts, not query I/O measurements. The verifier reads full
files to inspect their layout and prepare the cleared copies. Footer reads,
page headers, predicate columns, network latency and decoding costs are outside
these counts. The experiment does not time this local verification.

## Decoder API and bounds

Parquet 58.4.0 exposes `DeltaBitPackDecoder<Int32Type>` and its consumed-byte
offset through the `experimental` feature. The Arrow byte-array decoder's
constructor is private, but this experiment does not need it. Normal Arrow
readers decode the completed buffers. No fork or dependency patch is needed.

The feature also enables `variant_experimental` and three `parquet-variant*`
packages. Here it is an opt-in feature of the isolated fixture workspace; the
production crate and ordinary fixture builds do not enable it. Any production
proposal must account for the extra dependencies and the unstable decoder API.

Before calling the dependency, the parser checks the encoded count against the
page's non-null value count and scans the block structure. It rejects negative
lengths, truncated structures, inconsistent payload sizes and short reads.
It preserves every null and length byte. Only unselected value payloads are
cleared. The current experiment caps each page at 8 MiB and 1,048,576 rows,
structural discovery at 1 MiB and nine fetches, block size at 32,768 values,
and selected ranges at 8,192. Unsupported layouts and exhausted budgets require
ordinary-read fallback; the verifier fails instead of hiding such a case.

## Remaining work for #452

Before making a recommendation for #453, add actual query execution with Delta
deletion vectors and the existing paced network harness. Compare ordinary and
experimental reads in the same executable, charge all discovery and payload
work, and record first-query time separately from initialization. Declare the
noise and losing-case criteria before three alternating pairs at 200 ms latency,
20 ms jitter and a shared 150 Mbit/s limit, with a fast-network control.

Inspect actual Zstd block types before considering raw-block reads. Compressed
blocks need a recorded fallback result. Preserve failures and all observations,
then use the measured limits and losing cases to update #453. The production
reader, published README numbers and original benchmark fixtures stay unchanged
during this experiment.
