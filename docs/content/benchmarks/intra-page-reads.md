# Experimental partial-page reads

This is the implementation and reproduction record for [#420](https://github.com/mag1cfrog/delta-arrow-reader/issues/420).
The option is disabled by default. [#419](https://github.com/mag1cfrog/delta-arrow-reader/issues/419)
tracks the later transport model, performance validation and decision about defaults.

## Run the portable checks

From the repository root:

```console
cargo test --locked --lib --test reader intra_page
cargo test --locked --features datafusion --lib --test reader intra_page
```

The tests generate a small Delta table locally. They require no service, Python
environment, downloaded dataset, local Cargo patch or experiment environment
variable. The fixture has three row groups, page boundaries, deterministic
nullable payloads, an all-null column and optional DV deletions. Checks compare
all returned values and nulls with ordinary decoding, and require fewer received
Parquet bytes for the supported sparse scans. Checks also cover dense and empty
selections, hidden predicate columns, empty projections, limits after DV filtering,
and a dense row group followed by a sparse one. DataFusion splits the file into
multiple tasks to check original row coordinates after repartitioning.

Additional format checks cover a value split across Zstd raw blocks, unsupported
codecs, dictionary encoding, V2 pages, checksums, missing indexes, corrupt levels,
and truncated input. Multi-frame Zstd pages and padded null levels are checked
against Parquet's standard decoder before verifying fallback. Request failures
and cancellation must release pending reads and concurrency permits. These are
correctness checks, not timings for the published benchmark. They run in the
existing test jobs.

## Integration and limits

The implementation uses the existing Parquet dependency's public `AsyncFileReader`
and virtual row-number APIs. The predicate records absolute file row numbers.
After filtering, the reader requests only the selected values from supported
non-predicate columns. The standard decoder still applies row selection; the
existing Delta reader applies DV deletions using original row coordinates.
Predicate columns retain complete data because Parquet may cache their decoded
batches.

Eligible pages are flat nullable PLAIN INT64 V1 pages with offset indexes. Data
must be uncompressed or held in a single Zstd frame of raw blocks, without page
or frame checksums. Unsupported pages go through ordinary reads. Malformed supported
structures return errors; the standard decoder handles errors in fallback
formats. These checks cannot detect arbitrary value corruption in files that
have no integrity checks.

Each experimental fetch is limited to 4,096 candidate pages, 8,192 selected rows
in one row group, 20 probe rounds and 32,768 requests. A candidate page is at most
8 MiB and 1,048,576 rows. The original requested ranges total at most 128 MiB.
Probes, ordinary output pages and selected values share a byte budget of the
smaller of 16 MiB or half the originally requested bytes. Reaching a limit uses
ordinary reads, after any probes already issued. A process-wide semaphore caps
experimental range reads at 512; dropping the read future releases its requests
and buffers. Ordinary reads retain their existing concurrency settings.

Explicit range-read policies other than `Automatic` suppress the experiment.
The `DeltaKernel` backend ignores it. These are fixed experimental limits, not a
network cost model. [#421](https://github.com/mag1cfrog/delta-arrow-reader/issues/421)
will compare bounded plans against the ordinary reader's plan before choosing one.

## Original prototype evidence

The [machine-readable record](intra-page-prototype.json) preserves the original
samples, network and resource settings, build identity and source hashes. Those
measurements used a local Parquet prototype on base commit
`2751f062e7bee4fe9866bb7fa23e4314d39714e4`. They have not been rerun with this public-API
implementation.

Both cases used the published TPC-H-derived SF10 scattered tables with DV,
unchanged queries and 894 output rows. The old artifact IDs are retained only
in the JSON for traceability. The proxy used 200 ms latency, +/-20 ms jitter and
150 Mbps shared, progressively delivered HTTP bodies. Each mode ran three times
in fresh reader processes with reused OS/MinIO caches, four physical cores and
8 GiB memory. Timing included table open, planning, filtering, DV handling and
consumption of all output batches; preparation was excluded.

| Scattered DV case | Ordinary median | Prototype median | Median HTTP body bytes | Median requests |
| --- | ---: | ---: | ---: | ---: |
| 416 columns | 11.881 s | 8.488 s | 186,249,508 -> 100,466,536 | 53 -> 13,981 |
| 90 columns | 23.239 s | 13.550 s | 399,702,479 -> 106,012,680 | 68 -> 27,996 |

Body bytes and requests cover all object classes, including Delta logs and DV
files. Headers and TLS were outside the proxy's shaping boundary. These paired
DAR measurements explain why the experiment exists; they do not replace the
published five-reader campaign or establish performance on real S3. The full
workloads remain in the manual validation planned in
[#422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422).
