# Experimental partial-page reads

This records the opt-in reader from [#420](https://github.com/mag1cfrog/delta-arrow-reader/issues/420)
and transport-aware planning from [#421](https://github.com/mag1cfrog/delta-arrow-reader/issues/421).
The option is disabled by default. [#419](https://github.com/mag1cfrog/delta-arrow-reader/issues/419)
tracks performance validation and the decision about defaults.

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
all returned values and nulls with ordinary decoding. Local scans without
transport evidence must retain ordinary I/O. Decoder tests inject deterministic
transport estimates and require fewer received bytes only when partial reads
are economical. Checks also cover dense and empty
selections, hidden predicate columns, empty projections, limits after DV filtering,
and a dense row group followed by a sparse one. DataFusion splits the file into
multiple tasks to check original row coordinates after repartitioning.

Additional format checks cover a value split across Zstd raw blocks, unsupported
codecs, dictionary encoding, V2 pages, checksums, missing indexes, corrupt levels,
and truncated input. Multi-frame Zstd pages and padded null levels are checked
against Parquet's standard decoder before verifying fallback. Request failures
and cancellation must release pending reads and concurrency permits. These are
correctness checks, not timings for the published benchmark. Planner checks cover
bandwidth, latency, the decision margin, dependent probes and shared capacity.
They run in the existing test jobs.

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

The planner reuses the passive latency/throughput estimator from ordinary range
reads. It scores each dependent round as bytes plus request waves multiplied by
the bandwidth-delay cost. Partial reads, including probes already issued, must
beat the ordinary plan by more than 10%. Missing transport evidence, selections
covering at least half a row group, uncertain savings or unavailable capacity
use ordinary reads. Probes themselves also supply transport observations; there
are no calibration requests. Bytes belonging to output columns that need complete
reads are included before probing, so known costs can rule out the optimization
without extra I/O.

Each round reserves currently free capacity from a process-wide ceiling of 512
planned range reads, shared with ordinary range plans. Ordinary plans retain
their per-plan limit of 10. A round uses its reserved concurrency for both scoring
and execution, then releases it before parsing or requesting the next round. If gap merging
reduces the request count, excess reserved capacity is released before I/O.
Partial plans do not queue for permits. Dropping a future or encountering a
request error drops outstanding reads, buffers and reservations. Store retry
behavior is unchanged and stays within the reservation.

Each experimental fetch is limited to 4,096 candidate pages, 8,192 selected rows
in one row group, 20 probe rounds and 32,768 requests. A candidate page is at most
8 MiB and 1,048,576 rows. The original requested ranges total at most 128 MiB;
probes, ordinary output pages and selected values together cannot exceed that
original byte count. The planner chooses how much gap filling is worth paying
for, and transport does not merge the chosen ranges again. A later fallback may
still reread complete pages after probes already issued.

The `delta_arrow_reader::diagnostics::intra_page` tracing target reports
eligibility and fallback reasons. Cost decisions include estimated byte-equivalent
costs, planned bytes and requests, probe rounds and reserved concurrency. They
contain no object paths or credentials.

Explicit range-read policies other than `Automatic` suppress the experiment.
The `DeltaKernel` backend ignores it. Files buffered in memory use ordinary
reads without returning to the remote store. The original 16 MiB / 512 prototype
settings are not fixed per-fetch defaults in this implementation.

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
