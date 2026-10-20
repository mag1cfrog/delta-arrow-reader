# Delta-length partial-read experiment

This isolated [#452](https://github.com/mag1cfrog/delta-arrow-reader/issues/452)
experiment tests whether reading encoded value lengths first can avoid enough
string/binary payload to make a complete Delta query faster. Production
execution, public defaults, CI workflows and published README results are
unchanged.

## Decision

Proceed to [#453](https://github.com/mag1cfrog/delta-arrow-reader/issues/453)
after PR #475 is reviewed and merged. Start with flat, uncompressed V1
DELTA_LENGTH_BYTE_ARRAY pages. The experiment shows repeatable query and I/O
benefits, with a known cost when discovery finds that selected values occupy
most of a page. Keep that losing control in production acceptance.

Retain the current 5% modeled fallback allowance. It charges the observed
decision response plus the modeled extra cost of discovery followed by a full
read. It does not promise zero overhead or a 5% wall-time limit. The controlled
slow-network losing case costs 650-666 ms extra, or 3.74%-3.83%, in every pair;
the model predicts 656-658 ms. This is understood exploration cost, not an
unexplained estimator regression.

There is no universal 1 ms estimator target. The goal is complete-query benefit
with bounded discovery work. The current estimator runs 1,024 paired trials
from the measured request-duration distribution, uses up to four reusable
workers with one shared computation permit, and caches results within each
immutable profile. Runtime AVX-512 selection has a scalar fallback. Time spent
waiting for or computing a decision counts against its query budget.

A 4% allowance avoided the controlled losing probes but discarded useful reads
on smaller pages, costing 5.839-5.849 seconds in all three pairs. Lookahead and
response-retention alternatives did not solve the fallback problem without
other losses. Further simulator micro-optimization did not give a repeatable
query benefit. The archived history records those alternatives and their
failures; they are not part of the candidate.

## Query results

These are medians of three alternating pairs, excluding initialization and
including scan planning, metadata, predicates, real DVs, discovery, payload
reads and complete decoding. Each invocation has a fresh client and automatic
calibration. There is no warmup query or hidden repeated execution.

| Network | Layout | Ordinary | Experimental |
| --- | --- | ---: | ---: |
| 200 ms +/-20 ms, paced 150 Mbit/s | Long values | 30.118 s | 5.290 s |
| Same controlled network | Selected long values carry most bytes | 17.374 s | 18.034 s |
| Same controlled network | Mixed pages, losing pages first | 31.532 s | 20.775 s |
| Same controlled network | Alternating useful/losing pages | 31.507 s | 19.802 s |
| Real S3, us-west-2 | Long values | 10.314 s | 1.560 s |
| Real S3, us-west-2 | Selected long values carry most bytes | 6.534 s | 6.284 s |

All three S3 long-value pairs improve by 82.31%-85.85%; their 7.256-9.003-second
savings exceed the larger within-mode range of 1.671 seconds. Reader data bytes
fall from 499,312,716 to 1,738,727, with 105 partial pages and requests increasing
from 113 to 353. The four cost decisions together take 7.47-7.79 ms, already
included in query time.

The S3 losing control still performs 128 probes and then reads every complete
page, reusing each prefix. Data bytes remain 258,186,888 and requests increase
from 148 to 276. Paired query changes are -12.82%, -5.84% and +2.85%. Its lower
median establishes neither a failed-probe speedup nor a regression bound.
Decision responses total 4.13-5.49 ms. Controlled-network measurements isolate
the consistent 650-666 ms discovery penalty more clearly.

The final controlled campaign has 48 timings and 16 independent value/null
checks across four layouts and two networks. Its 1 ms/10,000 Mbit/s control is
a subsecond diagnostic: admission varies near the budget boundary, so there is
no stable fast-network gain claim. The S3 campaign adds 12 timings and eight
exact checks on two existing tables in one region. All 84 profiles complete;
no timing pair in these two campaigns is excluded or replaced. S3 initialization
ranges from 2.943 to 4.113 seconds and is reported separately.

Both campaigns use eight logical CPUs, 8 GiB memory, zero swap and 32 shared
request permits. Reader byte counters exclude metadata, calibration and
protocol overhead. Proxy captures additionally cover query metadata and DVs.
CPU time and peak RSS are process-wide, including initialization and, for
validation runs, IPC export. IPC export and exact comparison are outside the
query timer. Full records preserve these separate measurement boundaries.

This compares two paths in one experimental executable. The prototype uses a
different client and request limit from production. These results do not
establish production performance, performance in other S3 regions, or new
README timings.

## Supported layout and safety limits

The initial scope is flat required/nullable BYTE_ARRAY containing UTF-8 or
binary values, V1 DELTA_LENGTH_BYTE_ARRAY encoding, no compression, an offset
index and no page checksum. Nested columns, V2 pages, other encodings and Zstd
remain ordinary reads. The Zstd raw-block investigation is retained in the
earlier evidence; its sequential discovery cost did not justify integration.

Parquet 58.4.0 exposes `DeltaBitPackDecoder<Int32Type>` and its consumed-byte
offset through the optional `experimental` feature. No fork or dependency patch
is needed. This feature also enables `variant_experimental` and three
`parquet-variant*` packages. It is confined to the isolated fixture workspace;
production integration must account for that unstable API and dependency cost.

Before invoking the dependency, the parser checks non-null/value counts,
encoded block structure, lengths, payload bounds and progress. It rejects
negative lengths, truncation and inconsistent payloads. Limits are 8 MiB per
page, 1,048,576 rows, 1 MiB structural discovery, nine chunks including any
cached prefix, a 32,768-value block and 8,192 selected ranges. Existing prefix
bytes count toward every bound. All null and length bytes remain intact; only
unselected payload bytes are zeroed in buffers used with the exact row selection.
No table is rewritten.

Candidates must be 1-8 MiB, average at least 64 stored bytes per row, and select
at most one-eighth of the rows. Discovery starts with 8 KiB. Sparse rows alone
do not imply sparse payload. The whole requested batch, including ordinary
columns and dependent requests, must fit the modeled fallback budget. Each
extension is charged before I/O. Partial reads then require at least 25% total
byte savings and more than 10% modeled remaining-read savings. Unsupported or
unaffordable reads reuse any fetched prefix and read the remaining bytes.
Malformed supported input returns an error.

## Reproduce and inspect the evidence

The [final evidence guide](https://github.com/mag1cfrog/delta-arrow-reader/releases/download/selective-read-benchmarks-2026-10-07/delta-length-452-final-REPRODUCE.md)
explains how to download and verify the supplement and replay the final timing
and I/O summaries offline. It includes measured sources, plans, raw records,
resource receipts, failed attempts, selected diagnostic campaigns and the full
experiment history. The earlier `delta-length-452-d9c0d6a.tar.gz` asset remains
available in the same release for the initial decoder and layout controls.

Bulk fixtures, independent oracle arrays, exported result arrays, executables
and credentials are excluded. Successful exact-check receipts and hashes are
retained; offline summary replay does not repeat value comparisons. Regenerate
the deterministic fixtures below to recheck values. Private bucket identifiers
are redacted in public copies, with original and published file hashes recorded.
The original local evidence remains unchanged.

The measured runtime predates the final crate-path update and relocation of the
experimental cost planner into benchmark code. Function bodies and reader logic
are unchanged. The supplement retains that measured source separately from the
reviewed source, with a relocation check. After synchronization with main, 11
decoder/inspection tests, 47 query/planner/calibration/estimator tests, Clippy,
rustfmt, the Python probe test and eight local exact array/view checks pass.
Those local checks exercise both successful partial reads and post-probe
fallback; their execution times are not performance evidence.

No shared production code changes in the final diff, so #452 does not require a
new four-query README regression run. That gate remains mandatory in #453.

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
mask removes physical row positions before selection. This local proof does not
load Delta deletion vectors or execute predicates; the network driver below does.

The writer emits uncompressed V1 DELTA_LENGTH_BYTE_ARRAY pages with offset
indexes. The verifier checks the actual page type, encoding and compression.
Each complete and partially cleared file is decoded into ordinary arrays and
the Utf8View/BinaryView array types used by DataFusion. Expected values come from
the original input batch, before Parquet encoding. The program checks values,
nulls and row IDs, then records the fixture, source and lockfile hashes.

## Run the network experiment

Use the pinned PyArrow environment and MinIO/proxy setup in
[EVIDENCE.md](EVIDENCE.md). Run these commands from `benches/selective_read`.
`experiment_root` must be an absolute directory outside the checkout;
`python` must resolve to the pinned PyArrow environment.

```sh
export CARGO_TARGET_DIR="$experiment_root/target"
export RUSTFLAGS='-C target-cpu=x86-64'
cargo +1.98.1 build --release --locked --features delta-length-network \
  --bin selective-read-fixtures --bin delta-length-pages --bin delta-length-query
"$experiment_root/target/release/selective-read-fixtures" length-pages --case long \
  --binary "$experiment_root/target/release/delta-length-pages" \
  --output "$experiment_root/long"
```

The cases are `long`, `short`, `selected-long`, `selected-long-batch`,
`length-heavy`, `mixed-bad-first` and `mixed-alternating`. Each
contains two unpartitioned Delta files, a real DV per file, eight required or
nullable UTF-8/binary payload columns, and `row_id`, `grouped` and `scattered`
columns. `grouped = 1` selects eight adjacent rows per 2,048; `scattered = 1`
selects one per 997. `dense` reads all live rows. DVs delete every 103rd physical
row. Expected values are filtered from the generated Arrow input before Parquet
encoding and saved as compressed Arrow IPC. The Python checker independently
compares the reader's export with these references.

The generator uses SplitMix64 and parquet-rs 58.4.0. Its value bytes and page
layouts differ from the retired Python writer; compare candidates against the
same fixture directory. The manifest records the writer, distributions, page
layout, objects and reference checksums. Existing fixtures remain readable.
`selected-long-batch` keeps the selected-long value-length rule but increases
rows per file from 8,192 to 65,536 and rows per group from 4,096 to 32,768.
This lets discovery pass the initial cost gate and tests the later fallback.
The mixed cases use 65,536 rows per file and an 8 MiB page target, with the
8,192-row limit defining page boundaries. Their value distributions and
verified layouts are described in the feedback experiment below.

Generation also runs `delta-length-pages inspect FILE` against every Parquet
file. The manifest records actual page sizes, row counts, structural byte counts,
encoding, compression and offset-index presence, not just writer settings.
Add `--compression zstd` to `generate` to keep the same values and writer geometry
with Zstd level 3. The inspector then records each page's actual block types,
stored sizes, frame flags and block-header offsets. It fully reads and decodes
the generated pages offline; its byte counts are not measured query transfers.
The compression level is recorded as a writer setting; Parquet metadata
identifies the codec only.

Upload the manifest's objects to a content-addressed path on the dedicated
MinIO server. This uses the existing upload verification without re-reading
whole files when MinIO provides a full-object checksum:

```sh
cargo +1.98.1 run --manifest-path ../../Cargo.toml --locked -p xtask -- selective-read upload-delta-length \
  --fixture "$experiment_root/long" --state "$experiment_root/storage"
```

Use the printed `table_root` URI as `table_uri`, with the paced proxy running:

```sh
cargo +1.98.1 run --manifest-path ../../Cargo.toml --locked -p xtask -- selective-read run-delta-length \
  --python /path/to/oracle-venv/bin/python --harness . \
  --fixture "$experiment_root/long" \
  --state "$experiment_root/storage" --table "$table_uri" --selection scattered \
  --binary "$experiment_root/target/release/delta-length-query" \
  --output "$experiment_root/slow-long-scattered"
```

The runner checks ordinary arrays and view arrays for both modes, then records
three ordinary/experimental timing pairs in AB, BA, AB order. Each invocation
uses a fresh process with eight logical CPUs, an 8 GiB memory cap and at most
32 concurrent data requests. MinIO and OS caches are reused. No query warmup or
repeated execution is hidden inside a sample. Use new output directories for
each run. Input and reference hashes are verified once outside timing.

## Handoff to #453

Use the current policy and the documented failed-discovery tradeoff as the
integration baseline after #475 is reviewed and merged. There is no fixed 1 ms
requirement. The allowance is 5% of modeled ordinary-read cost, superseding the
issue's historical 10% wording. It is not a wall-time guarantee or, by itself, an
accepted measured regression limit.

Start with flat required or nullable BYTE_ARRAY columns representing UTF-8 or
binary values, V1 DELTA_LENGTH_BYTE_ARRAY encoding, no compression, an offset
index and no page checksum. Read physical nullability from the Parquet
descriptor. Other encodings, nested types, V2 pages, checksummed pages and all
Zstd pages keep ordinary reads in this first integration. Zstd remains a
separate research question, not a general impossibility claim.

Use the decoder API and validation bounds described above. The measured
candidate filter is 1-8 MiB pages, at most 1,048,576 rows, at least 64 stored
bytes per row and no more than one-eighth selected rows. It is an eligibility
filter, not a byte-savings estimate. The prototype starts with an 8 KiB prefix;
changing that size or other limits needs another measurement. Check existing
cached bytes before extending the prefix. Reuse production parsing, range
caching and resource accounting where their contracts fit. Do not copy the
experiment's transport or calibration module into the production reader.

Production cost selection must cover the whole requested batch, including
ordinary columns. Price request capacity and each dependent discovery round
using the existing production profile and shared concurrency. Reject an
unaffordable initial probe before I/O; charge subsequent discovery before
another read. The experiment requires full discovery-plus-fallback cost within
105% of ordinary reads, at least 25% total byte savings and more than 10% modeled
remaining-read savings. Preserve production's accounting of both modeled and
observed work; a modeled budget is not a wall-time guarantee. Variable lengths
must not use the fixed-width planner's uniform row-offset estimate.
Before falling back, evaluate bounded merges with the same cost formula used
to approve payload reads.

Keep missing-profile and explicit opt-out paths on ordinary reads. When a
later probe discovers unsupported layout, exhausted budget or insufficient
savings, reuse downloaded prefixes and fetch only missing bytes. Preserve all
null and length control bytes. Treat malformed supported input as an error,
not a successful partial result. Existing shared request and memory limits
must also cover concurrent length arrays, caches and full-size reconstruction
buffers; per-page bounds alone do not bound aggregate memory.

Before enabling the new path, repeat the beneficial and losing cases with the
actual production client, calibration and scheduling. Require actual partial
I/O in the positive cases and record every preflight rejection or later
fallback. The slow selected-long case now permits a measured full fallback at
concurrency 32; this does not establish its cost under production's larger
request allowance. Retain exact
value/null comparisons, real DVs, ordinary arrays, DataFusion view arrays,
malformed-input checks and failure/cancellation coverage.

Apply #449's regression gate: three alternating pairs for the new cases and
all four retained README DV queries against the PR base, with initialization
separate from the first complete query. Investigate a median regression over
5% or a consistent smaller regression; unresolved or inconclusive results do
not pass. The parent's final eight-case comparison remains separate. No new
production support or default is enabled by #452.
