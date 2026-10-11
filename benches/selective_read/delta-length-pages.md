# Delta-length decoder experiment

This isolated experiment for [#452](https://github.com/mag1cfrog/delta-arrow-reader/issues/452)
checks whether discovering lengths and fetching selected string/binary values
is cheaper than reading complete pages. It includes a local decoder proof and
a Delta query driver using the existing network harness. Production execution
and the published README benchmarks are unchanged.

## Decision

Proceed with [#453](https://github.com/mag1cfrog/delta-arrow-reader/issues/453)
for flat, uncompressed V1 DELTA_LENGTH_BYTE_ARRAY string and binary pages.
On the slow network profile, measured-cost reads reduced the long-value
scattered query from 30.095 to 5.316 seconds and the grouped query from 29.799
to 4.539 seconds. Query downloads fell from 499.461 to 1.887 MB and from
493.156 to 2.950 MB, respectively. Both skewed-value controls rejected discovery
before I/O and retained the ordinary byte and request counts.

These results justify production integration, not a production speedup claim.
The prototype uses a different client and a 32-request limit. Fast-network
results are subsecond diagnostics; some skewed runs still probe and fall back.
Keep Zstd on ordinary reads in #453: sequential block discovery alone was
slower than downloading complete columns in the measured layout. The
[integration requirements](#handoff-to-453) below define the remaining gates.

## Evidence

The separate `delta-length-452-d9c0d6a.tar.gz` asset in the
[benchmark evidence release](https://github.com/mag1cfrog/delta-arrow-reader/releases/tag/selective-read-benchmarks-2026-10-07)
retains the source patch, raw samples, initialization measurements, request
traces, resource records, fixture identities and earlier attempts. Its measured
runtime sources match commit `d9c0d6a`. The published README results are a
different campaign and remain unchanged.

```sh
gh release download selective-read-benchmarks-2026-10-07 \
  --repo mag1cfrog/delta-arrow-reader \
  --pattern 'delta-length-452-d9c0d6a.tar.gz*' \
  --pattern 'delta-length-452-REPRODUCE.md'
sha256sum --check delta-length-452-d9c0d6a.tar.gz.sha256
```

The accompanying reproduction guide replays the timing and I/O summaries from
the recorded samples. Bulk fixtures, independent oracle files and exported
result arrays are excluded from this archive; their hashes and successful
comparison records are retained. To recheck actual values, regenerate the
deterministic inputs and run the correctness checks below. Offline summary
replay does not repeat those value comparisons.

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
packages. Parquet's experimental feature is opt-in within the isolated fixture
workspace; the production crate and ordinary fixture builds do not enable it.
The inspector also enables the fixture workspace's existing optional `thrift`
dependency to locate compressed page bodies. Any production proposal must
account for the extra dependencies and the unstable decoder API.

Before calling the dependency, the parser checks the encoded count against the
page's non-null value count and scans the block structure. It rejects negative
lengths, truncated structures, inconsistent payload sizes and short reads.
It preserves every null and length byte. Only unselected value payloads are
cleared. The current experiment caps each page at 8 MiB and 1,048,576 rows,
structural discovery at 1 MiB and nine fetches, block size at 32,768 values,
and selected ranges at 8,192. Unsupported layouts and exhausted budgets require
ordinary-read fallback; the verifier fails instead of hiding such a case.
The caller can supply an already fetched body prefix. The parser checks its size
and examines all of it before requesting more bytes. A supplied prefix counts
as the first discovery chunk, so reuse does not bypass the byte or fetch limits.

## Run the network experiment

Use the pinned PyArrow environment and MinIO/proxy setup in
[EVIDENCE.md](EVIDENCE.md). Run these commands from `benches/selective_read`.
`experiment_root` must be an absolute directory outside the checkout;
`python` must resolve to the pinned PyArrow environment.

```sh
export CARGO_TARGET_DIR="$experiment_root/target"
export RUSTFLAGS='-C target-cpu=x86-64'
cargo +1.98.1 build --release --locked --features delta-length-network \
  --bin delta-length-pages --bin delta-length-query
python delta_length_network.py generate --case long \
  --binary "$experiment_root/target/release/delta-length-pages" \
  --output "$experiment_root/long"
```

The cases are `long`, `short`, `selected-long`, `selected-long-batch` and
`length-heavy`. Each
contains two unpartitioned Delta files, a real DV per file, eight required or
nullable UTF-8/binary payload columns, and `row_id`, `grouped` and `scattered`
columns. `grouped = 1` selects eight adjacent rows per 2,048; `scattered = 1`
selects one per 997. `dense` reads all live rows. DVs delete every 103rd physical
row. The independent expected values are filtered from the Python input before
Parquet encoding and saved as compressed Arrow IPC.
`selected-long-batch` keeps the selected-long value-length rule but increases
rows per file from 8,192 to 65,536 and rows per group from 4,096 to 32,768.
This lets discovery pass the initial cost gate and tests the later fallback.

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
python - "$experiment_root/long" "$experiment_root/storage" <<'PY'
import json
import sys
from pathlib import Path
import storage
fixture, state = map(Path, sys.argv[1:])
manifest = json.loads((fixture / "manifest.json").read_text())
prefix = storage.digest(fixture / "manifest.json")
with storage.exclusive(state):
    storage.verify_server(state)
    for item in manifest["objects"]:
        storage.put_verified(state, prefix + "/" + item["path"],
            fixture / item["path"], item["sha256"], checksum=True)
print(f"s3://{storage.BUCKET}/{prefix}/")
PY
```

Use the printed URI as `table_uri`, with the paced proxy running:

```sh
python delta_length_network.py run --fixture "$experiment_root/long" \
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

## Measurement and decision boundaries

Both modes use the same executable, Parquet decoder, projection and row
selection. Delta Kernel loads snapshot version 0 and the actual DVs. The driver
reads the predicate column, applies the DV, and fully consumes the selected
payload. This measures the experimental transport against ordinary Parquet
reads, not the production Delta Arrow Reader planner or a competing engine.

Initialization includes loading the snapshot, creating the runtime and measuring
the network profile. The supervisor captures initialization I/O, resets proxy
counters, and releases the query.
Query time includes scan planning, Parquet metadata, predicate/DV reads, length
discovery, payload I/O and decoding. IPC export and result comparison are outside
that timer. `process_resources` contains whole-process user CPU seconds, system
CPU seconds and peak RSS in KiB; it includes initialization and, for validation
runs, IPC export. It is not query-only CPU time.

Proxy captures include every query response body, including metadata and DVs.
Reader counters cover data ranges only and distinguish prefix discovery from
payload reads. `logical_prefix_chunks` counts chunks for pages whose length
discovery completed, including the supplied cached prefix. `discovery_requests`
also counts probes that failed to complete length discovery. The header and all
discovered length/null bytes remain intact. Unread
payload bytes exist only as zeroes in the in-memory buffer used with the exact
row selection; no modified table is written.

The initial predeclared policy considered flat, uncompressed V1 BYTE_ARRAY pages of
1-8 MiB, at least 64 bytes per row, and at most one-eighth selected rows. After
bounded length discovery it required at least 25% byte savings. Other pages
used ordinary reads; fallback reused already fetched bytes. This static filter
did not price latency or request processing. Rejecting a page after discovery
could still cost an extra network round trip.

The declared slow profile is 200 ms latency, uniform +/-20 ms jitter and shared
paced 150 Mbit/s. The fast control is 1 ms, no jitter and 10,000 Mbit/s. Long
values use grouped, scattered and dense selections on the slow profile; short,
selected-long and length-heavy cases use scattered selections. The fast profile
checks long, selected-long and length-heavy scattered selections. Every case
retains four correctness runs and six timing runs.

Before timing, the campaign requires every pair to improve by more than the
larger within-mode range for a repeatable gain. A median regression above 5%,
or a consistent smaller regression, needs investigation. `experiment.json`
records those criteria, identities and the active profile before querying;
per-run files retain observations and failures. A lower median alone is not
sufficient evidence.

## Cost-model experiment

The current driver includes the production `range_planning.rs` source and uses
its cost and bounded range-planning helpers. Production source stays unchanged.
The experiment keeps its existing 32-request limit; it does not adopt the
production reader's complete scheduling or calibration implementation.

Before fetching page prefixes, the driver limits discovery to 10% of the
modeled cost of reading the requested batch normally. This includes ordinary
columns in that batch. It prices every dependent discovery round separately,
reserves the initial prefixes together, and checks the shared budget before
fetching any further prefix. If the first round costs too much, the driver
issues the original reads without probing. If later discovery exhausts the
budget, it falls back and reuses the prefix already downloaded.

After locating selected values, the same planner can merge their ranges. The
driver requires at least 25% total byte savings and more than 10% modeled
remaining-read savings. The discovery budget limits speculative work before
value sizes are known; it is not a 10% wall-time guarantee or an acceptable
regression budget. The experiment still investigates regressions under the
criteria above.

`delta_length_network.py run` now passes `auto` to both modes. Each process
measures transport during initialization using the same client and concurrency
limit as its query. Three plans each fetch four concurrent 1 MiB ranges. Payload
intervals that overlap count once; a throughput sample needs at least 10 ms of
delivery time. Three further plans each replenish 64 small, 4 KiB reads across
32 slots. The central half of completion times estimates sustained request
capacity without treating one jittered burst as a steady rate. The model uses
the median latency, throughput and request interval from these plans.

Calibration has a ten-second deadline. A complete run requests 12.75 MiB in
204 ranges, excluding Delta metadata and retries. Missing samples, failed reads
or timeout leave the profile unavailable and experimental queries use ordinary
reads. Both modes perform the same calibration; its time, samples and I/O are
recorded separately from the first query. A fast link can fail the minimum
delivery-duration requirement, in which case the driver does not guess its
bandwidth. Proxy settings describe the test environment but do not enter the
query cost model.

These measurement rules follow the production estimator, but run at the
prototype's 32-request limit on its existing SDK client. Production uses a
different partial-read transport and a larger shared concurrency limit. #453
must reuse production calibration and validate its actual scheduling; the
prototype's measured profile cannot simply be carried across.

For direct invocations, optional `auto` or a cost-profile JSON path goes
immediately before the output directory. `auto` requires S3. Manual profiles
retain the integer `latency_ms`, `mbps` and `request_overhead_bytes` fields for
controlled checks. Without a profile, experimental mode uses ordinary reads.
The query record includes the supplied or measured profile and counts pages
rejected before probing and additional probes stopped by the budget. Source
hashes and all per-process measurements are saved with each experiment.

The earlier campaigns below supplied the proxy's bandwidth cap, maximum latency
including jitter and zero request-processing cost. Their observations remain
separate from the measured-profile follow-up.

## Uncompressed observations, 2026-10-11

The initial nine-case matrix completed all 36 full-result checks and 54 timing
runs. Slow-profile medians below are query-only seconds. Each cell lists ordinary
then experimental reads; MB means 1,000,000 bytes at the proxy response-body
boundary, including query metadata and DVs.

| Case and selection | Seconds | Downloaded MB | Requests |
| --- | ---: | ---: | ---: |
| long, scattered | 30.119 / 6.014 | 499.461 / 1.888 | 122 / 411 |
| long, grouped | 29.806 / 5.361 | 493.156 / 2.951 | 119 / 296 |
| long, dense | 29.572 / 29.575 | 499.330 / 499.330 | 118 / 118 |
| short, scattered | 5.461 / 5.463 | 37.873 / 37.873 | 157 / 157 |
| selected-long, scattered | 5.269 / 5.730 | 33.858 / 33.858 | 49 / 73 |
| length-heavy, scattered | 7.220 / 7.235 | 8.218 / 8.218 | 589 / 589 |

The long fixture contains about 500 MB in two approximately 250 MB Parquet
files. Its actual payload pages hold 1,024-3,072 rows and occupy 2.10-6.31 MB.
The grouped and scattered gains exceeded within-mode timing ranges in all three
pairs. They required at most two dependent discovery rounds per page. Slow
initialization medians were 0.618-0.650 seconds and are excluded from the table.

Dense, short and length-heavy controls used the same bytes and requests in both
modes. Their small time differences do not establish a benefit. The selected-long
control exposed an 8.7% regression: 24 candidate pages were rejected after
reading their lengths, adding 24 requests without saving bytes. Reusing each
prefix avoided duplicate downloads but could not remove the extra latency.
Row selectivity alone is therefore insufficient for an integration decision.

For scattered long values, timing-run median process CPU fell from 1.91 to
0.48 seconds; peak RSS was 150.2 versus 153.0 MiB. For grouped long values, CPU
was 2.10 versus 0.43 seconds, with peak RSS 146.0 versus 155.3 MiB. The prototype
still reconstructs full-size page buffers, so memory does not fall in proportion
to downloaded bytes.

The fast controls completed below one second and are overhead diagnostics,
not a basis for a speedup claim. They exposed avoidable row-selection work on
pages already excluded by size. The reader now checks type and size first;
no eligibility threshold changed. The original matrix and executable are retained
separately from the subsequent fast-profile checks of this reorder. The slow
numbers above belong to the original matrix, not a rerun of the revised binary.
The three follow-up cases passed all 12 full-result checks with identical
download counts, request counts and reader metrics. Their ordinary/experimental
medians were 0.721/0.394 seconds for long values, 0.165/0.181 for selected-long
values and 0.270/0.271 for length-heavy values. These short samples remain
diagnostics; the selected-long case still has no I/O benefit.

The first in-process campaign could not start the fast proxy after inheriting
the observer's CPU affinity. No fast sample had started. A fresh process resumed
only the declared fast cases; the failure log and resume record are retained.
Use the separate CLI invocations above when changing profiles.

These results support continued feasibility work on wide values. The follow-ups
below examine unsuccessful discovery and actual Zstd layouts before a production
proposal for #453.

## Cost-model follow-up, 2026-10-11

The bounded-discovery version completed 24 full-result network checks and 36
timing runs using the same fixtures. Three alternating pairs produced these
slow-profile medians; times exclude initialization and include query planning,
predicate/DV reads, discovery and full consumption.

| Case and selection | Ordinary seconds | Experimental seconds | Downloaded MB | Requests |
| --- | ---: | ---: | ---: | ---: |
| long, scattered | 30.093 | 6.044 | 499.461 / 1.888 | 122 / 411 |
| long, grouped | 29.805 | 5.350 | 493.156 / 2.951 | 119 / 296 |
| selected-long, scattered | 5.267 | 5.267 | 33.858 / 33.858 | 49 / 49 |

Both long-value gains exceeded the larger within-mode timing range in every
pair. Their bytes, requests and partial-page counts matched the static-policy
experiment. The selected-long case rejected all 24 candidate pages before
discovery. It removed the former 24 extra requests and retained exactly the
ordinary bytes and request count. This demonstrates rejection for that fixture;
it does not establish that all skewed variable-length layouts avoid probing.

Slow initialization medians were 0.618-0.643 seconds. Timing-run process CPU
medians were 1.55/0.48 seconds for scattered values, 2.01/0.42 for grouped values,
and 0.19/0.18 for selected-long values. Maximum process RSS was 151.2/162.0 MiB,
145.9/153.8 MiB, and 41.9/42.7 MiB respectively. Full-size reconstruction buffers
remain necessary in this prototype.

Fast-control medians were 0.709/0.389 seconds for long values, 0.175/0.164 for
selected-long values, and 0.286/0.300 for length-heavy pages. The latter two
transferred identical bytes with identical request counts. Their pairwise
differences changed sign and were smaller than the within-mode ranges; these
subsecond observations do not establish a speedup or a repeatable regression.
They also do not establish a production performance guarantee.

Twenty local full-result checks covered dense, short, length-heavy and
missing-profile fallback, plus deliberately exhausted discovery budgets, in both
array formats and modes. A synthetic 500 ms cost profile stopped 49 second-round
probes on the long fixture, reused their first prefixes, and returned the exact
expected values. This local check validates fallback, not network performance.
All other local controls used identical data bytes and requests in both modes.

The first campaign attempt failed in the shared launcher after one ordinary
validation: the cost-profile argument had displaced the required final output
directory argument. The corrected campaign kept the output directory last.
No timing sample had run; the failed attempt and its successful validation are
retained separately. After exact comparisons passed, duplicate IPC outputs were
removed with a receipt, recovering 40.0 GB. Fixtures, references, every sample,
failure, resource record and network trace remain available.

## Zstd layout survey, 2026-10-11

The same four inputs were written with PyArrow 25.0.1, Zstd level 3, V1 pages
and DELTA_LENGTH_BYTE_ARRAY. Their original-input oracle hashes matched the
uncompressed fixtures for all three selections. Only compression changed.

| Case | Payload pages | Entirely raw pages | Raw blocks | Compressed blocks |
| --- | ---: | ---: | ---: | ---: |
| long | 105 | 52 | 1,935 | 1,939 |
| short | 128 | 64 | 160 | 192 |
| selected-long | 32 | 0 | 1,534 | 160 |
| length-heavy | 512 | 0 | 0 | 512 |

No RLE blocks appeared. Every entirely raw page belonged to a binary column.
For long values, the 52 binary pages contained 16-49 raw blocks each. Their
249.351 MB of decoded bodies occupied 249.358 MB including page/frame headers.
The 53 UTF-8 pages used compressed blocks throughout: 249.566 MB of decoded
bodies occupied 131.587 MB. These results describe the generated random binary
and hex-text values, not all binary or string columns.

Selected-long binary pages mixed raw blocks with a compressed block. Their
65-129 blocks per page also show why page size alone is not a discovery-cost
estimate. Zstd block headers give stored sizes; the decoded size of a compressed
block is not available from that header. A raw block inside a mixed frame does
not establish a cheap mapping from decoded value offsets to file offsets.
See [RFC 8878, block types](https://www.rfc-editor.org/rfc/rfc8878.html#section-3.1.1.2.2).

Twenty local full-result checks covered grouped and scattered long values and
scattered selections for the other three cases. Both ordinary and experimental
modes returned the original-input values, nulls and live row IDs in ordinary
and view arrays. The query executable was the unchanged cost-model binary.
With the same explicit cost profile, experimental mode rejected every Zstd
column before discovery and used exactly the ordinary data bytes and requests.
This validates the current fallback, not a Zstd partial-read speedup. No network
timings were collected for this survey.

The first survey attempt stopped because its evidence writer tried to replace
an existing checkpoint. The second completed the long/short controls, then hit
the offline inspector's 128-block limit on a valid 129-block page. The final
inspector allows up to 8,192 blocks within its 8 MiB decoded-page limit; production
limits are unchanged. All failed attempts and completed checks were retained.
The final survey rechecked the reused long/short layouts, then completed all
declared controls. Verified duplicate IPC outputs were removed after comparison.

The inspector and length-parser unit tests pass, as does Clippy for all targets.
The original local decoder proof still passes all 20 cases. Regenerating the
default uncompressed short fixture produced the same object and oracle hashes.
Raw layout alone is insufficient to recommend enabling the optimization. The
following diagnostic measures block-header discovery before adding length/value
mapping and reconstruction to the query driver.

## Raw-block discovery cost, 2026-10-11

[delta_length_zstd_probe.py](delta_length_zstd_probe.py) compares downloading all
eight payload columns with discovering only the raw binary pages' block headers.
It uses the same long-value Zstd fixture and processes its four row groups in
order, as the isolated query driver does. Within each group, requests run with
32 workers. Both modes use the existing curl/SigV4 client through the paced
proxy. The measured process had eight reader CPUs, an 8 GiB memory limit and
no swap, enforced by `storage.reader_prefix`.

This is an optimistic discovery diagnostic, not a query benchmark. Offline
inspection supplies eligible pages and Parquet header lengths. Each page's first
8 KiB request contains its frame header; subsequent block positions are computed
from the headers actually returned over the network. The diagnostic does not
use the offline block offsets to schedule requests. It omits length discovery,
selected-value reads, reconstruction, predicates, DVs and result decoding.

Three alternating pairs on each profile produced these medians:

| Profile | Complete payload reads, seconds | Raw-header discovery only, seconds |
| --- | ---: | ---: |
| 200 ms +/-20 ms, shared 150 Mbit/s | 21.143 | 39.436 |
| 1 ms, shared 10,000 Mbit/s | 1.151 | 2.368 |

Every pair favored complete reads by more than the larger within-mode timing
range. Complete reads used 32 requests and 380,945,504 bytes. Header discovery
used 1,935 requests and 431,633 bytes. Every complete response matched its local
column hash; every discovered header matched the independently inspected layout.
All 12 samples and request traces were retained, with no failures or retries.
The slow proxy passed the existing latency, pacing and cancellation calibration.

The four groups' longest header chains require 48, 42, 49 and 49 requests.
With the slow profile's minimum 180 ms delay, those 188 dependent rounds alone
need at least 33.84 seconds under this scheduling. The traces recorded 37.984
seconds of injected delay on those critical paths. This already exceeds the
measured complete reads, before adding selected payloads or decoding. More
workers cannot overlap dependent requests within a page.

Curl starts a process and connection per request. Its overhead is included in
both modes and dominates the fast control; that control does not predict a
pooled Rust client's performance. Slow-profile median client CPU was 2.032
seconds for complete reads and 11.235 seconds for headers. Whole-process peak RSS
was about 513 MiB, a cumulative high-water mark rather than a mode comparison.
The slow-profile latency floor establishes the limitation independently of
curl's process overhead.

The sequential header strategy is unsuitable for this measured layout and
schedule. Keep these Zstd columns on ordinary reads in the initial #453 proposal.
This does not rule out other page geometry or a different discovery schedule.
The production fixed-width planner already estimates dependent Zstd rounds
before probing; variable-length integration must preserve that check. The
diagnostic bypasses the cost gate deliberately to measure the rejected work.

## Larger skewed batches, 2026-10-11

The uncompressed `selected-long-batch` table occupies 258.7 MB. Its 128 payload
pages each contain 8,192 rows and occupy 1.58-2.37 MB. After DVs, the scattered
predicate selects 130 of 129,798 live rows, but those rows contain 99.905% of the
live payload bytes. The query executable and cost policy are unchanged from the
cost-model follow-up above. Three alternating pairs produced these medians:

| Profile | Ordinary seconds | Experimental seconds | Query requests |
| --- | ---: | ---: | ---: |
| 200 ms +/-20 ms, shared 150 Mbit/s | 17.379 | 18.012 | 157 / 285 |
| 1 ms, shared 10,000 Mbit/s | 0.574 | 0.610 | 157 / 349 |

Both modes downloaded exactly 258,201,068 bytes per query. All 128 candidate
pages passed the initial cost gate, then fell back. Slow-profile discovery made
128 first-prefix requests; the budget refused 64 further requests. Fast-profile
discovery made 192 requests across at most two dependent rounds per page.
The eight full-result checks passed for both modes and array forms. Traces
confirmed that fallback reused prefixes without repeating or omitting page bytes.
All 12 timing samples and the failed preflight source-hash check were retained;
that failed attempt stopped before starting storage or taking any sample.

The slow regression was 3.65%. Every pair lost 0.621-0.651 seconds, exceeding the
0.055-second within-mode range. This is a repeatable cost of unsuccessful
discovery, not a zero-cost rejection. The fast median increased 6.20%; all pairs
lost time, but only one difference exceeded the within-mode range. Those
subsecond measurements require investigation rather than a precise slowdown
claim. Timing-run process CPU medians were 1.25/1.26 seconds on the slow profile
and 0.52/0.61 on the fast profile. Maximum process RSS was 196.9/212.7 MiB and
195.9/222.0 MiB, respectively. These resource figures include initialization.

Trace analysis found an avoidable request in the prototype. Every page's length
and null information fits in the first 8 KiB response. After removing its
25-byte Parquet header, the transport caches 8,167 body bytes. The parser starts
with a separate 4 KiB buffer; growing it to 8 KiB asks the transport for another
25 bytes even when the cached prefix already contains the complete structure.
The slow budget rejected these 64 extensions, while the fast profile permitted
them. The follow-up below tests letting the parser examine all cached bytes
before requesting more. The initial probe still has a cost when the lengths
reveal that the selected values contain almost all of the payload.

## Cached-prefix reuse, 2026-10-11

The parser now starts with the body bytes already fetched alongside the page
header. It requests only extensions beyond that prefix. Regression checks cover
a complete cached length stream, an incomplete prefix that needs more bytes,
and supplied prefixes exceeding the page or discovery limit. The initial prefix
still counts toward the nine-chunk limit and the transport's cost budget.

The repeated network matrix passed 24 full-result checks and completed 36 timing
runs, using the same fixtures and three alternating pairs per case. Slow means
200 ms +/-20 ms with shared 150 Mbit/s; fast means 1 ms with shared 10,000 Mbit/s.

| Profile and case | Ordinary seconds | Experimental seconds |
| --- | ---: | ---: |
| Slow, long scattered | 30.113 | 5.330 |
| Slow, long grouped | 29.803 | 4.565 |
| Slow, selected-long scattered | 5.269 | 5.269 |
| Slow, selected-long-batch scattered | 17.366 | 18.017 |
| Fast, long scattered | 0.728 | 0.369 |
| Fast, selected-long-batch scattered | 0.553 | 0.576 |

The slow long-value queries retain gains exceeding the timing spread in all
pairs. Their experimental medians were 6.044 and 5.350 seconds before the fix.
Query requests fell from 411 to 362 for scattered matches and from 296 to 247
for grouped matches. Discovery now uses one round per page in both cases.
Transferred bytes changed only slightly: this fix removes dependent requests,
while the existing partial reads provide the byte savings.

For larger skewed batches, both profiles now use 128 initial probes and no
extensions. Fast query requests fell from 349 to 285; slow requests remained
285 because the old budget already refused the extensions. Traces in both array
formats verify that every page is covered once by its prefix and remaining
suffix, with no gaps, repeated bytes or 25-byte extension requests.
Ordinary reads use 157 requests and the same 258,201,068 bytes. Initial discovery
still costs time: all slow pairs lost 0.647-0.664 seconds, beyond a 0.024-second
within-mode range. All fast pairs also lost time, by 0.023-0.033 seconds versus a
0.021-second range. These short fast samples are diagnostics, not a general
slowdown estimate. The smaller selected-long case still rejects discovery
before I/O and uses exactly the ordinary 49 requests and 33,858,108 bytes.

For the larger skewed batch, ordinary/experimental process CPU medians were
1.24/1.36 seconds on the slow profile and 0.50/0.56 on the fast profile. Maximum
process RSS was 194.6/222.6 MiB and 194.2/231.6 MiB, respectively. The first
probe and full-page reconstruction still have costs after this fix.

The parser, planner and budget unit tests, Clippy and the 20-case local decoder
proof passed. Twenty local full-result controls also passed. The first local
control script stopped at an obsolete expectation: its synthetic 500 ms profile
previously rejected redundant extensions. With reuse, all 105 long-value pages
complete discovery from the initial prefix. The updated control requires that
behavior and exact values in both array formats. Both attempts are retained.
Verified duplicate IPC outputs were removed after comparison; samples, traces,
resource records, fixtures and independent oracles remain available.

## Measured-profile follow-up, 2026-10-11

The driver now measures latency, throughput and request capacity on its own
client during initialization. Cost thresholds, fixtures and query timing
boundaries are unchanged. The six-case matrix passed 24 full-result checks and
completed 36 timing runs. All 40 slow-profile initializations completed
calibration. Measured latency was 206-210 ms, throughput 18.68-18.75 MB/s, and
request cost 119,657-123,449 byte-equivalent units. That last figure prices
request capacity; it is not additional downloaded data.

| Profile and case | Ordinary seconds | Experimental seconds |
| --- | ---: | ---: |
| Slow, long scattered | 30.095 | 5.316 |
| Slow, long grouped | 29.799 | 4.539 |
| Slow, selected-long scattered | 5.270 | 5.269 |
| Slow, selected-long-batch scattered | 17.353 | 17.368 |
| Fast, long scattered | 0.731 | 0.718 |
| Fast, selected-long-batch scattered | 0.566 | 0.564 |

Both slow long-value cases retained gains beyond within-mode spread in every
pair. Their query bytes and requests are unchanged from the cached-prefix
follow-up. Both slow selected-long cases now reject discovery before I/O.
The larger case rejects all 128 candidate pages and uses exactly the ordinary
157 requests and 258,201,068 bytes, instead of 285 requests. Its paired extra
time was 12-17 ms against a 16 ms within-mode range. This removes the previous
repeatable 0.65-second discovery penalty; it does not establish zero overhead.
The smaller case retains the ordinary 49 requests and 33,858,108 bytes.

Four untimed local controls isolate the changed decision. With latency fixed
at 207 ms and bandwidth at 149 Mbit/s, changing only request cost from zero to
122,367 byte-equivalent units changes the larger skewed case from 128 probes
and fallbacks to zero probes. The positive long-value case still reads 105 pages
partially under both profiles. All four results match the independent input
oracle. No layout or selectivity threshold was changed to obtain this result.

The fast results do not establish a gain. Nine of 20 initializations lacked
three transfers with at least 10 ms of delivery time and returned no profile.
All five experimental long-value runs used ordinary reads, with the same
122 requests and 499,461,298 bytes as their controls. The larger skewed case
used 157 requests when calibration was incomplete, but 285 in its two runs
with complete profiles. Those two runs still probed and fell back; measured
calibration does not eliminate unsuccessful discovery under every condition.
Both fast timing comparisons stayed within the observed spread. Their medians
mix available and unavailable profiles and are not speedup claims.

Slow calibration took 2.555-2.576 seconds; total initialization took
3.361-3.416 seconds. Fast initialization took 0.062-0.202 seconds. These costs
are reported separately from the first fully consumed query. For slow
scattered/grouped long values, ordinary/experimental process CPU medians were
1.64/0.50 and 2.02/0.50 seconds. Peak RSS remained below 151 MiB. The larger
slow skewed case used 1.08/1.35 CPU seconds and peaked at 196.8/198.1 MiB.
These process resource counts include calibration, so they cannot isolate
query-only CPU overhead.

All 24 query-binary unit tests and Clippy passed, including measurement checks
for overlapping transfers, sustained request capacity and unavailable profiles.
Samples, initialization captures, query traces and source hashes are retained;
verified duplicate IPC outputs were removed after comparison. Production
source and the published README benchmarks remain unchanged.

## Handoff to #453

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
another read. The experiment's limits are 10% of modeled ordinary-read cost
for discovery, at least 25% total byte savings and more than 10% modeled
remaining-read savings. Preserve production's accounting of both modeled and
observed work; a modeled budget is not a wall-time guarantee. Variable lengths
must not use the fixed-width planner's uniform row-offset estimate.

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
fallback. In particular, rejection of the slow skewed case at concurrency 32
is not a promise about production's larger request allowance. Retain exact
value/null comparisons, real DVs, ordinary arrays, DataFusion view arrays,
malformed-input checks and failure/cancellation coverage.

Apply #449's regression gate: three alternating pairs for the new cases and
all four retained README DV queries against the PR base, with initialization
separate from the first complete query. Investigate a median regression over
5% or a consistent smaller regression; unresolved or inconclusive results do
not pass. The parent's final eight-case comparison remains separate. No new
production support or default is enabled by #452.
