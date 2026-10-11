# Delta-length decoder experiment

This isolated experiment for [#452](https://github.com/mag1cfrog/delta-arrow-reader/issues/452)
checks whether discovering lengths and fetching selected string/binary values
is cheaper than reading complete pages. It includes a local decoder proof and
a Delta query driver using the existing network harness. Production execution
and the published README benchmarks are unchanged.

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

The four cases are `long`, `short`, `selected-long` and `length-heavy`. Each
contains two unpartitioned Delta files, a real DV per file, eight required or
nullable UTF-8/binary payload columns, and `row_id`, `grouped` and `scattered`
columns. `grouped = 1` selects eight adjacent rows per 2,048; `scattered = 1`
selects one per 997. `dense` reads all live rows. DVs delete every 103rd physical
row. The independent expected values are filtered from the Python input before
Parquet encoding and saved as compressed Arrow IPC.

Generation also runs `delta-length-pages inspect FILE` against every Parquet
file. The manifest records actual page sizes, row counts, structural byte counts,
encoding, compression and offset-index presence, not just writer settings.

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

Initialization ends when the snapshot and runtime are ready. The supervisor
captures initialization I/O, resets proxy counters, and releases the query.
Query time includes scan planning, Parquet metadata, predicate/DV reads, length
discovery, payload I/O and decoding. IPC export and result comparison are outside
that timer. `process_resources` contains whole-process user CPU seconds, system
CPU seconds and peak RSS in KiB; it includes initialization and, for validation
runs, IPC export. It is not query-only CPU time.

Proxy captures include every query response body, including metadata and DVs.
Reader counters cover data ranges only and distinguish prefix discovery from
payload reads. Logical parser chunks can differ from network discovery rounds
because the initial 8 KiB request contains both the page header and some body
bytes. The header and all discovered length/null bytes remain intact. Unread
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
production reader's complete scheduling or automatic network calibration.

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

`delta_length_network.py run` saves `cost-profile.json` and supplies it to both
modes. This controlled experiment uses the proxy's bandwidth cap and its maximum
configured latency including jitter. Request-processing cost is explicitly zero,
an optimistic assumption. The slow proxy is checked with the existing network
calibration helper; the fast bandwidth cap is not a measurement of achievable
client throughput. Actual query time, CPU, memory and I/O remain measured.
These inputs test transport-based decisions, not the accuracy of automatic
profiling. Production integration still needs a calibrated request-processing
estimate and must validate these decisions with the actual reader.

For direct invocations, the optional cost-profile JSON goes immediately before
the output directory. Without a profile, experimental mode uses ordinary reads.
The query record includes the profile and counts pages rejected before probing
and additional probes stopped by the budget. Source and profile hashes are saved
with each experiment.

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

These results support continued feasibility work on wide values. Zstd layouts
and a validated policy for unsuccessful discovery remain necessary before #453
can recommend production integration.

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

## Remaining work for #452

Inspect actual Zstd block types before considering raw-block reads. Compressed
blocks need a recorded fallback result. The current cost experiment uses known
proxy settings and optimistic request-processing cost; it does not complete
automatic-profile validation or prove rejection across larger skewed batches.
Use the measured limits and losing cases to update #453 before recommending
integration. No new production type support or default is enabled by this
experiment.
