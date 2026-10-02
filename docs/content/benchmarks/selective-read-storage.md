---
title: Observe selective-read storage requests
description: Use one pinned local MinIO server to validate uploads and observe all five readers' S3 requests.
---

# Observe selective-read storage requests

The storage observer uses MinIO's native S3 trace and metrics endpoints. All five
readers use the same loopback endpoint, optionally through the network proxy
described below. The launcher enforces
the protocol's CPU and memory budgets. Timing invocations have no trace
subscriber; validation, plan export and I/O diagnostics run separately.

## Start the pinned server

Use Linux x86-64 with cgroup v2, a working systemd user manager, `taskset`, `curl`
with `--aws-sigv4`, Go toolchain downloads, and an authenticated `gh` CLI.
Prepare the [five reader builds](selective-read-runners.md), the
[oracle environment](selective-read-oracle.md), and
[public fixtures](selective-read-fixtures.md) first. Run from the repository root:

```sh
python3 -B benches/selective_read/storage.py prepare \
  --output ../selective-read-minio-build
python3 -B benches/selective_read/storage.py start \
  --build ../selective-read-minio-build --state ../selective-read-storage \
  --port 19000
python3 -B benches/selective_read/storage.py upload \
  --state ../selective-read-storage --fixtures ../selective-read-smoke \
  --output ../selective-read-upload.json
```

Build and state directories must be new. Preparation checks out source commit
`07c3a429bfed433e49018cb0f78a52145d4bedeb` and builds release
`RELEASE.2025-09-07T16-13-09Z` with Go 1.24.7, `CGO_ENABLED=0`, Linux/amd64 and
`GOAMD64=v1`. To reuse an existing clean checkout at that exact commit, pass
`--source PATH` to `prepare`. Keep the binary, build record, Go module files and
license together.

The service binds only to `127.0.0.1`. Generated credentials live in the private
state directory. The upload checks every local object's size and SHA-256 against
the fixture manifest, uses conditional PUTs to preserve existing objects, and
verifies each uploaded object with a complete GET and SHA-256. Its receipt records
the object inventory and `s3://selective-read/MANIFEST_SHA256` table root.
For paired DV fixtures, that inventory includes both snapshot logs and all
referenced DV payloads as well as Parquet objects.

The launcher selects eight logical CPUs for the reader, two separate physical
cores for MinIO, and a third separate core for the trace process. It never splits
SMT siblings across these groups. Insufficient CPU topology fails setup. MinIO
gets a 4 GiB memory cap; readers get 8 GiB. Both disable swap. Setup verifies
MinIO's effective affinity and cgroup limits; every reader launch verifies and
saves its own limits before table I/O. These caps include native allocations.

`server.json` records the CPU topology, placement, kernel, memory, mount, curl
version/hash, server build identity, startup command and cache policy.
`limits.json` records the actual server cgroup. Keep MinIO running throughout a
campaign. Clients are fresh for every invocation; server and OS caches are reused
without flushing. Start the server once before the campaign's uploads and
warm-ups. Do not restart it between readers or modes. Use an otherwise idle host;
affinity does not reserve CPUs against unrelated host processes.

## Add controlled network conditions

To measure a remote-network profile, attach the optional proxy after starting
MinIO and uploading the fixtures:

```sh
python3 -B benches/selective_read/network.py start \
  --state ../selective-read-storage --output ../selective-read-network \
  --latency-ms 200 --jitter-ms 20 --mbps 150 --seed 0
python3 -B benches/selective_read/check_network.py \
  --state ../selective-read-storage --output ../selective-read-network-check
```

The new output directory holds a Go 1.24.7 standard-library-only proxy build,
its source, binary hash, profile and effective process limits. The service uses
one physical core separate from the readers, MinIO and observer, with a 512 MiB
memory cap and no swap. Setup fails if that separate core is unavailable.
Port 19002 is the default; use `--port` to choose another loopback port.

All five readers receive the proxy endpoint through the existing storage
launcher. Uploads, health checks and MinIO trace subscriptions continue to use
MinIO directly. A stopped or changed attached proxy fails verification instead
of silently falling back to an unshaped connection.

The default profile adds 200 ms before forwarding each GET or HEAD, with uniform
jitter between -20 and +20 ms. It represents additional request/response delay,
not 200 ms in each direction. Jitter uses integer microseconds derived from
SHA-256 of the seed, HTTP method, original request URI and Range header.
Identical requests repeat the same delay regardless of scheduling order; a
different seed gives another reproducible assignment. This is request-level
variation, not a time-correlated model of Internet congestion.

Response bodies share one 150 Mbit/s budget across all connections, equivalent
to 18,750,000 bytes/s or about 17.9 MiB/s. The proxy paces and flushes small chunks
while preserving S3 signatures, Range requests and connection reuse. It does
not buffer a whole object before delivering it. Timer catch-up is bounded to
one 64 KiB chunk. A cancelled request can consume one reserved chunk's time;
the pacing schedule resets between idle reader invocations.

The calibration command checks signed full GET, HEAD and byte ranges, progressive
delivery, body contents, two explicit failed attempts, a cancelled transfer and
the combined rate of four concurrent downloads.
It requires the default latency, jitter and bandwidth values and accepts any
seed. `--latency-ms 0 --jitter-ms 0 --mbps 0` supplies an unshaped proxy control.
These settings model an HTTP transport envelope, not TCP RTT, packet loss, TLS
or a calibrated guarantee about any particular S3 deployment.

Each observation records the network profile and proxy provenance under
`storage_environment.network`. `network_io` counts response-body bytes accepted
by the proxy's client-facing HTTP writer. The existing `storage_io` and MinIO
trace retain their upstream boundary. The two can differ when a client cancels
after MinIO has sent data into proxy or socket buffers. Neither counter measures
bytes decoded by the reader or TCP/IP overhead.

I/O diagnostics also save `network-requests.jsonl`, including actual chosen
delays, request IDs, ranges and incomplete bodies. Its totals must reconcile
with the proxy counters. Timing runs collect only aggregate proxy counters,
without per-request trace records. `external_metrics.response_bytes` uses the
proxy boundary for shaped I/O diagnostics, with an explicit boundary label.
The campaign records and hashes the attached network configuration.

To detach the proxy while keeping MinIO running:

```sh
python3 -B benches/selective_read/network.py stop --state ../selective-read-storage
```

Stopping MinIO through `storage.py stop` also stops its attached proxy. The
proxy's build/profile record and shutdown receipt remain in its output directory.

The focused HTTP check can run without MinIO:

```sh
GOTOOLCHAIN=go1.24.7 go test -race -v \
  benches/selective_read/network_proxy.go \
  benches/selective_read/network_proxy_test.go
python3 -B benches/selective_read/test_network.py
```

Both checks are manual. This transport adds no CI job or performance threshold.

## Validate, time and observe a remote case

Use the oracle environment's Python for this example. All paths below are relative
to the repository root. The output directories must be new:

```python
import json
from pathlib import Path
import sys

sys.path.insert(0, "benches/selective_read")
import observe
import oracle
import run

state = Path("../selective-read-storage")
fixtures = Path("../selective-read-smoke")
binary = Path("../selective-read-dar-build/selective-read-dar")
root = json.loads(Path("../selective-read-upload.json").read_text())["table_root"]
case = "li.clustered.eq2-in20"
reference = Path("../remote-reference")
oracle.prepare(fixtures, case, reference)
payload = run.request(fixtures, case, "open", "validation", "remote-validation",
                      table_uri=root + "/li.clustered")
validation = Path("../remote-validation")
record = observe.invoke(state, binary, payload, validation, fixtures, reference)
assert record["status"] == "success"

timed = run.request(fixtures, case, "open", "timing", "remote-timing",
                    table_uri=payload["table_uri"],
                    correctness=validation / "correctness.json")
assert observe.invoke(state, binary, timed, Path("../remote-timing"))["status"] == "success"
io = dict(payload, purpose="io", run_id="remote-io")
assert observe.invoke(state, binary, io, Path("../remote-io"))["status"] == "success"
```

Substitute any other prepared executable to use the same server and observer.
The launcher removes inherited AWS profiles, endpoints and proxies, supplies the
dedicated credentials through the environment, and applies the same path-style
HTTP endpoint to all adapters. An exclusive state lock prevents overlapping
invocations and uploads. Keep this server dedicated to the benchmark.

For an existing saved request, the equivalent command is:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/observe.py run \
  --state ../selective-read-storage \
  --binary ../selective-read-dar-build/selective-read-dar \
  --request ../remote-io/request.json --output ../remote-io-repeat
```

Validation also requires `--fixtures` and `--reference`. Timing requires the
request's matching remote correctness certificate. A local-file certificate
cannot authorize the same query against S3.

`storage-observation.json` augments the ordinary reader observation with verified
limits, server identity, capture status, and I/O totals. A sibling directory with
the suffix `-io` holds the capture proof and sanitized `requests.jsonl`. Failed
captures retain their artifacts and invalidate the storage observation.

## Observation boundary

The pinned MinIO source records the byte counts returned by HTTP `Write` and
`ReadFrom` in its
[`ResponseRecorder`](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/internal/http/response-recorder.go).
These are response-body bytes accepted by the server's HTTP writer. They exclude
headers and include accepted buffered data, which can exceed what a canceled
client consumed. They are neither advertised Content-Length nor decoded rows.
The check below verifies full GET, range GET, HEAD, LIST, and an interrupted
64 MiB transfer against known requests.

Native trace is preferable here to a forwarding proxy: the readers retain their
own connections and S3 implementations. MinIO's trace publisher can drop records
when its subscriber falls behind. Every capture therefore reconciles all traced
requests and body bytes with the server's independent S3 counters at
`/minio/metrics/v3/api/requests`. A zero-byte control HEAD confirms subscription
before the reader starts and another confirms draining after it exits. Control
traffic is excluded from reader totals. Missing, duplicate, unrelated, rejected,
or undrained traffic invalidates the observation. Bucket discovery requests are
allowed; object requests and listing prefixes must belong to the selected table.

Only selected fields are saved. Authorization headers, credentials, raw response
bodies, and unrelated object names never enter the request artifacts. Every
attempt counts as a request. SDK retry metadata is retained when present;
identical requests alone do not prove a retry.

Capture starts before the reader process and remains active through process exit
and pending server requests. Diagnostic lifecycle markers identify stream
completion. Requests crossing or following that boundary remain visible,
including work after LIMIT. These timestamps describe overlap, not which query
caused an asynchronous request in a reuse session.

| Saved field | Meaning |
| --- | --- |
| `api`, `method`, `object`, `object_class` | Native S3 operation and relative key, classified as log, Parquet, DV, listing, bucket or other |
| `range`, `content_range` | Requested Range and returned Content-Range |
| `response_bytes` | Body bytes accepted by MinIO's HTTP writer for this attempt, including error bodies |
| `advertised_content_length`, `incomplete_body` | Separate advertised length and evidence of a short non-HEAD response |
| `status`, `request_id` | Native status and response request ID, including failed attempts |
| `sdk_invocation_id`, `sdk_attempt` | Retry metadata when supplied; otherwise null |
| `run_id`, `case_id`, `started_ns`, `ended_ns` | Invocation identity and native UTC request boundaries |

The summary counts calls separately from distinct touched Parquet objects, and
lists GET and HEAD objects separately. Log checkpoints ending in `.parquet` count
as log traffic. An empty result can still touch Parquet footers: inspect the
recorded ranges instead of treating a touched object as a complete data-file
read. A file absent from both object lists received no observed HEAD or GET.

`started_after_final_stream` and `ended_after_final_stream` expose work after the
last stream completes, including LIMIT cleanup. A response spanning that event
has one byte total; the observer cannot divide it into before/after bytes.
`response_bytes_after_stream` therefore stays null. UTC events identify overlap;
the monotonic `diagnostic_session_ns` is used only for observer overhead.

## Run the bounded storage check

Use the smoke fixture to check the entire path without a report-scale campaign:

```sh
../selective-read-oracle-venv/bin/python -B benches/selective_read/check_storage.py \
  --state ../selective-read-storage --fixtures ../selective-read-smoke \
  --binary ../selective-read-dar-build/selective-read-dar \
  --binary ../selective-read-delta-rs-build/selective-read-delta-rs \
  --binary ../selective-read-duckdb-build/selective-read-duckdb \
  --binary ../selective-read-polars-build/selective-read-polars \
  --binary ../selective-read-daft-build/selective-read-daft \
  --output ../storage-check
```

The check validates uploads, known request bytes, a canceled download, explicit
retry metadata, trace completeness, and output from every remote reader against
the independent oracle. It covers compound predicates in both layouts, an empty
result, LIMIT, wide projection, and ten-query reuse. The pinned Spark DV fixture
checks real sidecar traffic for the four supporting readers and Daft's explicit
unsupported result. It does not change the later report-scale DV scenarios.

For each reader, one warm-up per trace mode precedes four fresh-process diagnostic
runs in off/on/on/off order. `checks.json` preserves the separate durations,
median difference and ratio. These few smoke samples check observer cost; use the
campaign's repetitions and statistics before drawing performance conclusions.
Never combine diagnostic I/O and untraced latency as one measured sample.

This check is manual and adds no CI job or step. The
[campaign scheduler](selective-read-campaign.md) supplies repetitions and
reporting. The shared launcher enforces phase deadlines and records whole-process
CPU/RSS for both individual invocations and campaigns. Keep credentials, state data and generated artifacts
outside the repository; publish only reviewed, sanitized records.

Stop the owned service when finished:

```sh
python3 -B benches/selective_read/storage.py stop --state ../selective-read-storage
```
