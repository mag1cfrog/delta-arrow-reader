# Automatic partial-page reads: default validation

The default now prepares supported S3 tables and permits partial-page reads
when the measured network profile predicts a saving above 10%. Unsupported
layouts, missing evidence and predicted losing cases retain ordinary reads.
This check tests those defaults after the calibration, missing-evidence and
connection-pool fixes in PRs #441, #443 and #442.

## Query time and initialization

The retained TPC-H-derived scattered + DV tables use the same snapshots, queries
and independent references as the earlier reader comparisons. Both modes run
the same executable. `Default` uses the public partial-read and warmup defaults;
`Ordinary` explicitly disables partial reads and prepares only metadata.

The primary network profile is 200 ms latency, deterministic +/-20 ms jitter
and 150 Mbit/s shared, streamed bandwidth. Each fresh process initializes one
table, then runs two complete queries. Three alternating pairs per table give
the following medians in seconds:

| Columns | Mode | Initialize | First query | Second query | Initialize + first query |
| --- | --- | ---: | ---: | ---: | ---: |
| 416 | Ordinary | 1.36 | 10.35 | 10.35 | 11.70 |
| 416 | Default | 4.48 | 6.54 | 6.48 | 11.00 |
| 90 | Ordinary | 0.70 | 22.33 | 22.33 | 23.02 |
| 90 | Default | 3.79 | 10.28 | 10.28 | 14.08 |

First-query ranges were 10.326-10.375 versus 6.499-6.544 seconds for 416
columns, and 22.304-22.326 versus 10.268-10.286 seconds for 90 columns.
Combined medians use each session's initialization plus first query, rather
than adding medians. Query timing includes planning and full result consumption.

The query gains cost more requests and CPU. Across initialization and both
queries, median Parquet request counts rose from 80 to 27,878 for 416 columns
and from 108 to 44,692 for 90 columns. Median process CPU time rose from 1.98
to 4.86 seconds and from 3.95 to 7.68 seconds, respectively. Initialization
schedules 24 MiB across 3,084 requests, excluding retries, with a five-second
sampling limit after metadata loading.

## Fallback and resource checks

At 1 ms latency and 1 Gbit/s, one pair per table used exactly the same query
Parquet ranges and bytes in both modes. First-query times were 1.472 versus
1.448 seconds for 416 columns and 3.259 versus 3.264 seconds for 90 columns.
One pair cannot establish a small timing difference. Profiling still added
about 1.2 seconds to initialization even though both queries used ordinary reads.

All 24 separate MinIO exports matched every reference value and null, with
894 rows each. These include four concurrent queries through one initialized
table for each schema. Sampled file-descriptor peaks were 581 and 607 for those
concurrent checks; all timing sessions stayed at or below 550.

A clean downstream Rust consumer used the default builder against anonymous
native S3, without MinIO or a network proxy:

`s3://daft-public-datasets/red-pajamas/stackexchange-sample-deltalake-zorder`

At snapshot 4 in `us-west-2`, single-table calibration completed with a sampled
peak of 482 descriptors. Four simultaneous table initializations peaked at 483;
all four profiles timed out and their queries correctly used ordinary reads.
All five exports matched the retained, previously validated reference. Each
process opened six additional files successfully while its tables remained alive.
This table's query is ineligible for partial-page reads. These checks cover
native HTTPS, initialization, connection resources and fallback; eligible
partial-page performance on real S3 remains unmeasured.

Five fresh Python processes used an isolated installation of the new wheel.
The default constructor completed 24 MiB / 3,084 calibration reads. Explicit
`warmup="none"`, `warmup="query_planning"`, the Kernel backend and a custom
timeout each made zero Parquet calibration requests. All five returned the
same snapshot and schema and completed an empty scan. These are constructor
and opt-out checks, not Python query-performance measurements.

## Method and artifacts

There are 16 timing sessions: three pairs per primary case and one pair per
low-latency case. All declared samples are retained in the
[CSV](intra-page-defaults-samples.csv); [provenance](intra-page-defaults-results.json)
identifies the build, fixtures and validation. CSV I/O and CPU counters cover
initialization plus both queries. Proxy counters measure client-facing HTTP
body bytes; repeated ranges do not identify SDK retries.

The runner kept the existing benchmark resource settings: eight pinned logical
CPUs, an 8 GiB memory limit and a soft descriptor limit of 1,024. OS and MinIO
caches were reused. Descriptor polling ran every 25 ms with a stop threshold
of 896; observed peaks are not a hard global bound. No compiler was running
at any sample launch; CPU affinity was not an exclusive reservation. Fixture
verification and exact-result checks preceded timing.

To select these defaults in the [DAR runner](selective-read-runners.md), set
`DAR_INTRA_PAGE_READS=default` and `DAR_NETWORK_WARMUP=default`. The ordinary
control uses `off` for both with a retained `execution_mode="reuse"` request.
The runner records the resolved provider settings. Its other benchmark tuning
remains unchanged, and the existing `intra_page.py` comparison still uses
explicit modes. The acceptance driver selected the two new environment values
before invoking that harness.

The build is based on `515da2e46a83fdc00de262b3663e059c1732eb6b` plus this
default-enablement patch. The build record retains the exact patch and source
hashes; analysis verified the executable and all 83 recorded source files.
Raw logs, requests, traces, exports and drivers are retained locally under
`/home/hanbo/repo/selective-read-intra-page-defaults-final-20261009`.

Validation passed 625 Rust tests with eight ignored, 54 Python tests,
all-target/all-feature workspace Clippy on Rust 1.94, formatting, rustdoc with
warnings denied and the strict documentation build. The CLI suite also passed,
including 17 process checks; `inspect` explicitly skips warmup. These reader-only checks
leave the published five-reader results and README chart unchanged.

To skip profiling, use Rust `WarmupMode::None` or Python `warmup="none"`.
Rust callers can also disable partial reads on the table builder with
`with_experimental_intra_page_reads(false)`. See the
[execution options](../../public/reference/execution-options.md) for scope and
per-scan overrides.
