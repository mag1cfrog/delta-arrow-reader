# STRING parser attribution protocol (#1389, PR 1)

This reference fixes the measurement-only first PR of
[#1389](https://github.com/mag1cfrog/delta-arrow-reader/issues/1389). Product
source is pinned to `feat/spark-sql-staging` commit
`298015f9def26cc97a271bdbc0316e64083d7874`. The
[source inventory](parsers-1389-source-inventory.md) records every macro entry
and runtime consumer, including implicit conversions and per-batch schema
parsing. This PR changes no parser, error handling, dependency fork, core,
CLI/Python integration or staging-to-main activation.

The coordinator reviews and commits this protocol, inventory, manifests and
harness before release preflight. Commit the successful bound preflight receipt
before timing, rebuild that exact source and independently verify its identity.
A clean tree alone does not release timing. There are no pilot SQL timings or
unchanged reruns; an operational failure preserves partial evidence and stops.

## Amendment after the full-input interval failure

The first preflight at `fe91cc8b6c2314065b78b246804ab391ba051901`
completed ten exports and six exact native/Spark date/timestamp comparisons.
It then stopped on `interval_year_month` with ANSI off: the process exited
with SIGABRT (-6), reporting a failed 4,864-byte allocation under the original
16 GiB preflight address-space limit. The full log and partial exports remain
under `1389/pr1-preflight` and `1389/pr1-preflight.log` in the external evidence
root named below. No timing collection ran. This operational failure is retained as
evidence; it is not a semantic exclusion or an interval performance number.

The coordinator's separately specified untimed allocation probe used the exact
release analyzer from that build, ordinary `2 months` text, no retained results,
separate processes, affinity 2-5, a 2 GiB address limit and a 60-second limit.
Across 4,096 calls, interval VmSize grew from 7,080 to 65,960 KiB, about
14.375 KiB per call; RSS/HWM also grew. The date fallback control's VmSize stayed
at 7,080 KiB. The probe and logs remain under `1389/allocation-diagnostic`.
This observes retained memory after repeated parsing. Source strongly identifies
owned cycles in the constructed recursive graph; it does not attribute every
observed byte to a particular allocation.

This amendment fixes the interval input predicate to
`l_orderkey BETWEEN 1 AND 8192` for all three interval shapes and their day-time
projection control. It keeps real SF1 rows and original input/NULL expressions.
There is no LIMIT, resampling, regenerated fixture, memory-cap increase or product
change. A fully decoded existing native date export independently establishes
exactly 8,184 selected key/line pairs, including 90 rows whose interval input is
NULL. The full export contained 6,001,215 rows; selected key/line pairs were
unique, with line numbers 1-7. The
[count proof](../parsers/interval-input-count.json) binds the original IPC and
independent decoder source SHA-256 values and raw result path. The coordinator
and independent reviewer verify those historical bytes as provenance before
this amendment is committed. Their external paths are not runtime prerequisites
for reproducing the benchmark.

The domain gives an absolute row ceiling of 8,192 keys times seven lines,
57,344 rows. Four executions per timed process give at most 229,376 interval
parser calls before NULL skips. Extrapolating the observed probe slope gives
about 3.15 GiB retained virtual memory at that ceiling, below the unchanged
12 GiB measured-process cap. The actual subset has 32,376 non-NULL calls across
four executions, suggesting about 455 MiB by the same estimate. These are sizing
estimates, not measured SQL peaks or guarantees; preflight still stops on any
operational failure. Actual batches and memory peaks must come from execution
evidence. DATE, both TIMESTAMP shapes and date_format keep all 6,001,215 rows.

Review and commit this changed protocol, manifest, count proof and controller
before rebuilding or resuming preflight in a new evidence directory. The first
failed preflight remains immutable. The fixed timing order and all resource
limits below stay unchanged, and no valid timing run is repeated.

## Fixed workload and references

The [manifest](../parsers/queries.json) contains seven parser shapes and one
projection control. All retain Int64 `l_orderkey`, Int32 `l_linenumber` and one
result column. The inputs are native ViewTable expressions over the retained
SF1 source. Each replaces every row whose order key is divisible by 97 with
NULL, so the full comparison checks actual NULL placement as well as values.

| Shape | Input when non-NULL | Spark output | Native reference |
| --- | --- | --- | --- |
| `date_fallback` | Source ship year plus `-2-9` | Date32 | CAST of the same text to DATE |
| `timestamp_ltz` | Source date plus ` 12:34:56` | Timestamp(Microsecond, UTC) | ARROW_CAST to exactly the same unit and UTC annotation |
| `timestamp_ntz` | Same timestamp text | Timestamp(Microsecond, no zone) | ARROW_CAST to exactly the same unit without zone |
| `interval_year_month` | Line number plus ` months` | Interval(YearMonth) | ARROW_CAST of the same text to Interval(YearMonth) |
| `interval_day_time` | Line number plus ` days 2 hours` | Duration(Microsecond) | No equivalent native string conversion with this executed type; use Spark alone above its input projection |
| `interval_calendar` | Line number plus ` months 1 day 2 seconds` | Interval(MonthDayNano) | ARROW_CAST of the same text to Interval(MonthDayNano) |
| `date_format_string` | Same ordinary timestamp text | STRING formatted as `yyyy/MM/dd HH:mm` | TO_CHAR after the equivalent native timestamp parse, `%Y/%m/%d %H:%M` |
| `interval_day_time_input` | Same day-time input column | Plain STRING projection | Control in each Spark ANSI mode |

The day-time native interval parser returns a calendar interval; Arrow's cast
from that type to Duration rejects nonzero day/month fields. Changing the text
to seconds-only or replacing conversion with a fixture-specific calculation
would measure a different operation. This shape therefore has no native target
flag. Its `native_sql` field is an unused input projection, required by the
shared manifest structure; the schedule and preflight never run it as a native
reference. Neither projection output nor its type is compared with the interval
output. Compare the complete interval outputs independently between ANSI modes,
and compare complete control outputs between modes.

The date fallback input always bypasses the ten-byte ASCII fast path. Other date
forms reach the same row parser and are inventoried, not separately timed.
LTZ/NTZ are separate shapes because their executed types and localization differ.
The three interval families retain their distinct executed types. Public aliases
and planner coercions are grouped under the direct kernel they execute.
Explicit input formats and ordinary SQL statement parsing are out of scope.

Input text construction is appended to #1385's unchanged common native view;
the original date_text/decimal_text expressions and dimension stay intact.
Both planners use that same native source expression, so CONCAT/SUBSTRING/CASE
construction does not introduce separate Spark string-function work. Inspect
plans to confirm unused input columns are pruned and each shape reads only its
required source columns. Source-to-text work stays inside the query timers.
Each native reference is matched to its own shape. Isolated overheads are not
additive or exact parser CPU costs.

## Harness, fixture and build

The `--parsers-1389` selector in [execution.rs](../execution.rs) selects only the
new manifests and rejects concurrency/profile modes. It reuses the #1385
registration, timers, full stream drain, export sort, IPC comparator, counters
and memory tracker. Two additional benchmark-local schema labels allow actual
Duration(Microsecond) and calendar interval exports; the old schema helper and
all historical selectors retain their behavior. The
[controller](../parsers/run.py) imports the existing #1388 process/IPC helpers
and #1385 aggregation rather than duplicating an execution engine.

Preserve all 86 historical #1379/#1380/#1381/#1385 files byte-identically. The
coordinator's protected inventory is retained outside Git. No historical input,
schedule, receipt or report is rewritten.

Reuse the genuine 6,001,215-row SF1 snapshot from
[the #1380 fixture receipt](execution-1380-fixture.json), with six Parquet files,
80 stored columns and the virtual file_bucket column. Verify base manifest,
base log, derived log and every retained Parquet size/SHA-256; do not regenerate.

```text
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/base-sf1
/home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot
```

Use new evidence directories beneath
`/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389`.
The retained fixture, exports and scratch have a 64 GiB allowance, separate from
the serialized owned build cache. Use local Linux and serialize all Cargo/query
work. From the Spark workspace:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --bench execution \
  --message-format=json-render-diagnostics
```

Keep opt3/debug1, no RUSTFLAGS, target-CPU, LTO, codegen or #1381 CLI overrides.
Record command/CWD, verbose toolchain, source commit/product tree objects,
locked source/config hashes, compiler artifact path/profile and executable
SHA-256. Preserve an independent read-only executable seal. Resolve BINARY from
this build's compiler-artifact JSON. Before queries, the controller's `--check`
validates the fixed schedule, negative source-I/O checks, exact timestamp-unit
checks, aggregation and rejection of Python's disabled assertions.

## Complete untimed preflight

After protocol/source review and the coordinator's build release, run from the
repository root:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py --check
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py preflight BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/pr1-preflight-reduced
```

Export 22 complete results: six native-reference shapes in native/off/on, plus
the day-time shape and control in both Spark modes. Decode 12 native/Spark pairs
and two independent off/on pairs. Check all values, NULLs, multiplicities, rows
and executed SQL types. Require exactly 8,184 rows for each interval/control
export and all 6,001,215 rows for every date/timestamp/date_format export,
matching the independently verified complete-input count proof. Bind the
committed proof's hash and contents into both new receipts; verify fixture rows
and all new output counts without requiring the old external evidence files.
A completed decoded native/Spark row mismatch excludes the whole
shape; if every output has the same incorrect declared count, stop as an invalid
workload. This is an exact per-shape count check, not a partial-result allowance.
Require two output partitions, completed source counters, identical observed
paths and exact received/range byte totals for each semantic pair. The day-time
shape and control must also have identical source traffic.

Retain actual Arrow storage/unit/zone types separately from SQL types, IPC
checksums, every read range, operator counters, raw plans before export sorting,
and plan diffs. The existing comparator normalizes only string storage,
nullability and metadata; it never normalizes timestamp units/zones or interval
families. Actual expected type checks retain that distinction. The controller
only converts the microsecond display glyph to ASCII for matching its declared
type; the original actual-type strings remain in evidence.

A confirmed decoded value/type/count or native/Spark source-I/O mismatch leaves
the entire declared native shape untimed, with the mismatch reported. Never
silently turn it into a projection-control case or change product semantics.
Only the comparator's three exact exit-1 mismatch diagnostics prove a decoded
mismatch. Crashes, OOM, missing IPC, malformed records, incomplete comparisons or
counters and other operational errors stop preflight rather than excluding a
shape. A day-time/control off/on disagreement also stops preflight.

Write the complete result receipt to `parsers-1389-preflight.json.gz` and a
compact bound receipt under `../parsers/`. Bind the manifests, protocol,
inventory, source objects, shared helpers, binary and fixture. Freeze and commit
these before the exact-source timing rebuild. The coordinator independently
checks that the rebuilt binary is the sealed preflight binary.

## Fixed bounded timing and interpretation

The [schedule](../parsers/schedule.json) has three fresh process replicas per
configuration and per A/B arm, one warmup and three retained iterations each.
The first 60 processes collect same-binary noise. The remaining 66 collect main
timings, rotating configuration order. With all shapes eligible this gives 126
processes, 126 warmups and 378 retained samples. There are no profiles,
concurrency cases, pilot timings or repeated collections.

Keep #1385's affinity 2,3,4,5, two runtime workers, two target partitions, batch
8192, 2 GiB measured/4 GiB preflight pools, 12/16 GiB address-space caps and 8 GiB
spill limit. Preflight has a 1,800-second phase bound, 300 seconds per export and
180 seconds per comparison. Measurement has a 3,600-second phase bound and
180 seconds per process. Preserve partial evidence and stop on failure.

After the committed freeze, exact rebuild and coordinator timing release:

```sh
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py measure BINARY /home/hanbo/.local/share/delta-arrow-reader-review/measurements-2026-11-18/1380/delta-snapshot /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/before-once
python3 crates/delta-arrow-reader-spark-sql/benches/parsers/run.py summarize /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/before-once /home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1389/before-summary
```

Validate each retained sample against preflight rows, partitions and exact source
I/O. The receipt must match the manifest's exact 8,184 or 6,001,215 count for
every eligible shape/configuration. Use that shape's actual row denominator for
ns/row, and label the interval results as the fixed reduced SF1 subset.
Aggregate medians within each process, then the median of the three process
medians; retain their minimum, maximum and IQR. For native-reference shapes the
noise floor is the absolute difference between native A/B arm medians. In each
ANSI mode the target flag is strictly `spark_time - native_time > noise`.
Equality is false; the old 1.25x/50 ms threshold does not apply.

For the day-time shape, report each Spark mode's completion above its same-mode
plain input projection in ms and ns/row. Its noise is the absolute difference
between the A-arm and B-arm excesses, each formed by subtracting the matching
projection arm median. Retain both raw arm spreads and both absolute medians.
This is a query/control difference with a different output type, not a native
parity or native-target claim. Planning remains in both complete query times.

Report all eligible before values in ms and ns/row, noise, spread, exact flags
and every untimed exclusion. Retain planning, execution, first-batch, RSS/pool
and source metrics as supporting evidence. Describe source attribution without
claiming a measured Chumsky-only CPU breakdown.

PR 2 needs a separately reviewed protocol before its one after collection, using
these same affected shapes, inputs, settings and aggregation. No row path may
keep building a parser unnoticed: name any remaining path and why. PR 3 is
limited to ordinary STRING-to-TIMESTAMP, only if its post-PR-2 gap exceeds its
noise floor. It requires its own protocol before timing. Compare before/after
native/off/on results, show native drift and keep separate campaigns labeled.
If a bound remains unmet after those changes, report the gap/cause, leave #1389
open and stop. This PR authorizes attribution only.
