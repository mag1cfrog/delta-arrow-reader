# NULLIF selection protocol (#620)

This developer reference specifies one local Linux release comparison and one
attributed optimization. The product baseline is staging
`e48dab25e421eca49086577b4fc0f683490f3d83`. The old NULLIF benchmark, all 86
protected historical assets and every #1388 asset remain unchanged. New inputs
are generated in memory from the [manifest](../nullif_620/manifest.json); no
Delta fixture, new dependency, frontend activation or other issue is involved.

## Source and caller inventory

Paths below are relative to `crates/delta-arrow-reader-spark-sql`.

| Path | Role and required boundary |
| --- | --- |
| `benches/nullif.rs` | Existing 262144-row DOUBLE benchmark; planning/execution are separate but only counts are checked. Preserve it. |
| `vendor/sail/crates/sail-plan/src/function/scalar/conditional.rs` | SQL NULLIF admission and ANSI/timezone construction. |
| `vendor/sail/crates/sail-plan/src/function/scalar/conditional/nullif.rs` | Comparability, original left result, temporary comparison conversions, float equality normalization, local VALUES variant. |
| `vendor/sail/crates/sail-plan/src/expression_errors.rs` | Constant/local comparison errors, LIMIT/guard reachability and local VALUES preparation. |
| `vendor/dependencies/datafusion-expr/src/udf.rs` | NULL-first contract, default false and wrapper delegation. Expression safety alone does not establish kernel conversion safety. |
| `vendor/dependencies/datafusion-physical-expr/src/scalar_function.rs` | `evaluate`/prefix facade reach `evaluate_legacy`; logical NULL selection, batch/first filtering, later operands, invocation, scatter. |
| `vendor/dependencies/datafusion-physical-expr-common/src/physical_expr.rs` | Shared selected evaluation, mask validation and failure-row remapping. |
| `vendor/dependencies/datafusion-physical-expr-common/src/utils.rs` | Shared scatter, including primitive and dictionary paths. |
| `vendor/dependencies/datafusion-functions/src/core/nullif.rs` | Same-binary native NULLIF control; comparable input coercion and native validity kernel. |
| `vendor/sail/crates/sail-function/src/scalar/math/spark_checked_arithmetic.rs` | NULL-first checked and TRY add/subtract/multiply. |
| `vendor/sail/crates/sail-function/src/scalar/datetime/spark_date_day_arithmetic.rs` | NULL-first date arithmetic; skipped string-day conversion must remain skipped. |
| `vendor/sail/crates/sail-function/src/scalar/predicate/spark_like.rs` | NULL-first LIKE and its pattern/error timing. |
| `vendor/sail/crates/sail-function/src/scalar/array/spark_array_subscript.rs` | NULL-first array subscript and required index failures. |

These are all five NULL-first UDF owners found by searching every declaration
and delegation of `short_circuits_on_null_first_arg`. A shared edit must inspect
all of them. A scatter edit has broader callers and requires a fresh scope
decision before product code changes. Existing projection helpers in the shared
evaluator are candidates for reuse only after measured attribution.

## Workload and exact results

The fixed NULLIF matrix has 262144 total rows, input batches of 1024, 8192 and
262144 rows, and four left validity patterns: no NULLs, every eleventh row NULL,
half NULL, and all NULL. The manifest defines every right value and NULL exactly.
Six expressions cover a column against zero/nonzero literals, two columns,
scalar left, computed left and computed right. Native, Spark ANSI off and Spark
ANSI on yield 216 cells. All values are ordinary nonnegative DOUBLE integers;
computed operands add one. Preserve the computed expressions in retained plans.

Native uses the native planner in the same Spark-configured SessionContext,
executable and dependency forks. Sessions use two runtime workers, two target
partitions and the selected batch size. Actual batches/partitions are recorded;
target partitions do not promise two physical output partitions.

Untimed preflight checks every result row against the independent input formula,
including NULL position, exact valid-value bits and executed Arrow DataType.
Retain full IPC and logical/physical fields plus physical plans. Decode complete
native/Spark and baseline/candidate IPC pairs with the benchmark's `compare`
command. Type/value mismatch exits 2 and excludes the entire corresponding
cell across configurations; malformed IPC or other execution failures stop
preflight. Field nullability and metadata differences are recorded and do not
exclude type/value-equivalent cells. Any baseline/candidate result mismatch
stops the candidate before timing.

The preflight also uses volatile identity operands to assert zero planning
evaluations and exactly one left evaluation per input row plus one right
evaluation per non-NULL left row, including all-NULL and empty input. Existing
correctness suites remain required: `conditional_expressions` (dictionary value
NULLs, key NULLs and all-NULL dictionaries; mixed operands; scalar and empty
controls), `comparison_coercion` (comparability, original result types, implicit
casts, local/provider/constant distinctions), `timestamp_precision`,
`generated_alias_metadata`, `like_escapes` and `like_arity`. The direct NULLIF
unit tests preserve unmatched NaN payload/zero bits and exact type on slices and
empty physical batches. Their original captured outcomes are immutable.

The candidate slice must add committed differential cases covering its admitted
domain and fallback boundaries. Record exact baseline/candidate executed types,
values/bits/NULLs, failures and planning/execution counters in both ANSI modes.
Include direct column/literal internal-cast failures on skipped and required
rows, local VALUES, provider, LIMIT 0 and guarded parents. Existing broad error
matchers or successful test names alone do not establish that equivalence.
Preserve the 3178 prior named outcomes (3175 passed, three ignored).

The manifest also predeclares a separate two-column Int32 add/subtract/multiply
matrix with the same sizes and validity patterns (108 cells). If attribution
selects a shared change affecting checked arithmetic, collect those baseline
controls before its candidate timing and compare native/non-NULL controls too.
Run the existing checked/TRY arithmetic, `date_day_arithmetic`,
`local_day_arithmetic`, LIKE and `array_access` correctness suites for affected
owners. Further caller timings require a concrete scope amendment before timing.

## Baseline attribution before candidate choice

The harness-only replay cites the frozen product baseline's
`scalar_function.rs:233-360` for argument fields/invocation and the NULL-first
sequence, and `datafusion-physical-expr-common/src/utils.rs:77` for scatter.
It uses the actual planned physical operands and UDF, rebasing source batches to
the projection's input schema. Separately time first evaluation, logical NULL
selection, batch filtering, first-argument selection/reuse, later argument
evaluation, kernel invocation and scatter. Assert full type/bit/NULL agreement
between replay and actual physical evaluation on every input batch.

For each ANSI mode, one process visits all 72 Spark cells with one warmup and
three retained stage passes. Retain stage samples, input batches, input rows and
selected rows. These diagnostic timings include timer overhead and replay
allocations. Their sums are not a serial wall-time decomposition and cannot
replace uninstrumented query execution. All-NULL and no-NULL cells retain their
real short-circuit paths. No product code is instrumented and no profiles are
scheduled. Choose one optimization only after examining this evidence.

## Build, source binding and preflight

Root reviews and commits this protocol, harness, controller and manifest before
any Cargo release build, query execution or timing. Root also authorizes each
bounded campaign. Serialize all local Cargo and query work. Use the sole owned
cache from the Spark workspace:

```sh
CARGO_TARGET_DIR=/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=1 \
cargo +1.96.0 build --locked --release --workspace --bench nullif_620 \
  --message-format=json-render-diagnostics
```

Keep opt3/debug1 and the full workspace feature graph. Do not add RUSTFLAGS,
CPU, LTO, codegen or CLI profile overrides. Record command/CWD, verbose toolchain,
source commit/index, lock/config/source hashes, resolved compiler artifact and a
separately sealed executable SHA-256. The controller records current source,
binary and protocol bindings plus host/kernel/CPU and affinity. Root verifies
that the exact committed source produced the sealed executable.

Evidence lives under
`/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/620`.
Keep one preflight IPC per cell/configuration; stage agreement does not need a
second IPC copy. Compare before compression, then gzip IPC and retain both raw
and compressed hashes. Never commit decoded rows or large IPC files. The 216
uncompressed outputs contain about 432 MiB of value buffers per campaign, plus
validity/framing. The controller checks actual retained evidence against the
1 GiB cap and compresses before retaining the next campaign. Stop on cap or
process failure; keep partial evidence. Each process is bounded to 180 seconds
and each campaign to 1800 seconds. Use affinity 2,3,4,5.
The cap covers retained input/output and log evidence under `620`, including
raw IPC immediately after emission and before comparisons. It excludes the
owned build cache and sealed executable copies. Store executable seals under
`/home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19/620-artifacts`, and
remove them with that owned cache after remote proof. Run Python without `-O`;
the controller refuses optimized mode because it uses validation assertions.

The controller is `benches/nullif_620/run.py`. Its commands are:

```text
check
preflight BINARY OUTPUT nullif [BASELINE_RECEIPT]
attribute BINARY OUTPUT nullif
measure BINARY OUTPUT RECEIPT
summarize OUTPUT
```

`OUTPUT` must be a new directory under the owned evidence directory. Use
`checked` instead of `nullif` only for the predeclared shared-caller controls.
Preflight writes `receipt.json`; measurement verifies its source/binary/input
bindings. A receipt-only commit may change HEAD/index while Rust/build inputs
and the sealed binary remain identical; record both identities. Source, binary,
manifest, harness, controller or protocol changes require a new exact preflight.
Root retains the corresponding build/toolchain/seal record alongside these
controller metadata. The controller never builds or retries a binary.

Run untimed baseline preflight and commit its compact receipt, then rebuild and
verify the final exact binary before baseline execution and attribution. After
one attributed candidate is independently reviewed, run required correctness
checks and exact old/new preflight; commit its compact receipt, rebuild/seal the
final source and collect candidate timing once. No valid timing is repeated.

## Fixed measurement and report

Both campaigns use the manifest's same six fresh process schedule: three main
replicas and three native B replicas, with the second pair reversed. Every cell
has one warmup and three retained samples. Main replicas rotate mode order.
Planning covers only planning plus physical-plan creation. Execution covers
only `collect` of that plan. Output/type checks, registration, export and stage
replay are outside these timers. Retain raw planning/execution samples and call,
row and batch counts. Each retained sample must match the preflight formula.

Report median of process medians, min/max and IQR, ms and ns/row for native/off/on,
before and after. Noise is the absolute difference between native A and native
B arm medians within the same binary and campaign under the same aggregation.
The per-ANSI flag is strictly `Spark execution - native execution > noise`;
equality is false. Baseline/candidate drift is reported separately and must not
be relabeled as this same-binary noise. Sequential campaigns are not paired
causal samples. Report planning separately from execution.

Publish compact raw samples, metadata, exclusions and stage evidence with the
report, along with exact source/build identities and native timing drift. For
existing dependency-fork edits, append accurate provenance to `UPSTREAM.md` and
show unchanged same-binary native outputs and native timing controls. If the
target remains missed, report the remaining gap and supported cause, keep #620
open and move on. A small cost does not replace the required optimization. No
second candidate or extra timing/profile round is authorized by this protocol.
