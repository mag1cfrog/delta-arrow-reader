# Pending RANGE work attribution

This protocol measures issue #1099 through SQL-planned `BoundedWindowAggExec`.
It makes no product change. The Python assembler adds observations only to
disposable source copies outside the checkout, following the #1390 assembly
and build-receipt pattern.

The audience is a reviewer deciding which bounded-window work justifies one
implementation attempt. Read the complete results and native exclusions before
running any region clocks or execution timings.

## Frozen workload

`manifest.json` fixes three input sizes (1024, 2048 and 4096), three batch sizes
(1, 32 and 1024), two SQL shapes and both Spark ANSI modes. The existing
`bounded_windows` benchmark sets its normal batch size to 1024. Neither query
has an outer sort. Both explicitly order the window by `k ASC NULLS FIRST`.
Captured complete rows are sorted by numeric `id` after execution.

The ordinary shape uses constant INT keys of zero and `UNBOUNDED PRECEDING AND
1 FOLLOWING`. The wrapped shape uses INT_MIN keys and `UNBOUNDED PRECEDING AND
1 PRECEDING`. A separate five-row reference has four zero keys followed by key
2. Its first four results must become available while physical input EOF is
still withheld.

The provider declares its actual source order. The driver checks that the SQL
plan contains one real bounded executor, rejects blocking sorts and batch
coalescing/repartitioning, and requires the physical ORDER expression to remain
a Column. It records the actual aggregate arguments, preserving `COUNT(*)`.
Source and executor observations check the delivered batch boundaries.

The native reference uses plain `SessionStateBuilder::new()` with default
features, target partitions set to one and the tested batch size. It does not
install `configure_spark_sql`. Both Spark modes and native run in the same
release binary.

## Run the independent reference

Run from the repository root. Every output directory below must be new and
outside the checkout. `evidence` refers to the external directory owned by this
review.

```sh
evidence=/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/1099
probe=crates/delta-arrow-reader-spark-sql/benches/pending_range_1099/run.py
env TZ=UTC PYTHONDONTWRITEBYTECODE=1 SPARK_LOCAL_IP=127.0.0.1 \
  JAVA_HOME=/home/hanbo/.cache/hermit/pkg/openjdk-21.0.8_9 \
  PYSPARK_PYTHON="$evidence/spark-reference-py312/bin/python" \
  "$evidence/spark-reference-py312/bin/python" "$probe" \
  capture "$evidence/spark-reference-full"
```

The capture reuses the maintained Python 3.12 protocol without changing it. It
records exact Spark 4.2.0 revision, effective settings, all five Field stages,
complete rows, error causes and phases, input and driver hashes, and owned JVM
cleanup. Fourteen SQL/ANSI references cover both shapes at all three sizes and
the progressive control. ANSI-on wrapped queries must fail at `collect` with
`ARITHMETIC_OVERFLOW`, while retaining all five schema observations.

## Freeze and assemble

Review and commit only the attribution files before assembly. Assembly refuses
a dirty checkout and checks that the declared baseline is an ancestor. It never
edits the checkout. Existing fixtures and product sources remain in the archived
tree and its content hash.

```sh
python3 "$probe" --check
python3 "$probe" assemble canonical "$evidence/source-canonical"
python3 "$probe" assemble observed "$evidence/source-observed"
python3 "$probe" build "$evidence/source-canonical" \
  /home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
  "$evidence/build-canonical"
python3 "$probe" build "$evidence/source-observed" \
  /home/hanbo/.cache/delta-arrow-reader/costs-168-2026-11-19 \
  "$evidence/build-observed"
```

The build uses Rust 1.96.0, `--locked --release`, opt level 3, debug level 1,
16 codegen units, no LTO, no incremental compilation, unwind panics, and disabled
debug assertions and overflow checks. Compiler wrappers and additional Rust
flags are rejected. Receipts identify the assembled source, release settings,
compiler-reported executable and copied binary hash. Files changed by the
overlay receive fresh timestamps before either build to avoid reuse of the
other source copy's dependency fingerprints.

## Establish eligibility before clocks

```sh
python3 "$probe" trace "$evidence/build-canonical/receipt.json" \
  "$evidence/build-observed/receipt.json" \
  "$evidence/spark-reference-full/spark.jsonl" "$evidence/trace"
```

This runs two counts-only processes, with 63 contexts each: 54 shape/size/batch/
configuration combinations and nine progressive controls. It checks complete
rows, names, types, nullability, metadata and error phases against Spark. The
observed and canonical builds must agree exactly.

`trace/eligibility.json` records the actual native comparison for each Spark
context. The 27 ordinary constant contexts must agree with native. Wrapped
contexts must stay excluded from all clocks because their complete native
result or error differs. Their comparison counts, copying and retention remain
part of the report. Counts-only observers do not read clocks.

Pending diagnostics retain the first eight and last eight deferrals. Each tuple
contains output index, retained length, committed start/end, candidate start/end,
and cumulative upper/lower comparisons. Other diagnostics have a fixed bound;
the full result rows are retained for exact comparison.

## Measure eligible work

```sh
python3 "$probe" measure "$evidence/trace/eligibility.json" \
  "$evidence/measure"
```

This schedules one observed-region process and three fresh canonical measure
processes. Each canonical process runs every eligible native and Spark context
once, with two warmups and seven execution samples. Arm order rotates across
cells and processes. All processes use CPU 2 and have a 300-second timeout.
Native controls share the exact executable with the Spark arms. There are six
required runtime processes per revision and one reserved optional profile slot.
Baseline and candidate together, plus the shared Spark capture, have a cap of
15 runtime processes. Build processes are separate from that execution schedule.

Wall samples include actual executor work, the gate coordination and output
collection. They exclude planning, input construction, result encoding and
printing. Each sample's complete result and Fields are checked after its timer.
Report all three fresh-process native distributions and Spark/native ratios;
an in-process median alone does not establish between-binary non-regression.

Coarse region clocks describe the observed build and include observer overhead.
Use the canonical build for wall comparisons. If coarse regions leave ownership
unresolved, use the reserved slot for one bounded symbolized profile of an
eligible constant context. Do not profile wrapped contexts or start a broad
profiling sweep.

## Read the attribution

`before_eof` and `after_eof` are cumulative snapshots. Their difference isolates
EOF work; peaks use the maximum rather than subtraction. Counts distinguish
upper and lower comparisons, target attempts, pending retries, committed rows,
argument/filter/order evaluation, accumulator updates, and three concat sites.
`trace/work.json` separates those phases and records which source-derived
baseline predictions match the observed counters.

For a baseline input of N rows in full batches of B rows, let K=N/B. Source
tracing predicts pending upper comparisons of `B*K*(K+1)/2`, followed by another
N comparisons when EOF permits the first result. Each growing input-buffer
concat site copies `B*(K*(K+1)/2-1)` row equivalents. Compare these predictions
with the actual counters at every size and batch setting.

The partition buffer and original-input buffer each retain N rows before EOF.
Copy counters report requested output row equivalents and cell equivalents at
each concat call. For this two-INT schema, each copied row represents eight
bytes of value payload. These counts do not claim unique resident bytes or
allocator traffic. Argument array rows also describe evaluation volume, not
necessarily allocation: a Column can return shared storage, while a scalar
argument can expand to an array.

Select the fix only after checking search, copying, argument work and retention.
A pending frontier must remain separate from members already applied to the
accumulator. Preserve pruning, upper-before-lower comparison order, lookahead,
completion and deferred errors. Lower comparison counts alone cannot establish
that growing-buffer copying or whole-buffer argument work has been resolved.

No product optimization, dependency changes, fixture replacements, builds or
query runs are part of the attribution-only source change.
