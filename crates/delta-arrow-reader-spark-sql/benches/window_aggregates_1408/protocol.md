# Sliding aggregate attribution (#1408)

Use the 18 unchanged #1391 statements in [manifest.json](manifest.json) on
its original 1,000,000 seeded rows. The manifest pins the staging source
before measurement. Build on local Linux with Rust 1.96.0, locked dependencies,
release optimization and debug level 1. Keep the original 8,192 batch size,
single partition and worker, 12 GiB process cap and CPUs 2-5.

Build an ordinary timing driver and a scratch-only phase driver from the same
source. The scratch observer records complete preflight outputs from both.
Require all input rows, successful execution, the executed Arrow types, and
exact values for each function, including the bit patterns of floating results. For paired functions, compare with the native
results. For Spark-only functions, require exact agreement across the two
builds and ANSI modes. Exclude a function from timing if its results or types
differ. Save each physical plan so the report can identify the executed
accumulator or evaluator and each output conversion.

## Clocks and controls

Run the original query and its paired projection. Use one warmup and three
retained executions per process, with three fresh-process replicas for
ordinary timings. Rotate sorted case IDs left by the replica number times
six. On alternating replicas reverse the within-case configuration order.
For the eight paired cases, run the original native A/A control with two
ordinary processes per replica, alternating their label order. The noise
floor uses the #1391 rule for projection-subtracted ns per input row.
For the ten Spark-only functions the ordinary timings cover both ANSI modes.

Use one phase profile process per configuration for each paired case and one
with ANSI off for each Spark-only case. Profile phases are independent of the
ordinary wall measurements. A phase clock wraps the original operation and
sums its time and calls. Creation includes the accumulator or standard window
evaluator factory. Frame replay includes selecting and passing its rows back
to an accumulator after a reset. New additions to an existing frame,
retractions, raw batch updates, and partial-state merges are recorded
separately. Evaluation includes the accumulator state or scalar production,
or the standard window evaluator for the native first/last value cases.
Output conversion includes scalar-to-Arrow materialization and the final
list-to-string or Utf8 cast where it occurs. The remainder includes sort,
window bounds, row result collection and probe overhead.

Each driver retains the original 120-second case limit. The parent gives the
case process 130 seconds including startup, preflight up to 300 seconds, and
the session at most four hours. Keep each result or failure and do not repeat
completed timing processes. Require a successful process exit and the
original result row count and types for its samples.

The report identifies which implementation incurred each cost. For each
Spark-only function, the code trace and phase profile state what can be
reused independently of frame contents and whether removing a row can retain
exact state and output bits. The report records phases that the measurements
cannot separate. No product or dependency change belongs in this PR.
