# Attribute the remaining NULLIF execution cost (#620)

Measure Float64 normalization, equality and output validity, and row
selection for computed right operands. The [manifest](../nullif_620_round3/attribution.json)
pins the rebased release executable. The staging merge adds the rebase
protocol and report. All selected inputs already passed value and
executed-type checks in that release executable.

Linux denied both software events for perf recording. The original
profiler attempt collected zero benchmark samples. Use two existing
bench entry points and a scratch build for the split below.

## Observe normalization and kernel work

Apply the diagnostic-only patch below with `git apply --unidiff-zero` to the pinned release source and
build the same `nullif_620` target. Use the same Linux release settings
and toolchain as the rebase run. Record both binary identities. The
patch adds per-batch timers for normalization, Arrow equality and Arrow
output validity. It logs a row count and three durations immediately
after the timed regions. The shipped implementation stays untouched.

Run `nullif_620 measure nullif main BLOCK ELIGIBILITY` once per manifest
cell per block, for six blocks. Give each process one eligibility receipt
that excludes the other 71 NULLIF cell IDs from the
[frozen manifest](../nullif_620/manifest.json). Start odd blocks with the
direct-column cells. Start even blocks with computed-right cells.
Retain the harness JSON and the timer lines for every process. The
harness checks every result and includes one warmup and three retained
observations per mode. Each Spark plan has one partition. Attribute
records to ANSI mode using the harness's rotation and batch count.
Confirm that the row counts match the selected or full inputs before
assigning times. Drop warmups from the aggregate timers and retain them
in the raw trace.

The timer and per-batch output change overall execution cost. Report the
three timer regions individually as diagnostic observations. Use fresh,
unmodified release builds for the candidate decision. If any case
unexpectedly changes its result or executed type, exclude it and report
the difference.

## Observe computed-right selection

In the uninstrumented release executable run `nullif_620 attribute nullif`
once in each ANSI mode. The existing stage replay verifies
bit-for-bit result and type agreement with the actual physical
expression. For computed-right cells, the current evaluator still
uses the replay's selection, right evaluation and scatter steps.
Report those stage times at the four manifest anchors. Keep the
complete raw replay records and identify them as replay.
Use the comparable rebase receipt for the unprofiled native/Spark
gap, without subtracting stage medians from overall execution.

Run the 48 instrumented processes and both replay processes sequentially
on CPUs 2-5. Use a minimal process environment and UTC. Bound the
campaign to 15 minutes and each process to 90 seconds. A timeout, failed
process, unmatched row count or missing stage observation ends the
campaign. Retain available raw samples and an explicit failure record.
Commit all compressed samples with the report. The selection and
instrumented data answer different attribution questions.

## Scratch patch

The scratch commit is `704f12893480fb9525269e4e551e7532e0b0b940`.

```diff
diff --git a/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/scalar/conditional/nullif.rs b/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/scalar/conditional/nullif.rs
index 92f63063..8e013f86 100644
--- a/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/scalar/conditional/nullif.rs
+++ b/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-plan/src/function/scalar/conditional/nullif.rs
@@ -233,0 +234 @@ impl ScalarUDFImpl for SparkNullIf {
+        let normalize_started = std::time::Instant::now();
@@ -247 +248,6 @@ impl ScalarUDFImpl for SparkNullIf {
-        match apply_cmp(Operator::Eq, &input.args[0], &input.args[1])? {
+        let normalize_ns = normalize_started.elapsed().as_nanos();
+        let compare_started = std::time::Instant::now();
+        let equal = apply_cmp(Operator::Eq, &input.args[0], &input.args[1])?;
+        let compare_ns = compare_started.elapsed().as_nanos();
+        let validity_started = std::time::Instant::now();
+        let result = match equal {
@@ -257 +263,7 @@ impl ScalarUDFImpl for SparkNullIf {
-        }
+        };
+        let validity_ns = validity_started.elapsed().as_nanos();
+        eprintln!(
+            "NULLIF_PARTS {} {normalize_ns} {compare_ns} {validity_ns}",
+            input.number_rows
+        );
+        result
```
