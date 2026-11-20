# Classifying the 42 function cases excluded by #1391

[#1406](https://github.com/mag1cfrog/delta-arrow-reader/issues/1406) starts on
staging `6a520463`. The [case manifest](../functions_1406/manifest.json) identifies
every exclusion from the frozen #1391 receipt and retains its original SQL. The
source fixture, harness, manifest, receipt and reports from #1391 stay intact.

## Check results before timing

The reduced driver uses the #1391 generator and changes only the input IDs. The
reference slice contains six rows from each of the first 16 groups. The IDs
are `round * 4096 + group` for rounds 0 through 5. Each group exercises the
five-row window frame. Group 0 contains only null payloads. The committed
[`input.json`](../functions_1406/input.json) holds the resulting columns used in
Spark. The same SQL runs in DAR and Spark 4.2.0 with UTC and both ANSI settings.
The capture helper records Spark's exact version, source inputs and executed
result schema. Spark constructs the original array and timestamp types in a view
over the primitive capture inputs.

Compare complete row multisets, keeping duplicates. Compare executed types
recursively, preserving field names and dropping nullable flags and metadata for
the compatibility decision. Keep those flags and metadata in the raw records.
Record Spark rejections separately. Only functions with values and types that
match Spark in both modes enter the Spark-only timing manifest. Record opposing
values or executed fields for any mismatch. This classification applies to the
committed reduced input.

The stock `map_entries` probe uses DataFusion from the root workspace and the
first two entries of the same map fixture. Use the registry's
`nested_expressions` feature to register `map_entries` for the stock comparison.
Record the locked dependency sources and whether the statement panics.
AES column support is tracked in #1405.

## Measure on Linux

Use Rust 1.96.0 release builds with the release settings of #1391. Run one driver
at a time on CPUs 2 through 5, with a 12 GiB address-space cap. Run eligibility
checks and all builds before timing. Write and commit the eligibility manifest
before the clocks start. Use the original `functions` driver and one million rows
for cases that match Spark. Run three fresh processes per ANSI setting. Rotate
case order by the replica index and reverse ANSI order in the middle replica.
Each process keeps three executions after one warmup. Report execution nanoseconds
per input row and the increment above the plain projection as medians of process
medians. Record timeouts as incomplete outcomes.

The bitmap window clocks use 512 and 2,048 input rows. Use eight rounds in 64
and 256 groups, respectively, with the same ID formula. Use the same three
replicas per case, size and ANSI mode, each in its own process. Separately count
the binary payload bytes and the number of values produced by the window and by
one aggregate over the same input. This compares payload growth against output
frame count with the input row count held fixed. Keep process peak RSS from each
timing run. Report payload bytes separately from process memory.

Use a 65-minute outer session bound. The old exact per-case execution bound of
120 seconds applies to the full `measure-set` driver, with a separate 600-second
subprocess bound. The reduced driver has a 180-second subprocess bound.

## Commands

From the Spark workspace, build `functions` with
`cargo +1.96.0 build --locked --release --bench functions`, using
`CARGO_PROFILE_RELEASE_DEBUG=1` and `CARGO_BUILD_JOBS=2`. The reduced build
generates a temporary benchmark entry and removes it after compiling.

```sh
python3 benches/functions_1406/run.py check
python3 benches/functions_1406/run.py build TARGET REDUCED_BUILD
python3 benches/functions_1406/run.py input REDUCED_BINARY RAW_INPUT_OUTPUT
TZ=UTC JAVA_HOME=JDK_21 SPARK_42_PYTHON benches/functions_1406/capture_spark.py SPARK_CAPTURE
python3 benches/functions_1406/classify.py REDUCED_BINARY SPARK_CAPTURE CLASSIFICATION_OUTPUT
timeout 65m python3 benches/functions_1406/measure.py \
  FULL_BINARY REDUCED_BINARY COMMITTED_TIMING_MANIFEST MEASUREMENT_OUTPUT
```
