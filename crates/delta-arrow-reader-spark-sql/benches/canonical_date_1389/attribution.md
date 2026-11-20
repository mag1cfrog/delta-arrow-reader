# Attribute the ten-byte DATE cost (#1389)

Probe staging `9c2e5855e9cc6d704c562936e9e517c3f4cd32cb`. Reuse the
[unpadded-date guard probe](../date_1389/sample_guard.rs). Its scratch
variant replaces the seven input literals with `2020-01-01` through
`2020-01-07` and calls `has_padded_date_shape` in place of
`has_unpadded_date_shape`. Build the padded guard from the exact staging
`bytes.len() == 10 && bytes.iter().enumerate().all(...)` expression
in `SparkDate::string_to_date32`, up to the Arrow parse call.
These are valid members of #1391's `date_s` input corpus.

Link Arrow from the staging-equivalent locked release build. Compile with
Rust 1.96.0, optimization level 3 and debuginfo 1. Run on Linux CPUs 2-5
with 4 GiB and a 60-second limit. Use the same warmup and seven alternating
pairs of 131,072 non-null rows as the original probe. Compare exact dates
before timing. Count the byte guard as a measured cost if all seven paired
differences are positive and the median difference exceeds its median
absolute deviation. This excludes array construction and planning.
A separate whole-query comparison will decide whether the change reaches
native noise on the one-million-row fixture.

## Complete array controls

The small padded guard probe had two negative differences and does not
identify the source of the full cost. Use the original UDF and Arrow cast
in one release library test. Append `attribute.rs` in a disposable staging
checkout. For the controlled row loop, copy staging's
`SparkDate::string_to_date32` to `string_to_date32_without_shape_check`
in the same `impl`. In that copy only, remove the canonical shape guard
before the first `Date32Type::parse` call. The seven known-valid complete
input dates guarantee that all calls use this branch. Include one-in-sixteen
NULLs as in #1391. Compare original UDF, guarded row loop, the same loop with
the guard removed, and the existing Arrow cast. Require exact Date32 values
and NULLs before clocks. This diagnostic copy is never a product candidate.

Use the same release workspace build settings and CPU limit as the other
phase attribution tests. Allow one 60-second test execution with 4 GiB
address space. Warm one batch per case, then seven rotations of 64 batches
of 8,192 rows. Report median and MAD for each case and paired differences.
Use the same direction and median-above-MAD rule per paired difference.
This allows a distinction between date shape checks and row/array handling
without relying on a difference between separate timing campaigns.
