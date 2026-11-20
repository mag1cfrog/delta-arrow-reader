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
