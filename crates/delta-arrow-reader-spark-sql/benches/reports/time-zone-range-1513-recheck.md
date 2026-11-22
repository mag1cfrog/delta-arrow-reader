# Time zone range recheck (#1513)

The correction remains unmerged. It fixes the reproduced boundary panics,
and 10 ordinary Spark conversion cells failed the performance gate with
changed execution code. The two flagged controls used the same routines
and state layouts in both builds.

## Correctness

- Staging `d39ce212` and candidate `d9623a45` completed the [protocol](time-zone-range-1513-recheck-protocol.md)
  on Linux with Rust 1.96.0 release builds. The candidate uses the exact
  product and test patch archived in the [first report](time-zone-range-1513.md).
- Full Linux Spark workspace tests, formatting and root-crate Clippy passed.
  The differential tests cover 2,318 calendar inputs in each ANSI mode.
  In the public Spark 4.2.0 comparisons, 42 former panics returned the
  reference value and executed type, six became errors, six prior errors
  were preserved and two prior successes matched. Spark rejects the ROC
  session zone during configuration.
- All 16 timing statements passed preflight. The 42 Spark and native
  configurations kept identical rows and executed types with the
  one-million-row input guard. There were no exclusions.

## What changed on ordinary inputs

For `FROM_UTC_TIMESTAMP` and `TO_UTC_TIMESTAMP`, the candidate plans a
tagged result from `ConvertTz` in place of the outer cast. The calendar
path uses `project_local` to check the offset and return a calendar or
boundary result. `FROM_UTC_TIMESTAMP` projects the instant into Los
Angeles. `TO_UTC_TIMESTAMP` projects into UTC first, then resolves that
wall clock in Los Angeles. The loop propagates the new fallible result.
The old calendar call returned an optional local datetime. The ordinary
tagged output also takes the internal strict timestamp cast. The full
recovery pass runs only if that cast fails.

For the fixed-offset cases, the old and new loops can both add an offset
directly to in-range values. The new `convert_singleton_zones` loop
collects fallible results, including the path for values near the limits.
This changed the emitted loop even for the ordinary values in the timing
fixture. The `ConvertTz` state grew from 80 to 112 bytes and the lazy
variant grew from 128 to 160 bytes at unchanged 8-byte alignment. The
lazy and eager entry bodies changed, as did the shared singleton-zone
body (842 to 1,264 bytes). The timings measure the combined change for
each statement.

## Controls

The physical plans for the two flagged controls were identical across
builds. The native `TO_CHAR(FROM_UNIXTIME(...))` used the same normalized
`ToCharFunc`, `FromUnixtimeFunc` and `CastExpr` bodies. Their state
size/alignment pairs were 96/8, 72/8 and 152/8 in both binaries. Relocated
call targets, literal loads and the formatting records checked out.

The ANSI-on `TO_UNIX_TIMESTAMP` timestamp-input control used the same
`SparkUnixTimestamp` body and columnar cast path, with 96/8 UDF state in
both binaries. Its relocated jump table retained all 27 destinations.
These two flags are build-layout shifts under the control rule.

## Timing

The [compressed archive](time-zone-range-1513-recheck-samples.jsonl.gz)
contains all 1,512 samples across 36 processes, the release identities,
and the preflight and gate calculations. The previous 48 samples were
kept in their original archive. Times below are above projection in
nanoseconds per input row. Paired change is the median of within-block
differences. A/A noise is the largest within-block staging self-pair
difference. The two layout shifts passed after instruction and layout
comparison.

| Statement | Mode | Staging | Candidate | Paired change | A/A noise | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `time` | native | 10.210 | 10.237 | 0.031 | 0.038 | pass |
| `time` | spark-off | 14.123 | 13.975 | -0.102 | 1.105 | pass |
| `time` | spark-on | 13.998 | 13.989 | -0.022 | 0.057 | pass |
| `convert_timezone` | native | 0.081 | 0.077 | -0.003 | 0.008 | pass |
| `convert_timezone` | spark-off | 39.923 | 40.134 | 0.301 | 0.616 | pass |
| `convert_timezone` | spark-on | 40.098 | 39.742 | -0.282 | 0.579 | pass |
| `convert_timezone.los_angeles` | spark-off | 137.045 | 133.883 | -3.136 | 11.755 | pass |
| `convert_timezone.los_angeles` | spark-on | 136.100 | 126.940 | -7.681 | 16.693 | pass |
| `convert_timezone.offset` | spark-off | 41.050 | 43.785 | 2.874 | 2.001 | slower |
| `convert_timezone.offset` | spark-on | 41.078 | 43.339 | 2.457 | 2.173 | slower |
| `from_unixtime` | native | 211.670 | 215.278 | 4.239 | 3.806 | layout shift |
| `from_unixtime` | spark-off | 282.786 | 279.119 | -3.667 | 9.318 | pass |
| `from_unixtime` | spark-on | 282.056 | 279.698 | -3.066 | 2.289 | pass |
| `from_utc_timestamp` | native | 0.050 | 0.052 | 0.001 | 0.008 | pass |
| `from_utc_timestamp` | spark-off | 39.174 | 39.065 | -0.146 | 0.127 | pass |
| `from_utc_timestamp` | spark-on | 39.217 | 39.079 | -0.161 | 1.382 | pass |
| `from_utc_timestamp.los_angeles` | spark-off | 109.449 | 115.373 | 5.928 | 0.924 | slower |
| `from_utc_timestamp.los_angeles` | spark-on | 109.244 | 115.795 | 6.362 | 0.280 | slower |
| `from_utc_timestamp.offset` | spark-off | 42.694 | 43.649 | 0.949 | 0.168 | slower |
| `from_utc_timestamp.offset` | spark-on | 42.658 | 43.951 | 1.303 | 0.178 | slower |
| `make_time` | native | 5.100 | 5.143 | 0.034 | 0.043 | pass |
| `make_time` | spark-off | 4.419 | 4.358 | -0.080 | 0.319 | pass |
| `make_time` | spark-on | 4.464 | 4.366 | -0.042 | 0.218 | pass |
| `to_time` | native | 86.967 | 87.255 | 0.434 | 0.704 | pass |
| `to_time` | spark-off | 14.028 | 14.002 | -0.030 | 0.051 | pass |
| `to_time` | spark-on | 13.999 | 13.955 | -0.037 | 0.262 | pass |
| `to_unix_timestamp` | native | 50.500 | 50.861 | 0.387 | 0.551 | pass |
| `to_unix_timestamp` | spark-off | 16.436 | 16.393 | -0.043 | 0.226 | pass |
| `to_unix_timestamp` | spark-on | 16.369 | 16.415 | 0.055 | 0.026 | layout shift |
| `to_utc_timestamp` | native | 0.050 | 0.053 | 0.003 | 0.008 | pass |
| `to_utc_timestamp` | spark-off | 39.178 | 38.990 | -0.181 | 0.311 | pass |
| `to_utc_timestamp` | spark-on | 39.044 | 39.105 | 0.046 | 0.226 | pass |
| `to_utc_timestamp.los_angeles` | spark-off | 112.705 | 119.044 | 6.434 | 0.741 | slower |
| `to_utc_timestamp.los_angeles` | spark-on | 112.446 | 119.496 | 7.069 | 0.963 | slower |
| `to_utc_timestamp.offset` | spark-off | 42.691 | 43.683 | 0.998 | 0.148 | slower |
| `to_utc_timestamp.offset` | spark-on | 42.646 | 43.702 | 1.041 | 0.140 | slower |
| `try_to_time` | native | 86.960 | 87.284 | 0.377 | 0.584 | pass |
| `try_to_time` | spark-off | 14.011 | 13.960 | -0.071 | 0.093 | pass |
| `try_to_time` | spark-on | 13.970 | 13.960 | -0.007 | 0.087 | pass |
| `unix_timestamp` | native | 50.394 | 50.482 | 0.131 | 0.560 | pass |
| `unix_timestamp` | spark-off | 16.381 | 16.366 | -0.023 | 0.061 | pass |
| `unix_timestamp` | spark-on | 16.432 | 16.432 | 0.056 | 0.306 | pass |
