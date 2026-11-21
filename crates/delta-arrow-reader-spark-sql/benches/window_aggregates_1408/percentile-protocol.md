# Unweighted DOUBLE percentile comparison (#1408)

Run the percentile window in [percentile.json](percentile.json) using the
[matched comparison](string-aggregation-protocol.md#schedule-and-pass-rule)
and the unchanged million-row fixture in [manifest.json](manifest.json).
Both builds use the same projection, affinity and release flags. The
staging executable predates benchmark and report changes only.

Before timing, require exact rows by ID and executed types in both ANSI
modes, on staging and candidate and against native QUANTILE_CONT. Compare
floating values by their bits. Exclude the function if a result or type
differs. The differential tests use explicit unit frequency to keep the
original cast and frequency path. They cover bitwise results with signed
zeros, infinities and NaN payloads, NULL values, empty input, ordinary
aggregation, and sliding, growing, whole-partition and empty frames.

The target requires the candidate median in each ANSI mode to stay within
its candidate native median plus the native process-median A/A floor.
Report median and range across nine retained samples per arm in ns per
input row above projection. Record any remaining gap after the one change.
