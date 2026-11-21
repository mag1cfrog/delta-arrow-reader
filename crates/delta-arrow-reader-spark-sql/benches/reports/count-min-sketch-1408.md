# Sliding COUNT_MIN_SKETCH result (#1408)

Retraction avoids creating and filling a new sketch whenever the frame
loses a row. The candidate median fell by 57-62 ns per input row. All
retained ANSI-on timings improved. The ANSI-off ranges overlap by 3.7 ns,
so the predeclared range comparison does not establish a reduction in
both modes. This function has no native timing reference.

Times are median [min-max] ns per input row above projection, using
nine samples per arm from the same session.

| Path | Staging | Candidate | Median change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 881.921 [827.986-886.295] | 824.807 [768.247-831.706] | -57.113 |
| Spark ANSI on | 884.399 [881.155-895.875] | 822.836 [815.536-832.220] | -61.562 |

Insertion and retraction update the same buckets and total by opposite
amounts. Each result still serializes the seed-derived coefficients and
the full table. The original profile found that output construction also
accounts for substantial work. This run compares total row cost without
assigning that cost to phases of the candidate.

The preflight matched every Binary result by ID and the executed types
on the original million-row fixture, across builds and ANSI modes.
The 32 staging observations include 18 exact result sets and 14 argument
failures. They cover sliding ROWS and RANGE, growing, whole-partition and
empty frames, ordinary aggregation with and without input, parameter
limits and a filtered non-NULL row that leaves its partition's frame.
Independent frame replays cover integer, text and binary sketches across
multiple hash rows. All 638 local library and integration tests passed,
as did formatting and root Clippy.

The [protocol](../window_aggregates_1408/count-min-sketch-protocol.md) was
committed at `a6aac8f57fbe0dee84277ea7ec40ccabc2753c0c` before clocks. The
single Linux release session ran 2026-11-21 02:05:43 to 02:07:43 UTC.
All 12 processes completed, retaining 36 query/projection pairs.
Local evidence is under `costs-168-2026-11-20/1408/count-min-sketch`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `066c8d1504bf8d8900cd4f09131ad22b821b25ed` | `52e2f8feb1b77f66d4dc5065744d78a5ecd28be1c2a04e342a99c6c8b89caccb` |
| candidate | `e2dcd44a1085538d9515f786c779703e61bc2b4b` | `2996198a632a5bd495fce420e2405586c01f05dfcda41419341fd87d14165068` |
