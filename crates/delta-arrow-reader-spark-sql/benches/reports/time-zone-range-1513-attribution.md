# Time zone range panics (#1513)

At staging commit `689be9bc`, a timestamp can fit Arrow's signed 64-bit
microsecond type while its local date-time lies outside Chrono's range.
The first and last Chrono ticks are `-8334601228800000000` and
`8210266876799999999`. Two operations panicked when an offset pushed a
date-time across one of those limits.

| Operation | Calls with a reproduced panic | Panic site |
| --- | --- | --- |
| Project an instant into a target zone | CONVERT_TIMEZONE, FROM_UTC_TIMESTAMP, MAKE_TIMESTAMP and TRY_MAKE_TIMESTAMP and their LTZ aliases with an explicit zone | Chrono `DateTime::naive_local` |
| Read an NTZ wall clock in a zone | FROM_UTC_TIMESTAMP and TO_UTC_TIMESTAMP with an NTZ argument and fixed session offset, MAKE_TIMESTAMP and TRY_MAKE_TIMESTAMP and their LTZ aliases with a session zone | Arrow's `adjust_timestamp_to_timezone` subtracts the offset from a Chrono date-time |

For an instant within Chrono's range, the projection panics when the
instant plus the target zone's offset falls outside that range. In
CONVERT_TIMEZONE, the instant is the original timestamp after resolving
the source zone. When attaching a time zone to a wall clock, the
subtraction panics if there is one resolved offset and the resulting
instant falls outside the range. All tested shifts fit in the signed
64-bit timestamp type.
[Shared Sail conversion](../../vendor/sail/crates/sail-function/src/scalar/datetime/convert_tz.rs),
[Arrow cast](../../vendor/dependencies/arrow-cast/src/cast/mod.rs).

The original unit-test grid exposed 38 panics. The wider sweep probed
both range ends and the one-microsecond cutoffs for every Chrono zone in
the locked database and representative fixed offsets, including the
allowed extremes and one-second offsets. It observed 2,288 more
panics, with 2,318 distinct input tuples across the two grids. The
[calendar inventory](../../tests/fixtures/time-zone-range-1513/dar-calendar-panics.jsonl)
records the affected zone names, exact ticks, and conversion modes.

The isolated public SQL executable exited 101 on both panic sites.
A literal CONVERT_TIMEZONE panicked while building the physical plan.
Column arguments panicked during collection. Named session cases include
Paris, Los Angeles, Adak and ROC. TO_UTC_TIMESTAMP with an
already tagged timestamp uses UTC as the projection zone and did not
reproduce either panic. Its existing failure for one source offset
at the lower limit remains recorded.
[Public SQL observations](../../tests/fixtures/time-zone-range-1513/dar-public.jsonl).

## Spark reference

The Spark 4.2.0 capture recorded the outcome of every calendar and public
SQL case in both ANSI modes. The capture reads signed microseconds and executed types from
Spark's JVM results, which can represent these years. UTC-to-zone
projection used the same offset as Chrono for every valid name tested.
Spark raised INVALID_TIMEZONE for ROC, which Chrono accepts. The
calendar capture composes the two Spark UTC functions to reference the
internal non-classic operation when both zones vary.
[Calendar results](../../tests/fixtures/time-zone-range-1513/spark-outcomes.jsonl),
[public SQL results](../../tests/fixtures/time-zone-range-1513/spark-public.jsonl).

To reproduce the references, run `capture_spark.py OUTPUT` and
`capture_public.py OUTPUT` with Python 3.12,
Java 21 and PySpark 4.2.0. The scripts own and shut down their JVMs.
The public capture enables Spark's TIME type for the DATE-and-TIME
constructors. The fixture directory contains both capture scripts,
the original-grid inventory test body and the isolated Rust SQL
observer. Use those Rust probes on the pinned baseline.
