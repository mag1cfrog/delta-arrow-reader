# Attribute ordinary UTC TIMESTAMP LTZ on Apple Silicon (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1389-ltz-profile.json) pins staging and the [phase
probe](1389-ltz-profile.rs). Use the existing [test probe
adapter](date_format_profile.py) to append it to the original Sail
module in a scratch checkout and seal a native release built with
Rust 1.96.0. Restore the scratch source after sealing.

Generate 8,192 ordinary timestamps in the SF1 date range at
12:34:56 UTC and set every 97th input to NULL. Compare the Spark
array conversion in ANSI off and on with the Arrow cast on the
same Utf8View input. Require exact UTC microseconds, NULLs and
executed type before timing. Verify that the ordinary field parser
produces the expected naive timestamp and UTC localization agrees
with direct UTC conversion on every non-NULL row.

Measure five phases. The first pair compares the Spark and native
array casts. The second pair isolates the Spark localization step
and direct UTC conversion on matching preparsed inputs. The fifth
phase records the Spark field parser cost for context. It lacks
a separate native field-only comparison.

Use the [native session controls](1389-borrowed-tokens.md). The
adapter verifies QoS in the parent and on the timed test thread.
Run two staging arms in each of nine reversed blocks. Each process
warms each phase with one batch and retains nine rotations of 32
batches. Bound a process to 60 seconds and the session to 20 minutes.
Use the median of the within-process phase differences and the
A/A floor from the two arms' medians and half-ranges.

The probe identifies repeated UTC localization as a target if the
complete Spark array excess and the isolated localization cost
both exceed their floors. Report the isolated share of the array
gap. Incomplete observations are inconclusive. The probe omits
planning and source I/O. The full SF1 query comparison decides
any product change.
