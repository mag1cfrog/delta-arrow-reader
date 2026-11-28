# Attribute unpadded and canonical DATE casts on Apple Silicon (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1389-date-cast-profile.json) pins staging and the
[phase probe](1389-date-cast-profile.rs). Use the existing
[test probe adapter](date_format_profile.py) to build and seal the
native release with Rust 1.96.0 in a detached scratch checkout.
The adapter restores the Sail source after the build.

Use two 8,192-row Utf8View arrays. The unpadded array is in the
SF1 year range and has one NULL every 97 rows. The canonical
array contains ten-byte dates in the same year range. Require the
safe and strict Spark conversions and both diagnostic row loops
to agree exactly with the Arrow Date32 cast before timing.

Each shape has four phases. Compare the full Spark and Arrow
casts. Compare the existing guarded Spark row parser with a row
loop that calls the same Arrow parser on these already-validated
inputs. This isolates the guarded dispatch, including its
validation and error handling. Compare the latter row loop with
the full Arrow cast to test collection overhead with the same
row parser. Report the three phase gaps and their floors
individually. These probes omit planning and source I/O.

Follow the [Mac session controls](1389-borrowed-tokens.md) and the
same two-arm staging schedule as the
[UTC timestamp probe](1389-ltz-profile.md). Each of nine reversed
blocks retains two processes, each with nine rotations of 32
batches after the warmup. A process has 60 seconds and the
session has 20 minutes. Compare the paired phase medians with
the floor from the two A/A arm medians and half-ranges. A cause
requires an above-floor array excess and an above-floor
component gap. Incomplete observations are inconclusive.
