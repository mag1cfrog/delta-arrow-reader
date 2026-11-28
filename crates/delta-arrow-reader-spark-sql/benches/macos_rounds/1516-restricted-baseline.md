# Restricted #1516 baseline on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [protocol](1516-restricted-baseline.json) pins staging at the
start of #1516 and the [frozen restricted cases](../functions_1516/manifest.json).
The original #1391 statements, projections and #1516 restrictions
stay byte-identical. The [native adapter](restricted_1516.py) applies
the existing restricted fixture and exact result encoder overlays in
a detached scratch checkout. It builds a Rust 1.96.0 release and
restores the scratch files. It applies the same thread controls as
the [native baseline](../macos_baseline/protocol.md).

Construct the restricted batches from the #1391 seeded million
rows before planning or timing, then register the same batches in
native and Spark. Require exact decoded rows, executed types and
full source consumption for each of the two ANSI comparisons.
Keep each exclusion in the receipt and report. Review the source
for cases without an eligible native comparison, including all
three bitmap windows. The [maintainer's decision](https://github.com/mag1cfrog/delta-arrow-reader/issues/1516#issuecomment-7154429902)
carries forward the accepted Mac REGEXP_REPLACE judgment and
omits it from the session.

Use mains power, High Power mode and `caffeinate`. The main thread
and all benchmark workers set and verify user-interactive QoS.
CPU pinning, the Linux address-space limit and peak RSS are omitted.
Check for competing builds and benchmarks before each process.

Use the fixed #1560 function schedule with nine blocks and nine
retained query/projection pairs per process. Each block has a
primary native result, both Spark modes and two native A/A arms
in the same staging binary. Reverse case and arm order between
successive blocks. Cap a timing process at 130 seconds and the
session at three hours. Give preflight a separate hour with at
most five minutes per process.

Express the cost in ns per actual input row above projection.
The paired gap is the median of block Spark-minus-native
differences. The floor is the largest of the absolute difference
of the two A/A medians and both A/A half-ranges. A positive gap
above the floor enters the attribution list. A gap within this
floor is not resolvable on Apple Silicon. A failed process
excludes the remaining slots for its case and retains its raw
records. This session measures staging only.
