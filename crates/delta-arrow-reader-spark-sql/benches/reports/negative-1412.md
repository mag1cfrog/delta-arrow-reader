# Vector negation of checked BIGINT arrays (#1412)

An ANSI-on BIGINT array uses Arrow wrapping negation when no raw slot
contains the minimum signed value. That value, scalar operands,
other widths and ANSI off retain their existing paths.

Both ANSI modes meet the measured native target. Every ANSI-on
process pair improved by 0.525 to 0.609 ns per input row. The
ANSI-off and native controls varied within the repeat floor when
comparing their arm medians.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| NEGATIVE | Native | 2.850 [2.813, 3.013] | 2.831 [2.770, 2.881] | 0.104 |
| NEGATIVE | ANSI off | 2.906 [2.870, 2.970] | 2.909 [2.857, 2.956] | 0.104 |
| NEGATIVE | ANSI on | 3.454 [3.399, 3.618] | 2.890 [2.830, 3.103] | 0.104 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Nine SQL controls cover BIGINT bounds, an active or masked minimum,
scalar and narrower integer overflow, decimal and floating operands,
NULL and empty input. Each matches rows, types or errors in both modes.
All 669 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/negative-protocol.md) was
committed at `115e7992cee24062451822a191064673ae23aacd` before clocks.
The Linux release session ran 2026-11-21T11:52:38Z to 2026-11-21T11:53:09Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1412/negative`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `87829367f9dcc4a2d8e9bee5525e92342aea2831` | `ac9cbca6d8597e8b61fd3a35dffcb293872047ff393d1eb32c3031df5b1689d1` |
| candidate | `f09a1b28a33e5a2973c71a8f8dedef8930b5e2fa` | `0e184d2a78d4bf0f4eb4554ed4cf4eb6f430faf4af85096a8fd74bd63e19362f` |
