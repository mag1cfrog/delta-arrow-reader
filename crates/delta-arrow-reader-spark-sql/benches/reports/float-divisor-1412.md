# Direct zero-divisor check trial (#1412)

The checked division and remainder helper scans array divisors
and checks the validity of both operands at zero slots. Scalar
broadcast and length handling follow the existing paths.

Staging keeps the Boolean-mask check. The ANSI-on median rose by
0.013 ns per input row in the trial. That change is inside the
session's 0.032 ns native repeat floor. The retained staging path
was 0.749 ns above its same-session native reference with ANSI off
and 0.672 ns above with ANSI on.

## Candidate gaps

- Divide with ANSI off remains 0.768 ns per row above native. Its floor is 0.032 ns.
- Divide with ANSI on remains 0.693 ns per row above native. Its floor is 0.032 ns.

ANSI on still needs a validity-aware zero check. ANSI off uses its
existing NULLIF to supply NULL for zero divisors.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| Divide | Native | 4.325 [4.275, 4.678] | 4.316 [4.271, 4.580] | 0.032 |
| Divide | ANSI off | 5.074 [4.931, 5.175] | 5.084 [4.932, 5.146] | 0.032 |
| Divide | ANSI on | 4.997 [4.832, 5.199] | 5.010 [4.794, 5.165] | 0.032 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Nine SQL controls compare exact values, executed types and errors in
both ANSI modes. They cover signed zeros, zeros with a NULL numerator,
NULL divisors, NaN and infinities, scalars, empty input and
Float32/Float64 remainder through the shared checker.
All 669 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/float-divisor-protocol.md) was
committed at `8e3df84c64a6d3eb1c6ff6e0429f0a7ae90e3235` before clocks.
The Linux release session ran 2026-11-21T11:33:55Z to 2026-11-21T11:34:26Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1412/float-divisor`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `87829367f9dcc4a2d8e9bee5525e92342aea2831` | `ac9cbca6d8597e8b61fd3a35dffcb293872047ff393d1eb32c3031df5b1689d1` |
| candidate | `3822f3f6603c4a2b1ac8c73495efd1f39a271df6` | `5e031d2349ade5fbc3f099f5f561b496aca6787aacaf5868978be2b6d744aaae` |

The [candidate branch](https://github.com/mag1cfrog/delta-arrow-reader/tree/perf/1412-find-zero-divisors-without-a-mask) preserves the implementation and protocol.
