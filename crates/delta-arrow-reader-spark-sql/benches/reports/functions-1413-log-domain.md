# Logarithm input validity (#1413)

The mask shares each Double values buffer and retains the original comparison and logarithm kernel. The matched retention rule passes in both ANSI modes for LN, LOG, LOG10, LOG1P, LOG2. No comparison meets the protocol's regression rule.

## Remaining gaps

- LN with ANSI off remains 0.649 ns per row above native. Its floor is 0.017 ns.
- LN with ANSI on remains 0.668 ns per row above native. Its floor is 0.017 ns.
- LOG with ANSI off remains 0.678 ns per row above native. Its floor is 0.064 ns.
- LOG with ANSI on remains 0.645 ns per row above native. Its floor is 0.064 ns.
- LOG10 with ANSI off remains 0.696 ns per row above native. Its floor is 0.024 ns.
- LOG10 with ANSI on remains 0.705 ns per row above native. Its floor is 0.024 ns.
- LOG1P with ANSI off remains 0.646 ns per row above native. Its floor is 0.075 ns.
- LOG1P with ANSI on remains 0.625 ns per row above native. Its floor is 0.075 ns.
- LOG2 with ANSI off remains 0.693 ns per row above native. Its floor is 0.133 ns.
- LOG2 with ANSI on remains 0.708 ns per row above native. Its floor is 0.133 ns.

Spark still compares each input with zero and propagates invalid arguments as NULL before the existing math kernel. The native controls receive the already valid fixture without that domain check. These comparisons measure that remaining path together with its physical dispatch.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| LN | Native | 5.211 [5.129, 5.257] | 5.317 [5.204, 5.484] | 0.017 |
| LN | ANSI off | 6.275 [6.074, 6.378] | 5.966 [5.865, 6.024] | 0.017 |
| LN | ANSI on | 6.259 [6.096, 6.406] | 5.986 [5.899, 6.123] | 0.017 |
| LOG | Native | 5.810 [5.720, 5.923] | 5.653 [5.345, 5.750] | 0.064 |
| LOG | ANSI off | 6.710 [6.332, 6.776] | 6.331 [6.227, 6.634] | 0.064 |
| LOG | ANSI on | 6.775 [6.580, 7.018] | 6.298 [6.208, 6.396] | 0.064 |
| LOG10 | Native | 6.961 [6.851, 7.129] | 6.954 [6.843, 7.025] | 0.024 |
| LOG10 | ANSI off | 7.867 [7.777, 7.916] | 7.650 [7.531, 7.672] | 0.024 |
| LOG10 | ANSI on | 7.882 [7.765, 7.920] | 7.658 [7.531, 7.687] | 0.024 |
| LOG1P | Native | 5.675 [5.561, 5.777] | 5.713 [5.643, 5.929] | 0.075 |
| LOG1P | ANSI off | 6.520 [6.400, 7.328] | 6.359 [6.276, 6.478] | 0.075 |
| LOG1P | ANSI on | 6.510 [6.385, 6.532] | 6.337 [6.267, 6.394] | 0.075 |
| LOG2 | Native | 4.925 [4.834, 4.959] | 5.022 [4.935, 5.048] | 0.133 |
| LOG2 | ANSI off | 5.860 [5.777, 6.038] | 5.715 [5.598, 5.766] | 0.133 |
| LOG2 | ANSI on | 5.881 [5.772, 5.911] | 5.730 [5.630, 6.038] | 0.133 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential test compares original CASE and masked execution for both ANSI modes. It checks exact Double bits and fields over the full number/base matrix, signed NaNs and zeros, empty input and slices.
All 709 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/log-domain-comparison.json) was
committed at `f934d8feef814fe137bc2bddaabc566b23b2866a` before clocks.
The Linux release session ran 2026-11-21T18:36:06Z to 2026-11-21T18:38:28Z.
The 90 comparison processes and 30 native controls retained 360 pairs.
Local evidence is under `costs-168-2026-11-20/1413/log-domain`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `7746a93d390618fd73f17b5b13ef08395356d547` | `e26a1a19a2dbd97908392883bac8031188869c5f673d59c06c18ec7a32a8bd8d` |
| candidate | `12f2250f4b4c068c0aee9bcc1723b69f6c3996a3` | `4ce0313f70ac033e4325a4c07e63065d7fd405b9963f72abf0497f5e58d7f4cc` |
