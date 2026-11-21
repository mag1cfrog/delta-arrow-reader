# Checked batch results for integer TRY arithmetic (#1412)

Same-width signed TRY arguments first use the existing Arrow checked
kernel. If it fails, the original row kernel preserves successful
values and returns NULL for overflow.

## Remaining gaps

- TRY_ADD with ANSI off remains 4.371 ns per row above native. Its floor is 0.039 ns.
- TRY_ADD with ANSI on remains 4.110 ns per row above native. Its floor is 0.039 ns.
- TRY_MULTIPLY with ANSI off remains 3.492 ns per row above native. Its floor is 0.030 ns.
- TRY_MULTIPLY with ANSI on remains 3.687 ns per row above native. Its floor is 0.030 ns.
- TRY_SUBTRACT with ANSI off remains 4.008 ns per row above native. Its floor is 0.045 ns.
- TRY_SUBTRACT with ANSI on remains 3.585 ns per row above native. Its floor is 0.045 ns.

The TRY path still checks integer overflow and retains the widening
cast and its result annotation.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| TRY_ADD | Native | 3.333 [3.204, 3.392] | 3.294 [3.208, 3.345] | 0.039 |
| TRY_ADD | ANSI off | 9.726 [9.551, 9.804] | 7.665 [6.916, 7.823] | 0.039 |
| TRY_ADD | ANSI on | 9.737 [9.561, 9.858] | 7.404 [6.728, 8.060] | 0.039 |
| TRY_MULTIPLY | Native | 3.465 [3.413, 3.500] | 3.507 [3.420, 3.552] | 0.030 |
| TRY_MULTIPLY | ANSI off | 9.814 [9.550, 10.180] | 6.998 [6.798, 7.791] | 0.030 |
| TRY_MULTIPLY | ANSI on | 9.726 [9.572, 9.908] | 7.193 [6.714, 8.049] | 0.030 |
| TRY_SUBTRACT | Native | 3.310 [3.229, 3.356] | 3.335 [3.244, 3.402] | 0.045 |
| TRY_SUBTRACT | ANSI off | 9.740 [9.560, 9.769] | 7.342 [6.714, 7.744] | 0.045 |
| TRY_SUBTRACT | ANSI on | 9.710 [9.553, 9.803] | 6.919 [6.701, 7.319] | 0.045 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Nine untimed controls compare exact rows, data types or original
errors in both ANSI modes. They cover all four signed widths with
and without overflow, NULL operands, scalar broadcasting, empty
input and a selected or skipped fallible input.
The timing uses nonoverflowing batches. An overflowing batch
can run the initial checked attempt before the row kernel.
All 668 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/try-integer-protocol.md) was
committed at `275cdb44c88b7601e0b1a2a17ecd4d05998d859f` before clocks.
The Linux release session ran 2026-11-21T10:43:54Z to 2026-11-21T10:45:20Z.
The 54 comparison processes and 18 native controls retained 216 pairs.
Local evidence is under `costs-168-2026-11-20/1412/try-integer`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `3e900b8424cf1c8ac5310d8887751d52d722cf89` | `ce8704bcf0d3176de5485f345c1dd0647e02dc20ec8f4cfd028937330b9abd87` |
| candidate | `4b503cee9e369f57be0b3fef5dfd2595d0ddc985` | `22074ccd1360d9c31c3a46632db5d25c38b32e079a6950f68e0e40fec4f621db` |
