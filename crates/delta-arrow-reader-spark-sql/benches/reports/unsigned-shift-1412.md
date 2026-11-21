# Direct BIGINT unsigned shift results (#1412)

The signed BIGINT column feeds the existing shift and final cast
through an unsigned Arrow buffer view. The signed minimum keeps
the original ABS failure. Non-column and narrower inputs keep
the prior expression.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| `>>>` | Native | 14.852 [14.775, 14.974] | 14.898 [14.748, 15.698] | 0.090 |
| `>>>` | ANSI off | 22.418 [21.633, 24.589] | 5.282 [5.171, 5.581] | 0.090 |
| `>>>` | ANSI on | 22.257 [21.427, 24.513] | 5.315 [5.162, 5.506] | 0.090 |
| SHIFTRIGHTUNSIGNED | Native | 4.274 [4.197, 4.325] | 4.241 [4.193, 4.289] | 0.062 |
| SHIFTRIGHTUNSIGNED | ANSI off | 20.413 [20.249, 20.735] | 4.001 [3.924, 4.085] | 0.062 |
| SHIFTRIGHTUNSIGNED | ANSI on | 20.348 [20.214, 20.556] | 4.050 [3.921, 4.108] | 0.062 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Fourteen untimed SQL boundaries compared rows, executed types, NULLs,
and the original failure in both ANSI modes. They include signed
limits, NULL and invalid distances, computed and INT inputs, and
an empty source. A CASE can select away the minimum. A separate
LIMIT 0 control retained the no-scan EmptyExec plan. The direct
unit test checks sliced shared bytes, NULL validity, and the ABS
failure on a visible minimum.
All 666 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/shift-protocol.md) was
committed at `39a4d3a01b3ad0605ae64cc99db03fdf4810d9d1` before clocks.
The Linux release session ran 2026-11-21T10:03:25Z to 2026-11-21T10:04:23Z.
The 36 comparison processes and 12 native controls retained 144 pairs.
Local evidence is under `costs-168-2026-11-20/1412/unsigned-shift`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `b906c65d0e8c3b5c38a2c40f8c13cfe31e575754` | `bc200c25c5984ce872d59fb934b87dd4e62f0b2b5e5f8cf64d2c296f30625339` |
| candidate | `b08aab1714d31058cf5689740c50ad6afe6cfe76` | `6f350cf5481b3ea879faa9073a4102ff0110c0ba91b2f4d71e56f5ce99973233` |
