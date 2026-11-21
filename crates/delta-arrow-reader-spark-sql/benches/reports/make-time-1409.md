# Literal MAKE_TIME component results (#1409)

MAKE_TIME went from 5.859 and 5.912 to 4.211 and 4.273 ns per row above
projection with ANSI off and on. Valid literal minutes and seconds
contribute the same microseconds to every row and are prepared once.
The existing hour validator checks each nonnull hour.
Both ANSI modes meet the precommitted native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. The floor uses three native process medians
per control arm.

| Path | Staging | Candidate |
| --- | ---: | ---: |
| Native | 5.159 [5.067, 5.344] | 5.096 [4.961, 5.174] |
| ANSI off | 5.859 [5.797, 5.979] | 4.211 [4.139, 4.343] |
| ANSI on | 5.912 [5.802, 6.053] | 4.273 [4.154, 4.527] |

The native A/A floor was 0.043 ns per row.

The million-row preflight matched every value and the executed
Time64(Microsecond) result across builds, ANSI modes and native.
The committed differential compares scalar and array components,
including hour and minute bounds, fractional seconds, NULLs, empty
input and failed validations with the original precedence.
All 636 library tests, formatting and Spark-root Clippy passed.

The [protocol](../time_conversions_1409/make-time-protocol.md) was
committed at `0776ea789ed4b3a86e1496b35465c24f21232abd` before clocks.
The Linux release session ran 2026-11-21T04:48:34Z to 2026-11-21T04:49:04Z.
The 18 comparison processes and six native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1409/make-time`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `ffcca7e84b1f60b6182913ced7e133b97a014c67` | `c197e077258c88e43919f5f149bb93ffa7bfacd6bc98d91b83ff06c04d291bcb` |
| candidate | `42de836662a847b1059fafc29c8be4ab71d8a3f8` | `15e79c04e2fe31503dcc31fb74246270c44f14260eecce9730b545dc5d92e81a` |
