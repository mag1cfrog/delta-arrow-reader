# SEQUENCE endpoints without identity selection (#1410)

The staged INT plan evaluates two lazy endpoints over a row selection,
even when the selection covers the whole batch. Its scatter already
returns the array directly when no row was removed. The candidate
borrows INT captures and checks each endpoint for logical NULLs.
The first NULL resumes the original filtering with the endpoint values
already computed. Non-INT endpoints and the error retry keep the original
evaluation. The integral generator and the native range are unchanged.

| Path | Staging median [range] | Candidate median [range] | Median paired change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 20.649 [20.211, 25.715] | 19.329 [19.240, 24.588] | -1.100 |
| Spark ANSI on | 20.877 [20.421, 25.681] | 20.228 [19.753, 23.821] | -0.470 |
| Native | 12.430 [10.480, 14.920] | 12.494 [10.334, 15.825] | +0.196 |

Times are ns per input row above projection. Ranges span nine samples.
The ANSI-off process differences were -1.385, -0.609 and -1.100 ns.
The ANSI-on differences were -0.364, -0.470 and -0.660 ns. Both median
reductions exceeded the 0.340 ns native A/A floor. The native process
differences were +0.880, +0.196 and -0.842 ns, so the gate passed
without a layout exception.

The remaining differences above native are 6.834 ns per row with ANSI
off and 7.734 with ANSI on. Removing identity selection accounts for
only part of the gap. Spark still runs lazy physical expressions and
its checked inclusive generator. The native reference uses a half-open
range with an aligned result layout. The paired run times the whole
expression and does not isolate those remaining operations from each other.

## Validation

Both builds agreed on one million rows and executed Int64 and List(Int32)
types in native and both ANSI modes. The new differential compares the
complete and selected evaluators on sliced and empty batches, scalar steps,
staggered and all-NULL endpoints, skipped division and reachable errors.
The generator differential and existing SQL fixtures stayed unchanged.
Formatting, root Clippy and the full workspace passed with 3,305 tests
passed and three ignored. Independent review checked the NULL transition
and the unchanged error retry.

The [protocol](../array_functions_1410_round3/sequence-protocol.md)
was committed at `7f62b392` and the
[build pins](../array_functions_1410_round3/sequence.json) at `5719f840`
before the paired session. It ended at 2026-11-23 05:06:35 UTC.
The archive contains [all 72 samples](../array_functions_1410_round3/sequence-samples.jsonl.gz)
from 24 processes.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Staging | `b9af4664f63a368fec0e27613d4e1cbe7ec55dcb` | `9a0c239b50cbab32b9e748d8879a090fc1ba275bfa72239035f94e200a2e4c2e` |
| Candidate | `f5ea78cb8485523738775056d28a2232e9c32762` | `95af9da86355f99940a1c3834422a3f878b2e203410caabc55f124d8a666f444` |
