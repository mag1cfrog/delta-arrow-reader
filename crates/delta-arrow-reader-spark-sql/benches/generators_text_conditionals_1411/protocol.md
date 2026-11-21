# Generator, UTF-8 and conditional attribution protocol (#1411)

Use [manifest.json](manifest.json) to pin the staging head, the last
#1410 release executable and nine complete statements from the frozen
#1391 manifest. Only benchmark and report files distinguish that
executable's source from staging. The existing profile runner verifies
the build and patch hashes and the copied statements.

Run [capture.py](capture.py) on Linux using the
[bounded profile schedule](../array_functions_1410/protocol.md).
Keep the frozen table, build flags, CPU affinity and memory limit.
Save the physical Spark plans in both ANSI modes and the native plan.
Require exact keyed rows and executed types before taking one ANSI-off
CPU profile for a function. A differing result excludes the function
and is reported with its cause.

Use the saved symbols, plans and code to attribute specific work and
state any uncertainty from the samples. Order the next slices by the
largest #1391 gap. INLINE_OUTER, ENCODE and DECODE have native controls
that do different work. Their follow-up comparisons report cost and
its remaining components. The other functions use the same-session
native repeat floor for acceptance.

This captures profiles only. Each change and its matched release
comparison will use a separate committed protocol.
