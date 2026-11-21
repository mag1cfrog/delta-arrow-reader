# Array function attribution protocol (#1410)

Pin staging and the release executable in [manifest.json](manifest.json).
Reuse the last #1409 executable. Only benchmark and report files distinguish
its source from the pinned staging tip. Verify the executable and observation
patch hashes against the build receipt. Copy the nine complete cases from the
frozen #1391 manifest with their original SQL, projection and input columns.

## Preflight

Use the #1391 seeded million-row table, 8,192-row batches, one worker and one
target partition. Keep the original Rust release settings and CPU affinity.
For each case, save the physical Spark plans in both ANSI modes and the
native plan. Compare exact values by ID, both executed column types, NULLs
and row guards before profiling. Exclude a mismatch from sampling and report
it without a result fix.

## Profile and attribution

Take one user CPU profile per eligible function on its ANSI-off Spark plan.
Use `perf record` with `cpu-clock:u`, 997 Hz and an 8 KiB DWARF stack.
Each profile process runs the frozen query/projection pair through the
same harness three times, reusing the fixture. Limit each process to 45
seconds and 96 MiB of samples. Apply the frozen memory limit and a
20-minute bound to the full schedule.

Symbolize the stack evidence and report which query, projection and fixture
frames it can distinguish. Use the two physical plans and corresponding
source to compare work on the Spark and native paths. State uncertainties
from unresolved or low-count frames. These profiles attribute CPU work. The
per-row intervals printed by the profiled harness are evidence of completion,
not unprofiled performance estimates.

A cause is ready for a code slice when its Spark profile and source trace
show work the native plan avoids. Group the functions that share that work
and order slices by the largest measured difference in #1391. No product
changes are part of this attribution. Later candidate comparisons use their
own matched schedules and native A/A floors.
