# #1389 interval CPU attribution on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Use the [frozen interval row probe](../interval_1389_round3/profile_rows.rs)
and the staging source pinned in the [manifest](1389-interval-profile.json).
The borrowed-token candidate is held, so staging still feeds the batch
numeric grammar from its owned stream. Each process parses 256 batches
of 8,192 inputs for its shape and returns the exact completed call count.
The probe uses the same Sail batch parser as both Spark ANSI modes.
It isolates parser work from SQL planning, Arrow output and source I/O.

Build with Rust 1.96.0 in release with debug level 1. Add only the
existing native QoS check and the whole-probe elapsed time to the
frozen source. Build in the detached scratch checkout and restore it.
Use mains power, High Power mode and sleep prevention. Leave out Linux
CPU affinity and resource controls. Keep other builds and benchmarks
idle until the capture finishes.

Run the staging probe twice for each shape in nine blocks. Reverse
both orders on alternate blocks. Sample stacks for one second at
1 ms intervals with the native macOS sampler. Bound each sampler at
15 seconds, each worker at 20 seconds and the campaign at 15 minutes.
Keep every stack report and process result. A valid shape has all
18 successful captures and complete input counts. An incomplete
shape stays unresolved in this diagnostic.

For each usable shape, identify parser stacks. Attribute a stack to
the deepest named lexer, grammar or value-conversion phase. Keep
unresolved parser frames and non-parser frames in their own counts.
Compare the two staging arms using the same paired median and A/A
floor formula as #1560 for elapsed time per call. Estimate phase cost
from the named parser sample share and the observed per-call cost.
Whole-query gaps from the paired borrowed-token trial provide scale.
Treat those estimates as attribution. The uninstrumented SF1 cases
and their own A/A floors will decide a candidate.
