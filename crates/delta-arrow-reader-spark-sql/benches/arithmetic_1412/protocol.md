# Arithmetic and integer attribution protocol (#1412)

The [manifest](manifest.json) pins the staging tip, corresponding
release executable and the 15 statements from the frozen #1391 table,
including the DECIMAL division and rounding shape. The release receipt
preserves the executable, its source and the evidence patch hashes.

Run [capture.py](capture.py) with that release build on Linux using the
[bounded profile schedule](../array_functions_1410/protocol.md). It saves
physical plans for both ANSI modes and native. Require exact rows by ID
and executed Arrow data types first. Report any mismatches and exclude
the affected function from profiling and later timing.

Take one CPU profile for each qualifying shape. Use ANSI off unless
the manifest selects ANSI on, where #1391 found a larger Spark-to-native
gap. The session is limited to 30 minutes, with the same 45-second
capture, 96 MiB data cap, CPU affinity, release flags and test table.

Group the costs by the code and physical operations that produce them.
Each subsequent change uses its own committed comparison protocol,
largest attributed cost first. Its target is at or below the candidate
native median plus the matched-session repeat floor in both modes.
The profiles attribute work and cannot establish a timing result.
