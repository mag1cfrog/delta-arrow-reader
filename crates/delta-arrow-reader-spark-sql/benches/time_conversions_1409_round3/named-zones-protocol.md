# Los Angeles timestamp conversions (#1409)

Replay the three unchanged Los Angeles statements from the #1409
manifest. Compare the round-two base `a8e0ccf8` with the current
staging product in a single Linux release session. The earlier
fixed-offset report measured CONVERT_TIMEZONE at 136.978 to
147.541 ns per row with ANSI off and 139.551 to 132.253 with
ANSI on. Those sample ranges overlapped.

Use the [paired driver](run_pair.py), its frozen one-million-row
fixture and the [committed build pins](named-zones.json). Staging
`ce3bedec` and the sealed current build from `74503100` have
identical source, tests, workspace and lockfile. Before timing,
compare all rows and executed types in both ANSI modes and both
builds. Both arms run Spark SQL. Exclude queries that differ.
The fixed input covers one UTC date. The existing timezone
conversion tests cover daylight-saving transitions.

The current executable uses the build recipe from `716c8b85`,
available in the [previous trial's bundle](utc-identity-candidate.bundle).
The recipe bytes match those on staging.

Run three blocks with both binaries in turn and alternate their
order. Each block has the three cases in both ANSI modes with
both builds, giving 36 processes and 108 retained samples.
Keep the session within 3,600 seconds and each process within 130
seconds. Report ns per input row above projection, the range of
nine samples per arm, and all three paired process median deltas.
Verify the expected successful process records and retained
samples for each case before assessing its timing.
Treat a slowdown as reproduced if all three deltas are positive
and every current process median is above every historical process
median. Report any other result with its observed range.

Profile the current binary once with the three named-zone
statements, ANSI off. Use Linux perf sampling of user-space CPU
stacks at 499 Hz with 8 KiB DWARF stack snapshots, on the same
CPUs and fixture, bounded to 390 seconds. Keep the complete
sampled stack stream and the benchmark's samples collected under
profiling. Use the profile for attribution. Propose a single
product change only if a sampled hot path repeats work per row
that depends solely on the already-resolved zone. The date-specific
offset and daylight-saving transitions stay data dependent.

Run the profiler with `perf record -e cpu-clock:u -F 499 --call-graph
dwarf,8192` and the current release `functions measure-set` command.
Pass the unchanged #1409 manifest and a JSON list of the three case
IDs from the build pins. Use `spark-off` and the frozen CPU list.
Decode the captured stacks with `perf script` and store them
alongside the 108 unprofiled samples. Verify each profiled case
completed successfully and retained three samples.

The paired timing completed at `f7421fe2`. The host denied the
perf event before it collected any samples or started SQL. Use
the [LLDB stack sampler](sample_stacks.py) for the single profile,
with the same executable, fixture, cases, ANSI mode and CPUs.
It launches the benchmark as its own child, captures up to 128
stack snapshots spaced 11-17 ms apart, then waits for completion
within the 390-second bound. Keep the original stacks and profiled
result records. Samples show active call paths and include paused
threads, so use them to identify work and use the earlier paired
release run for elapsed time.
