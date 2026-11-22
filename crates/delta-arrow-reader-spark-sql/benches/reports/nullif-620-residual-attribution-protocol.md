# Attribute the remaining NULLIF execution cost (#620)

Measure the saved selection path after its merge. Attribute Float64
normalization, equality and validity work, and selection for computed
right operands. The [manifest](../nullif_620_round3/attribution.json)
pins the release executable from the rebase check. Changes between that
build's source and the staging merge add the rebase protocol and report
only. The executable passed the full release preflight for the selected
cases, including native value and type controls.

## Observe the actual execution path

Profile the existing `nullif_620 measure` entry point with Linux perf.
Retain both the decoded user-space CPU samples and the unchanged bench
samples. The profiler adds overhead, so bench times in this archive
describe the profiled run. Use a subsequent unprofiled paired run to
decide any candidate.

Generate one eligibility receipt per group by rejecting every ID from
the frozen [NULLIF manifest](../nullif_620/manifest.json) outside the
group. Use the same synthetic DOUBLE columns and 262,144 rows. Within
each group, the harness uses native SQL and both Spark ANSI modes with
one warmup and three observations. Alternate which group runs first
in each block. Run 12 blocks on CPUs 2-5 with no concurrent local
compilation. Limit each process to 90 seconds and the session to
15 minutes. Profile only the new child process in a minimal process
environment.

The recording command uses the manifest's event, frequency and stack
depth. For each group and block, set the output paths and run:

```sh
timeout 90 taskset -c 2-5 perf record -q --no-buildid-cache \
  -e cpu-clock:u -F 1999 --call-graph dwarf,4096 -o "$profile" -- \
  "$binary" measure nullif main "$block" "$eligibility"
perf script --ns -i "$profile"
```

## Attribute by call stack

Use frames on the actual NULLIF execution routes. Count normalization
only for samples under `SparkComparisonFloat` within `SparkNullIf`.
Count equality and output validity samples under the comparison and
NULLIF kernels in the same Spark caller. For computed-right selection,
look for batch filtering, right-expression evaluation and scattering
under the physical evaluator. Break out those three operations when
the frames distinguish them. Keep all other samples out of these counts.

Read the code around each frame to verify that it belongs to the
observed path. In particular, NULL-first selection also serves other
scalar functions, so restrict the result to the profiled query shapes.
Report the counts by group and any ambiguous frames. Treat zero matches,
unusable call stacks or overlapping call sites as insufficient attribution
for that cause. A positive count establishes where time is spent, with
no claim that sample shares are elapsed-time measurements.

Keep raw output from completed or partial timed processes. A timeout,
unsuccessful process or incomplete sample series ends the run and is
reported. Commit the compressed samples with the report.
