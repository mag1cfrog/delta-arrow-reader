# NULLIF candidate recheck (#620)

Compare staging `7b4495a1` with the candidate rebased on it (`107aa882`).
The candidate's product patch and differential tests are unchanged from
`perf/620-nullif-selection`. This check makes the merge decision using
same-session observations. The November 19 campaigns explain the cells
selected here.

## Builds and eligibility

Build the unchanged `nullif_620` benchmark target once per commit with Rust
1.96.0, `--locked --release`, release debuginfo level 1, two Cargo build jobs
and no compiler wrappers or custom flags. Record the compiler artifact path,
binary SHA-256, build command and environment. Run on Linux with UTC and an
affinity mask of CPUs 2-5. Both binaries use the frozen 262,144-row input and
the same two-worker, single-partition session.

Before timing, run the full NULLIF and checked-arithmetic preflights in each
binary. Require matching exact row values, NULLs and executed types against
staging native on the committed inputs. Compare complete IPC results using
the existing comparator when file bytes differ. Check the independent input
formula and the complete operand-effect records in both ANSI modes. Abort
before clocks if any preflight fails. Run the candidate's existing frozen SQL
and inline differential tests independently of the timed campaign.

## One session

Commit the [selection and schedule](../nullif_620_recheck/manifest.json) and
[controller](../nullif_620_recheck/run.py) before clocks. Record the full
protocol commit and both binary identities in the schedule receipt. Run
48 alternating four-process NULLIF blocks and 24 checked-arithmetic blocks. Odd blocks use staging,
candidate, candidate, staging. Even blocks reverse that order. Alternate
which family runs first during their shared blocks. Within a block, both builds have identical cell
order and configuration rotation. One warmup and three retained observations
per cell per process use the existing separate plan and execution clocks.
Builds and other measurements run outside the session.

The seven NULLIF cells cover the three previously flagged native controls,
the sparse and half-NULL computed-right cases with 262,144-row batches, and
the sparse two-column improvement at 8,192 and 262,144 rows per batch. Use
both ANSI modes on each Spark cell. Checked arithmetic covers add, subtract
and multiply across the old 36-cell matrix and all three modes. Preserve
every sample, including the planning controls. The 288-process schedule
has a 180-second per-process limit and a 45-minute overall limit. The untimed
preflight has a separate 25-minute limit. Keep any partial evidence and stop
if either schedule breaks its bounds or counts.

## Merge rule

For each cell, configuration and phase, take the median of the three samples
in each process. In each block, subtract the average of the two staging
process medians from the average of the two candidate medians. Report both
build medians, the median of the paired deltas, and the same-binary noise
estimate. The noise estimate is the larger median absolute difference between
the two same-binary positions, calculated separately for staging and
candidate. A gate flags an increase when the median paired delta exceeds
that noise and the exact one-sided sign test on the paired deltas passes a
Holm step-down test. Allocate 2.5% each to the native/checked and fallback
groups for a combined 5% error bound. Ties do not count as
sign observations. The rule uses observed variation and has no fixed
millisecond or percentage allowance.

Gate the three specified native comparisons, planning and execution for all
checked controls, and pooled execution for both fallback cells in both ANSI
modes. In each fallback cell, also gate the number of upper-mode observations
per build and block and the separately matched lower- and upper-mode
execution medians. Use splits at 3.45 ms for sparse NULLs and 1.59 ms for half
NULLs, near the midpoints between the previously reported regimes. Use all
process medians to report mode frequency and within-mode cost. Require ten non-tied matched block deltas and at least one same-build
repeat pair in that mode per build. With 16 fallback tests, ten same-direction
deltas can clear the most stringent Holm threshold. Missing or insufficient
mode observations leave acceptance unresolved. The mode count uses the paired sign rule in units of processes per block. It has no extra
repeat-noise term because each build contributes one count. Within-mode comparisons use the pairs with observations
on both sides. A same-binary spread estimate for a mode uses blocks with two
observations in that mode from the same build.

A detected native slowdown stops this merge. A detected fallback slowdown
requires attribution before merging. An unresolved mode comparison or a
detected checked-arithmetic regression leaves the candidate unaccepted.
Keep every gate with its block deltas, thresholds, noise and decision.
Report positive paired deltas and uncertainty even when a gate clears. This campaign measures the selected controls on
this host. The residual NULLIF attribution and any further implementation
are a separate step after the merge.
