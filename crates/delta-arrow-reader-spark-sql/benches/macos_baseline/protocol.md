# Native macOS baseline protocol (#1560)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
All timing tables use the label **Apple Silicon, native macOS**.

## Release binaries and session controls

Build the frozen benchmark drivers with Rust 1.96.0 in a detached disposable
checkout of the source pinned in `protocol.json`. The adapter records the input
commit, overlay hashes and executable hashes. It restores the checkout before
the run. The production CLI and Python package stay on their existing paths.

Use mains power and High Power mode, with Low Power mode off. Keep the Mac awake
with `caffeinate -is` for the entire session. Check for other builds, benchmark
processes or a Linux VM before and after the session and before each child
process. The benchmark main threads and Tokio workers, including the blocking
pool, set and verify user-interactive QoS from inside the thread. The macOS
scheduler retains control of core placement. Omit Linux CPU pinning, the process
address-space limit, peak RSS and Linux CPU governor readings. Keep DataFusion
memory pool limits, row counts, SQL, warmups, result checks and timing intervals
from the corresponding harness. Serial benchmark children run one at a time.

Write compressed process records as each process finishes. A failed or
interrupted process keeps every complete sample record it produced. Record an
untimable reason for a failed or invalid cell, then continue with the remaining
cells within the same bound. Fields containing local paths use named roots.
Build and preparation output is separate from the raw timing archive.

## Adapter self-check and fixture

Each historical runner listed in #1560 has a single-cell check. Preflight-only
checks verify full results or the executed plan without collecting a timing
sample. Any check that retains samples uses the same nine-block rule as the
baseline. Keep the self-check archive with the report. For older paired drivers
the adapter strips the Linux wrapper at process launch and invokes the same
per-cell entry point. The original runners and historical reports remain frozen.

Generate the SF1 base from the `execution-1380-fixture-commands.json` recipe at
its recorded source commit, with tpchgen 3.0.0 at the recorded revision and
Rust 1.98.1. The macOS preparation copy uses Darwin disk capacity and process
identity APIs and the existing POSIX deadline. Omit the Linux address-space
clamp while keeping the sort, write, spill, capacity and elapsed limits. Use the
frozen `execution/prepare.py` to derive the partitioned Delta snapshot. Verify
the Parquet files and Delta logs against the committed receipt. The generator
executable and preparation manifest can differ across hosts. If snapshot bytes
differ, require matching decoded values and exact types, known row counts, and
a full stock CLI export before any SF1 timing.

## Decision rule

Use nine blocks for each session, with reversed cell and arm orders on alternate
blocks. A process produces one median for each metric of a cell. Before and
after run in each block, along with two A/A staging arms, A and B. For a
Spark-versus-native comparison, before and the A/A arms use the native
configuration and after uses the specified Spark ANSI configuration. For a
same-cell candidate comparison, use the same configuration in all four arms.

The paired change is the median of the nine differences (after minus before).
The floor is the greatest of the absolute difference of the A and B medians,
half the A range, and half the B range. A change strictly below minus the floor
is faster. A change strictly above the floor is slower. The other changes are
within the floor and cannot resolve a historical open difference on this Mac.
For a candidate that does not touch a slowed control, apply the normalized
instructions and state layout check in `literal-utc-layout.json` before
deciding the gate. An incomplete session cannot pass a merge gate.
For the day-time interval cast, the native engine has no equivalent statement.
Use the original same-ANSI input projection. Each A/A arm contains both the
cast and the projection, and its sample for the floor is their difference.

## Baseline sequence

1. Run the seven cells in `protocol.json` against themselves in two A/A arms.
   The function cells were selected from the existing inventory to cover three
   cost ranges. Their measured Mac costs can differ from those ranges. Keep
   both the first-three-block floor and the nine-block floor. Start with three
   retained samples per process. If a cell's nine-block floor exceeds 3% of
   the average of the two arm medians in absolute value, repeat the selected
   cells of its harness once with nine retained samples. A zero cost also
   causes a repeat. Freeze that count for the harness. Report any remaining
   floor above 3% and continue.
2. Preflight all 633 #1391 entries on the same release executable. Keep the
   committed SQL, native comparison, one-million-row input and exclusion rules.
   Recheck complete rows and exact executed types in both ANSI modes. Run the
   original three-block rotation three times to satisfy the nine-block minimum.
   Keep the sixteen-case shards and original three retained samples. Reverse
   traversal on alternate blocks. Repeat every eligible paired case with a
   slower ANSI result, using the retained count fixed by the noise study. The
   repeat's result supplies both ANSI baseline rows for those cases.
3. Run the planning, parser, string and NULLIF cells in `baseline-cells.json`
   with the same frozen nine-block schedule. Retain the full rows and the
   component timing metrics exposed by each original harness. The wide
   planning cell uses its physical planning time, along with logical and total
   samples in raw evidence. SF1 execution uses query completion time and
   retains source I/O and row counts. NULLIF uses execution time and retains
   planning separately. A parser with a differing native result is untimable
   for the paired comparison. Reuse the same planning samples for the logical
   and physical rows in the report. The six additional time conversion SQL
   shapes use the existing named-zone and offset manifest. Keep their current
   Spark costs above projection and use that projection for the A/A floor.
4. Publish one Mac-only baseline table with the issue owner of each slower
   case, the status of each historical open difference, orphans and untimable
   cases. Keep all open differences eligible for the separate Linux x86
   confirmation.

`preflight.py` writes the non-function eligibility receipt and the NULLIF
exclusion file. `run.py preflight-noise` and `run.py preflight-functions` write
the independent function eligibility receipts. Pass the receipt for the same
release executable to each timing command. `run.py noise`, `sweep`, `repeat`,
and `cells` each use a fresh evidence directory and write their compressed raw
samples and summary there. Pass the noise summary to `repeat` and `cells` to
bind the retained sample counts. The report generator reads both function
summaries and the non-function summary and raw samples.
