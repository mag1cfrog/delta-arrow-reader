# Retesting the deferred set-session snapshot (#1390)

Compare staging `9433cb49` with the rebase of `cbe4704f`. The two Sail
source files match the archived candidate byte for byte. That change
delays the session snapshot until the resolver finds a pending set.
The earlier session flagged native-configured EXCEPT physical time
and native-configured INTERSECT total time.

Run the [paired controller](../planning_round3_1390/compare.py) with the
[committed build settings](../planning_round3_1390/session-snapshot.json),
the frozen #1379 fixture and CPU 2 on Linux.
Before clocks, require formatting, Spark-crate Clippy, the full local
Spark workspace and independent review. Require exact Arrow schemas,
logical and physical plans, properties and zero Delta data reads for
all 678 traces per arm. Use the existing #1379 executed references.
The benchmark only plans queries.

Run the same 50 processes for 25 paired and five stress statements in
all four configurations, with staging and candidate alternating in each
block. Retain all 13,500 timing pairs. Each process has 300 seconds, and
the schedule checks its 600-second budget at process boundaries.

Report `paired/except` physical and `paired/intersect` total in
native-configured, along with all other canonical rows. For each
row, the same-session noise floor is the absolute difference of the
two native A/B medians for its statement and phase. A row fails when
all five paired deltas are positive, their median exceeds the A/B
floor, and the candidate minimum exceeds the staging maximum.
All other measured rows pass this timing rule.

For a flagged control outside the changed path, classify the shift
as layout only if the actual normalized instructions and state
layouts match. A changed or unverified path keeps the failure.
Merge the rebased snapshot change if validation and the timing gates
pass. Otherwise keep the trial out and publish the remaining gap.
Report the median logical time across the 25 paired statements in
both Spark modes and preserve all raw samples in either outcome.
