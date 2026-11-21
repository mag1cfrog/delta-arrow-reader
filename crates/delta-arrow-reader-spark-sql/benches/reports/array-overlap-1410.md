# Direct BIGINT array membership results (#1410)

ARRAYS_OVERLAP checks BIGINT child buffers for membership after the
existing operand preparation, parent NULL and empty-array checks.
Small lists compare values directly. Larger lists index the smaller
input. A match wins before a NULL child can make a miss unknown.

## Remaining gaps

- ARRAYS_OVERLAP with ANSI off remains 64.826 ns per row above native. Its floor is 0.109 ns.
- ARRAYS_OVERLAP with ANSI on remains 66.054 ns per row above native. Its floor is 0.109 ns.

Spark retains compound operand preparation and its parent NULL and
empty-list CASE gates. The native reference calls array_has_any
directly. This comparison measures their total difference.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| ARRAYS_OVERLAP | Native | 17.345 [17.218, 17.526] | 17.505 [17.141, 18.046] | 0.109 |
| ARRAYS_OVERLAP | ANSI off | 127.281 [126.525, 134.266] | 82.331 [81.980, 82.647] | 0.109 |
| ARRAYS_OVERLAP | ANSI on | 127.228 [126.925, 132.963] | 83.559 [82.054, 90.622] | 0.109 |

Each timed statement matched all one million result rows and their
executed Arrow types across both builds, both ANSI modes and native.
The committed differential covers List and LargeList, both ANSI
modes, slices, NULLs, empty inputs, BIGINT limits and both large-list
lookup directions. The SQL regression and two fixture probes preserve
derived-operand outcomes and the original ANSI cast error.
All 769 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../array_functions_1410/array-overlap-protocol.md) was
committed at `8303e4e75fb08f72b65baf319545f0800b33c73f` before clocks.
The Linux release session ran 2026-11-21T05:42:39Z to 2026-11-21T05:43:16Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1410/array-overlap`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `994cb1eccad55a1ce0391d9834579de4c7c48f7e` | `0f7add96e82b089c3c014790051d456281251e0f9d49bea5b87327bbb022ee48` |
| candidate | `2d9ba819e0b7625999efc3aa1a63094def106198` | `ea6a0daed7b45c6c312e254aa387ed099d97222edf4d9ac225c2a10dfacc7918` |
