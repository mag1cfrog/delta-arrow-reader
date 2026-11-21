# INLINE_OUTER offset emptiness results (#1411)

INLINE_OUTER tests the list offsets for emptiness on List and LargeList
columns while preserving the original outer selection and expansion.

## Remaining work

Spark still selects the row lists, projects STRUCT fields and expands
rows, with a NULL result for empty or NULL lists. The native control
projects the fixture's known field, so its repeat floor describes the
control. The reported improvement compares the two Spark builds.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| INLINE_OUTER | Native | 0.046 [0.044, 0.055] | 0.051 [0.040, 0.075] | 0.007 |
| INLINE_OUTER | ANSI off | 99.301 [96.525, 101.618] | 18.994 [14.682, 19.908] | 0.007 |
| INLINE_OUTER | ANSI on | 98.237 [96.234, 102.388] | 19.342 [15.404, 19.612] | 0.007 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The predicate differential covers null and empty lists, NULL STRUCTs
and children, both offset widths, sliced arrays and empty batches.
The boundary SQL matched all 32 expanded rows, field names and types
in both ANSI modes across the two builds.
All 645 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../generators_text_conditionals_1411/inline-outer-protocol.md) was
committed at `e120bd45d25da0181c5e5d65899f7259b7deb07d` before clocks.
The Linux release session ran 2026-11-21T07:23:26Z to 2026-11-21T07:23:58Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1411/inline-outer`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `44827ae5e95edec98ea54554df5a23a6bb478985` | `9f13e80664c34f0da3f7c9bb99488d5550653fb96ae8e48235746d2033a666bc` |
| candidate | `edfdafd1bc7afa705fe8825a33d5e7ce7c4e1f1f` | `41b087b967a935f33870d326fbd1b48405e02728c1665afdd97e5d2f9edfa46d` |
