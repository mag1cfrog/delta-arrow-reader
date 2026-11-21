# Default INT SEQUENCE generation results (#1410)

The default INT generator stores checked offsets directly and emits
typed integral values for nonnull argument arrays. The lazy argument
path and the cap on row and batch lengths remain. The general
generator still handles NULLs, explicit steps and other input types.

## Remaining gaps

- SEQUENCE with ANSI off remains 8.394 ns per row above native. Its floor is 1.084 ns.
- SEQUENCE with ANSI on remains 9.113 ns per row above native. Its floor is 1.084 ns.

Spark still evaluates the endpoints through lazy arguments and
scatters the results back to the input rows. The native range plan
uses its own half-open range kernel. The comparison measures the
whole expression. The attribution profile records the component samples.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| SEQUENCE | Native | 13.412 [11.200, 16.406] | 12.374 [10.640, 15.648] | 1.084 |
| SEQUENCE | ANSI off | 26.372 [26.024, 34.544] | 20.768 [20.516, 26.734] | 1.084 |
| SEQUENCE | ANSI on | 26.784 [26.348, 32.986] | 21.486 [20.583, 26.834] | 1.084 |

Each timed statement matched all one million result rows and their
executed Arrow types across both builds, both ANSI modes and native.
The differential compares the previous generator with the new one
for sliced and empty batches, ascending and descending ranges, equal
endpoints, NULLs, INT endpoints, and exact row-size and batch-size
errors before any large allocation.
All 644 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../array_functions_1410/sequence-protocol.md) was
committed at `38874fe5f943b4314f90a0b30ac0ae66f3968e1c` before clocks.
The Linux release session ran 2026-11-21T06:55:13Z to 2026-11-21T06:55:46Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1410/sequence`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `98d89716850f25beb647327c8e47a9f2477a3e88` | `d95e11833c6dd42045e17f478aa1224f7cf38b1782bf3700a7ab45d8bd5e1e39` |
| candidate | `44827ae5e95edec98ea54554df5a23a6bb478985` | `9f13e80664c34f0da3f7c9bb99488d5550653fb96ae8e48235746d2033a666bc` |
