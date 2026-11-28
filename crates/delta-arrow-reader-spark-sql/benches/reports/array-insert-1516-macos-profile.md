# Literal second-position ARRAY_INSERT attribution on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

ARRAY_LENGTH inside the Spark interior condition costs 43.877 / 43.869
ns per input row with ANSI off / on. It accounts for about 96% of the
predicate clock and 58% of the CASE clock. The direct insert kernel
takes about 29.3 ns per row. The earlier uninstrumented [baseline](functions-1516-macos-restricted.md)
measured whole-query gaps of +3.599 / +3.536 ns per row above native.

The [protocol](../macos_rounds/1516-array-insert-profile.md) pins staging
`4e969486` and the unchanged restricted statement. Its 937,500 rows
have non-NULL arrays and inserted values. Exact output values, executed
types, physical plans and source guards agree with the uninstrumented
#1599 release in native and both ANSI modes.

**Apple Silicon, native macOS.** Phase median / A/A floor in ns per
input row. ARRAY_LENGTH is nested in the predicates. The direct copy
and predicates are nested in CASE. Clocks include probe overhead.

| Measured phase | Native | Spark ANSI off | Spark ANSI on |
| --- | ---: | ---: | ---: |
| Spark CASE | N/A | 75.433 / 0.342 | 75.412 / 0.594 |
| Spark CASE predicates | N/A | 45.536 / 0.321 | 45.501 / 0.509 |
| ARRAY_LENGTH inside Spark predicates | N/A | 43.877 / 0.324 | 43.869 / 0.510 |
| Spark interior direct copy | N/A | 29.257 / 0.272 | 29.257 / 0.105 |
| Spark CASE excluding predicates and direct copy | N/A | 0.679 / 0.003 | 0.679 / 0.003 |
| Native slice kernels | 33.417 / 0.368 | N/A | N/A |
| Native concatenation kernel | 27.477 / 0.337 | N/A | N/A |

Each Spark query has 123 CASE calls and 246 predicate calls. The NULL
array check and the interior test evaluate all rows. The physical
interior condition is `1 <= CAST(array_length(arr) AS Int64) - 1`.
It calls ARRAY_LENGTH 123 times and the direct copy 123 times. All
rows in this restricted input enter the interior branch. The native
query invokes 246 slice kernels and 123 concatenation kernels.

The comparison identifies construction of the intermediate array
length values as the first cause. For the existing literal position 2
on BIGINT List columns, an offsets predicate can provide the interior
boolean directly. The CASE still selects NULL parents, empty arrays,
appends and the evaluation of the inserted value.

Nine reversed A/A blocks completed 54 processes and retained 486
query/projection pairs and their raw phase snapshots. Formatting,
strict root-package Clippy and the full Spark workspace suite passed
on identical product and test inputs at `4fd00e77`.

- [Exact preflight and physical plans](array-insert-1516-macos-profile-preflight.json)
- [Phase medians, floors and counts](array-insert-1516-macos-profile-phases.json)
- [Compressed raw samples and snapshots](array-insert-1516-macos-profile-samples.jsonl.gz)
