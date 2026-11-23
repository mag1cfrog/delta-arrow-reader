# Literal second-position BIGINT array insert (#1516)

For literal position 2 and BIGINT List and value columns, the
interior insertion branch copies the first element, inserts the new
value and copies the suffix. The original CASE still selects the
branch and evaluates the input values. This removes the intermediate
left slice, singleton list, right slice and concatenation on that
branch.

| Path | Staging median [range] | Candidate median [range] | Median paired change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 173.976 [172.729, 181.666] | 86.678 [85.632, 87.086] | -87.134 |
| Spark ANSI on | 173.227 [172.940, 174.212] | 86.080 [85.669, 86.178] | -87.252 |
| Native | 80.559 [80.187, 82.471] | 80.395 [80.154, 81.172] | -0.153 |

Times are ns per input row above projection. Each range spans nine
samples, and the paired change is the median of three process
differences. Both Spark modes improved in every pair. Their median
reductions exceed the 0.330 ns native A/A floor. All three native
pairs also improved, so no layout exception is needed. Spark remains
6.283 / 5.685 ns per row above native in ANSI off / on. The Spark
path still evaluates the original length and CASE conditions. The
whole-expression timing does not separate their cost.

## Validation and evidence

Full-input checks matched all one million rows and their executed
types within each configuration across builds. On the restricted
937,500-row input, native and both ANSI modes agreed on all rows
and executed types. [Full-input checks](../functions_1516/array-insert-full-results.json)

The first implementation replaced the full CASE. A four-row
reproduction showed that it allowed the optimizer to move a derived
value cast under the NULL guard, suppressing a failure in ANSI mode.
The revised code retains the length branches and their value
references. Both staging and the sealed final build reproduce the
ANSI-on failure and ANSI-off NULL behavior. The SQL differential
covers the dynamic position comparison, empty and nullable lists,
nullable values and the derived cast. A direct Arrow test retains a
nonzero child offset and checks the named child field.

Formatting, root Clippy and the complete Spark workspace passed
with 3,310 tests passed and three ignored. Independent review checked
the preserved branch ordering, the regression and the Arrow test.
The first implementation was excluded before any timing.

The [protocol](../functions_1516/array-insert-protocol.md) was
committed at `fe7f2e0e`, and the
[build pins](../functions_1516/array-insert.json) at `319c2861`
before timing. All 24 processes completed, retaining
[72 raw samples](../functions_1516/array-insert-samples.jsonl.gz)
from 2026-11-23 06:35:52 UTC to 06:36:22 UTC.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Staging | `f80ea0275277e51a896991c18b0b2ec475c706d5` | `f9a61cb1a05d0ab5afe2718ea6c5db72dfd0a8f661eb90c8b43f3174859bad50` |
| Candidate | `55f48e2860f45d9a5bef99f8f7b5057e5d4f7e20` | `da6ac45a30760c714b9f3c736e6a0173f93dae8af8519e7e31d71c7f79967643` |
