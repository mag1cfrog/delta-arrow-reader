# SF1 string function recheck (#1388)

One Linux release session measured the three isolated statements against
native and both ANSI modes. All three returned the same 6,001,215
rows, SQL types and executed Arrow types. For each shape the three
paths read the same files and byte counts. Each timing path contains
six process medians from three retained iterations per process.

| Shape | Native ms [min, max] | ANSI off ms [min, max] | ANSI on ms [min, max] | Native A / B ms | Floor ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| substring_only | 183.000 [178.044, 191.542] | 194.008 [169.035, 201.256] | 188.749 [182.585, 195.257] | 186.177 / 183.002 | 3.175 |
| concat_only | 127.114 [126.056, 127.275] | 130.227 [127.042, 130.780] | 130.474 [129.581, 131.453] | 126.672 / 126.592 | 0.080 |
| upper_only | 224.042 [216.793, 231.580] | 222.162 [199.015, 230.871] | 219.177 [212.332, 230.682] | 219.705 / 226.423 | 6.718 |

| Shape | Native ns/row | ANSI off ns/row | ANSI on ns/row | Floor ns/row |
| --- | ---: | ---: | ---: | ---: |
| substring_only | 30.494 | 32.328 | 31.452 | 0.529 |
| concat_only | 21.181 | 21.700 | 21.741 | 0.013 |
| upper_only | 37.333 | 37.019 | 36.522 | 1.119 |

## Attribution

SUBSTRING exceeds native by 11.008 ms with ANSI off and 5.749 ms
with ANSI on. The paired ANSI-on minus ANSI-off deltas are
-14.810, +3.793, -12.199, -5.999, +2.891, +19.814 ms. Both modes execute the same physical plan.
The sign changes across replicates, so the session does not reproduce
a consistent ANSI mode difference.

The one ANSI-off profile samples the native
[short-prefix character bounds](../../vendor/dependencies/datafusion-functions/src/unicode/substr.rs)
and view construction inside `SubstrFunc`. Sail's
[wrapper](../../vendor/sail/crates/sail-function/src/scalar/string/spark_substring.rs)
delegates this prefix shape to the same implementation as native.
Spark also has a result naming projection. The profile identifies
character iteration inside the shared kernel as a per-row cost that
the Spark prefix path can avoid. It does not assign the entire
Spark-to-native gap to that work.

CONCAT exceeds native by 3.113 ms with ANSI off and 3.360 ms with ANSI
on. The one ANSI-on profile samples the
[encoded-length guard](../../vendor/sail/crates/sail-function/src/scalar/collection/spark_concat.rs)
in `can_concat_views_with_utf8_limits`. It checks the remaining Utf8
offset capacity after every view before calling the same concat
builder used by native. Preserve what the guard counts and retain the
old path outside a proven safe range.

UPPER's medians are below native in both modes. It uses the same
function on both paths and did not trigger a profile.

## Receipt

The [protocol](../strings_round2_1388/protocol.md) was committed at
`84c8e23a23de5aacfce0a8836eb5a855784f55b7` before clocks. The product source
was `2bd5002527a7156789115abffb440b96d8af2dc1`, and the executable SHA256
was `cb83cc7b345f40d81c6954889a71d8360637fb226799de0957a74c418da1eded`. Clocks ran from
2026-11-21T12:23:13Z to 2026-11-21T12:24:23Z. The 90 processes
retained 270 samples. The two profiles, complete outputs and source
counters are under `costs-168-2026-11-20/1388/attribution`.
