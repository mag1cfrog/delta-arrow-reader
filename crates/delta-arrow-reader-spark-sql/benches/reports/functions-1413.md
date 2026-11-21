# Remaining paired function costs (#1413)

This round profiled 17 groups covering 37 Spark functions with a native
reference. The [attribution report](functions-1413-attribution.md) links
each function to its measured execution path. All 37 cases matched complete
rows and executed types in native and both ANSI modes before timing.

Nine changes to the Spark path met their retention rules, covering 18
functions. SPLIT fell from about 89 to 29 ns per input row, three regex
functions from about 10.4 to 4.7, and YEAR from about 12.8 to 5.1. Each
case used one million input rows. At the end of the round, 19 functions
still exceed their native comparison floor in at least one ANSI mode.
LIKE has the largest residual at 4.62 ns per input row. #1413 remains open.

## Before and after

Each row uses a matched Linux release session with both builds. Before is
the staging arm for that function's comparison. Retained is the
candidate for an accepted change and the staging arm for a rejected
one. The six final retained-path cases compare the starting and final
product binaries directly. Native is the median from the retained arm.
Floor is the same-session candidate native A/A floor. Gap is retained
Spark minus retained native. Each value is a median ns per input row
above the plain projection.

| Function | Before off / on | Retained off / on | Native | Floor | Gap off / on | Disposition |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `split` | 88.8965 / 88.6708 | 28.8824 / 28.9036 | 29.3831 | 0.1432 | -0.5007 / -0.4795 | [Direct literal splitter](functions-1413-split.md) |
| `lpad` | 18.4337 / 18.3232 | 13.2482 / 13.2227 | 13.3493 | 0.1922 | -0.1011 / -0.1266 | [Removed input casts](functions-1413-string-view-casts.md) |
| `overlay` | 45.2902 / 45.3926 | 37.1778 / 37.3123 | 37.2153 | 0.4683 | -0.0375 / 0.0970 | [Removed input casts](functions-1413-string-view-casts.md) |
| `rpad` | 19.4295 / 19.6366 | 14.6903 / 14.7141 | 14.6998 | 0.1170 | -0.0095 / 0.0143 | [Removed input casts](functions-1413-string-view-casts.md) |
| `regexp` | 10.3972 / 10.3988 | 4.6542 / 4.6437 | 4.7655 | 0.0425 | -0.1113 / -0.1219 | [Literal LIKE path](functions-1413-regexp-literal.md) |
| `regexp_like` | 10.3934 / 10.4291 | 4.6490 / 4.6341 | 4.7105 | 0.0577 | -0.0615 / -0.0764 | [Literal LIKE path](functions-1413-regexp-literal.md) |
| `rlike` | 10.3984 / 10.3843 | 4.6920 / 4.6530 | 4.7644 | 0.0249 | -0.0724 / -0.1114 | [Literal LIKE path](functions-1413-regexp-literal.md) |
| `char` | 5.3501 / 5.3128 | 5.3501 / 5.3128 | 5.3737 | 0.0900 | -0.0237 / -0.0609 | [Alias trial rejected](functions-1413-result-aliases.md) |
| `repeat` | 42.2492 / 42.2376 | 42.2492 / 42.2376 | 42.2939 | 0.2800 | -0.0448 / -0.0564 | [Alias trial rejected](functions-1413-result-aliases.md) |
| `replace` | 24.4955 / 24.1064 | 24.4955 / 24.1064 | 24.5199 | 0.3351 | -0.0244 / -0.4135 | [Alias trial rejected](functions-1413-result-aliases.md) |
| `right` | 20.8035 / 20.8268 | 20.8035 / 20.8268 | 20.8457 | 0.1191 | -0.0422 / -0.0190 | [Alias trial rejected](functions-1413-result-aliases.md) |
| `row_number (window)` | 127.0405 / 128.5529 | 127.0405 / 128.5529 | 127.4675 | 1.3757 | -0.4270 / 1.0854 | [Alias trial rejected](functions-1413-result-aliases.md) |
| `stddev_samp (window)` | 318.2265 / 315.8504 | 318.2265 / 315.8504 | 317.1531 | 1.3566 | 1.0734 / -1.3028 | [Alias trial rejected](functions-1413-result-aliases.md) |
| `year` | 12.7607 / 12.7527 | 5.0587 / 5.0751 | 5.8237 | 0.1345 | -0.7650 / -0.7485 | [Faster Date32 extraction](functions-1413-year-extraction.md) |
| `ilike` | 9.4095 / 9.3653 | 3.7600 / 3.7716 | 3.6950 | 0.0217 | 0.0650 / 0.0766 | [ASCII prefix path](functions-1413-ilike-prefix.md) |
| `like` | 6.2178 / 6.2287 | 6.1537 / 6.1382 | 1.5288 | 0.0127 | 4.6249 / 4.6094 | [Simplified literal preparation](functions-1413-like-prefix.md) |
| `ln` | 6.2751 / 6.2593 | 5.9664 / 5.9856 | 5.3172 | 0.0171 | 0.6492 / 0.6684 | [Shared values under a validity mask](functions-1413-log-domain.md) |
| `log` | 6.7099 / 6.7751 | 6.3307 / 6.2976 | 5.6526 | 0.0636 | 0.6782 / 0.6450 | [Shared values under a validity mask](functions-1413-log-domain.md) |
| `log10` | 7.8670 / 7.8824 | 7.6498 / 7.6583 | 6.9537 | 0.0235 | 0.6961 / 0.7045 | [Shared values under a validity mask](functions-1413-log-domain.md) |
| `log1p` | 6.5202 / 6.5097 | 6.3588 / 6.3375 | 5.7126 | 0.0755 | 0.6462 / 0.6249 | [Shared values under a validity mask](functions-1413-log-domain.md) |
| `log2` | 5.8603 / 5.8807 | 5.7148 / 5.7300 | 5.0222 | 0.1334 | 0.6926 / 0.7078 | [Shared values under a validity mask](functions-1413-log-domain.md) |
| `median` | 4.9545 / 4.8129 | 3.6379 / 3.6561 | 3.0642 | 0.0308 | 0.5736 / 0.5919 | [One batch count check](functions-1413-percentile-count.md) |
| `percentile` | 5.2228 / 5.1317 | 4.0356 / 3.7526 | 4.2136 | 0.8825 | -0.1780 / -0.4611 | [One batch count check](functions-1413-percentile-count.md) |
| `unbase64` | 24.4218 / 24.4206 | 19.7578 / 19.7778 | 21.6626 | 0.1304 | -1.9048 / -1.8848 | [Reused decode buffer](functions-1413-unbase64-buffer.md) |
| `grouping` | 14.5787 / 14.5624 | 14.5787 / 14.5624 | 13.8588 | 0.0846 | 0.7199 / 0.7036 | [Presence-filter trial rejected](functions-1413-grouping-presence.md) |
| `grouping_id` | 14.6478 / 14.4760 | 14.6478 / 14.4760 | 13.8446 | 0.0353 | 0.8031 / 0.6313 | [Presence-filter trial rejected](functions-1413-grouping-presence.md) |
| `min_by` | 2.5207 / 2.5505 | 2.5207 / 2.5505 | 2.3948 | 0.0828 | 0.1259 / 0.1557 | [True-filter trial below floor](functions-1413-min-by-key.md) |
| `regr_intercept` | 4.5378 / 4.5383 | 4.5378 / 4.5383 | 4.5263 | 0.0152 | 0.0116 / 0.0120 | [Shared accumulator trial rejected](functions-1413-regr-moments.md) |
| `regr_slope` | 4.5182 / 4.5325 | 4.5182 / 4.5325 | 4.5048 | 0.0176 | 0.0134 / 0.0277 | [Shared accumulator trial rejected](functions-1413-regr-moments.md) |
| `regr_sxx` | 4.5108 / 4.5603 | 4.5108 / 4.5603 | 4.4900 | 0.0340 | 0.0208 / 0.0703 | [Shared accumulator trial rejected](functions-1413-regr-moments.md) |
| `regr_syy` | 4.5375 / 4.5432 | 4.5375 / 4.5432 | 4.5257 | 0.0151 | 0.0117 / 0.0174 | [Shared accumulator trial rejected](functions-1413-regr-moments.md) |
| `day` | 6.3542 / 6.3429 | 6.3740 / 6.3247 | 6.2167 | 0.0736 | 0.1573 / 0.1080 | [Same date-part path](#remaining-paths) |
| `dayofyear` | 5.9831 / 5.9476 | 6.0595 / 5.9734 | 5.8543 | 0.0242 | 0.2051 / 0.1191 | [Same date-part path](#remaining-paths) |
| `weekofyear` | 10.1694 / 10.2981 | 10.3416 / 10.3555 | 10.2434 | 0.0420 | 0.0982 / 0.1121 | [Same date-part path](#remaining-paths) |
| `years` | 5.8855 / 5.9239 | 5.9461 / 5.9268 | 5.8209 | 0.0207 | 0.1251 / 0.1058 | [Same date-part path](#remaining-paths) |
| `character_length` | 2.0851 / 2.0748 | 2.0798 / 2.0680 | 2.0667 | 0.0125 | 0.0130 / 0.0013 | [Same character-count work](#remaining-paths) |
| `rtrim` | 9.4564 / 9.5089 | 9.5152 / 9.5200 | 9.5419 | 0.0500 | -0.0268 / -0.0219 | [Adapter retained for executed fields](#remaining-paths) |

## Remaining paths

- **RTRIM.** The adapter calls DataFusion's existing RtrimFunc and tracks
  the argument field's nullability. Removing it produced identical rows
  on the million-row fixture. A LEFT JOIN filtered on the right key
  changes to an INNER JOIN later in planning. The trial then reported a
  nullable output for a required column. The trial failed the output-field
  check before clocks and the adapter remains. [Reproduction and trial](https://github.com/mag1cfrog/delta-arrow-reader/commit/5857f465053e291a0a4ad2816b518eb91ffbe811).
- **DAY, DAYOFYEAR, WEEKOFYEAR and YEARS.** Both engines use
  `DatePartFunc::invoke_with_args` from DataFusion 54.1.0 and the same
  Arrow date kernels. Unit normalization happens once per batch in both
  paths. The same result projection also appeared in the
  [alias trial](functions-1413-result-aliases.md). The profile could not
  separate its cost from the shared date kernel.
- **CHARACTER_LENGTH.** The frozen Utf8 paths in SparkLengthFunc and
  CharacterLengthFunc both check ASCII once per batch and count bytes or
  Unicode characters per value. Spark's implementation also handles
  binary values and returns Int32 for LargeUtf8. The comparison did not
  isolate extra Utf8 row work that could be removed.

The linked trial reports describe the other residuals. ILIKE retains
its ASCII check and Unicode fallback. LIKE retains the Spark predicate
that evaluates local failures. The logarithms retain their domain mask.
MEDIAN retains the numeric frequency table. GROUPING retains the count
and filter that determine empty grouping-set output. MIN_BY keeps its
ordered reducer, and the regression functions keep their full
accumulators. The timings measure the gaps for these complete paths.

## Final retained-path session

The [six-case protocol](../remaining_functions_1413/retained-paths-protocol.md)
commits the original #1413 binary and the final retained binary. Each
of the 36 preflight executions matched the expected million rows and
executed types. The session retained 108 paired processes and 36 native
A/A processes, with three recorded samples per process. The measured
RTRIM gap fell within the floor in both ANSI modes with the adapter in
place.

- Protocol commit `ccbf0a4517329ea2eba1754b0f4d249e0bcd573c`.
- Starting product source `a044f1e58310f71195073f0a6616cb10693aaef1`.
- Retained product source `e1580ef0056bdfe70161fe07be94ed10957ed7ec`.
- Raw results and process timings are retained locally under `costs-168-2026-11-20/1413`.
