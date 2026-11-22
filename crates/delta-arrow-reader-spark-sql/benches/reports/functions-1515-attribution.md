# Slowdowns after the final function comparison (#1515)

Removing only the literal-end TIMEDIFF edit recovered 5.41 to 5.83 ns
per input row for the two-column TIMESTAMPDIFF query. The same edit
saved 14.64 to 14.71 ns per row for the TIMEDIFF query with a constant
endpoint. Those paired changes exceeded the observed floors in both
ANSI modes. Both builds returned identical rows and executed types.

For the other 39 comparisons, the original whole-build comparison
reproduced 20 slowdowns. Another 16 fell within the session floor
and 3 became faster. The largest repeated paired increases were
7.10 and 6.25 ns per row for JSON_OBJECT_KEYS. The
[source review](functions-1515-source-attribution.md) found no
retained edits executed by those 39 comparisons.

## Evidence

The [committed protocol](functions-1515-protocol.md) fixes the
eligibility checks, pairing and threshold. The original session
has [3,024 samples](functions-1515-original-1414-samples.jsonl.gz).
The historical repeat admitted 28 statements and collected
[702 samples in 234 processes](functions-1515-repeat-samples.jsonl.gz).
The source isolation admitted both statements and collected
[72 samples in 24 processes](functions-1515-isolate-samples.jsonl.gz).
Every admitted statement passed full-input row and type checks in
both ANSI modes. Every scheduled process completed.

## All 41 original slowdowns

Times and paired changes are nanoseconds per input row above the
projection control. The before and after columns show medians.
The change column is the median of paired changes, so it can differ from the
difference between medians. Positive values mean slower.
Repeat uses the exact original binaries. Isolate compares the
same final source with and without the literal-end change.

| Statement | ANSI | Retained edit runs | Original before to after | Original change | Original floor | Check before to after | Check change | Check floor | Check result |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `BITMAP_CONSTRUCT_AGG` | off | None (repeat) | 1.4676 to 1.4779 | +0.0103 | 0.0092 | 1.4666 to 1.4906 | +0.0230 | 0.0112 | slower |
| `COUNT_MIN_SKETCH` | off | None (repeat) | 5.1146 to 5.2022 | +0.0975 | 0.0117 | 5.1035 to 5.2183 | +0.1041 | 0.0428 | slower |
| `HLL_SKETCH_AGG` | off | None (repeat) | 12.8814 to 13.0527 | +0.2069 | 0.0511 | 12.8786 to 13.0908 | +0.1888 | 0.1023 | slower |
| `HLL_SKETCH_AGG` | on | None (repeat) | 12.9053 to 13.0877 | +0.1939 | 0.0938 | 12.8498 to 13.0829 | +0.2184 | 0.0373 | slower |
| `KURTOSIS` | off | None (repeat) | 4.1175 to 4.1283 | +0.0132 | 0.0081 | 4.1205 to 4.1264 | +0.0058 | 0.0057 | slower |
| `THETA_SKETCH_AGG` | off | None (repeat) | 10.7402 to 10.9209 | +0.1807 | 0.0945 | 10.7586 to 10.9410 | +0.1785 | 0.1210 | slower |
| `THETA_SKETCH_AGG` | on | None (repeat) | 10.7592 to 10.9300 | +0.1874 | 0.0236 | 10.7357 to 10.9474 | +0.2117 | 0.0518 | slower |
| `THETA_UNION_AGG` | on | None (repeat) | 124.3267 to 124.9835 | +0.9458 | 0.6102 | 125.3364 to 124.9186 | -1.4230 | 3.5797 | within floor |
| `POSEXPLODE_OUTER` | on | None (repeat) | 86.6092 to 89.8041 | +3.2919 | 2.7613 | 86.5696 to 87.2915 | +0.7219 | 4.2638 | within floor |
| `MAKE_DT_INTERVAL` | on | None (repeat) | 8.9425 to 8.9792 | +0.0467 | 0.0400 | 8.9805 to 8.9658 | +0.0030 | 0.0289 | within floor |
| `TIME_TRUNC` | off | None (repeat) | 5.8913 to 5.9061 | +0.0160 | 0.0157 | 5.9006 to 5.8618 | -0.0406 | 0.0119 | faster |
| `TIMESTAMPDIFF` | off | TIMEDIFF (isolate) | 71.4587 to 77.2249 | +5.9292 | 1.0453 | 71.3188 to 77.1148 | +5.8260 | 0.3070 | slower |
| `TIMESTAMPDIFF` | on | TIMEDIFF (isolate) | 71.4510 to 77.1987 | +5.8023 | 0.0792 | 71.7610 to 77.1472 | +5.4124 | 0.2554 | slower |
| `WINDOW` | on | None (repeat) | 19.4976 to 20.0443 | +1.0712 | 0.5300 | 19.7687 to 20.0316 | +1.4994 | 1.7517 | within floor |
| `ST_ASBINARY` | off | None (repeat) | -0.0040 to -0.0022 | +0.0015 | 0.0007 | 0.0075 to 0.0059 | -0.0035 | 0.0066 | within floor |
| `JSON_ARRAY_LENGTH` | off | None (repeat) | 36.9073 to 38.4481 | +1.4966 | 0.1471 | 37.0380 to 38.3695 | +1.3681 | 0.2227 | slower |
| `JSON_ARRAY_LENGTH` | on | None (repeat) | 37.0234 to 38.6364 | +1.3755 | 0.1946 | 36.8921 to 38.0332 | +1.1852 | 0.2945 | slower |
| `JSON_OBJECT_KEYS` | off | None (repeat) | 92.1066 to 100.5411 | +8.2976 | 0.5701 | 93.3357 to 100.4236 | +7.1007 | 0.3873 | slower |
| `JSON_OBJECT_KEYS` | on | None (repeat) | 92.8731 to 100.2007 | +8.0453 | 1.1312 | 94.3214 to 100.5679 | +6.2465 | 5.9770 | slower |
| `EXISTS` | off | None (repeat) | 9.8174 to 10.0709 | +0.3644 | 0.0928 | 9.7689 to 10.0846 | +0.3078 | 0.0222 | slower |
| `EXISTS` | on | None (repeat) | 9.8682 to 10.0623 | +0.1941 | 0.1232 | 9.7798 to 10.0922 | +0.3488 | 0.1444 | slower |
| `FORALL` | off | None (repeat) | 12.7901 to 13.1158 | +0.3009 | 0.2427 | 12.8069 to 13.1052 | +0.2876 | 0.0130 | slower |
| `FORALL` | on | None (repeat) | 12.7791 to 13.1553 | +0.3993 | 0.1805 | 12.8263 to 13.1284 | +0.3014 | 2.3038 | within floor |
| `MAP_FROM_ENTRIES` | on | None (repeat) | 136.3022 to 137.3268 | +0.9143 | 0.5224 | 137.1200 to 136.9839 | -0.1361 | 0.2324 | within floor |
| `BIN` | off | None (repeat) | 33.9808 to 34.5474 | +0.4332 | 0.2936 | 34.2792 to 34.3175 | +0.0384 | 0.1337 | within floor |
| `BIN` | on | None (repeat) | 34.1476 to 34.4265 | +0.4752 | 0.1840 | 34.1559 to 34.5451 | +0.3151 | 0.1204 | slower |
| `WIDTH_BUCKET` | on | None (repeat) | 14.7654 to 15.0930 | +0.3594 | 0.1378 | 15.0156 to 15.1646 | -0.0252 | 0.3809 | within floor |
| `HLL_SKETCH_ESTIMATE` | on | None (repeat) | 39.6524 to 39.8820 | +0.2632 | 0.1332 | 39.5248 to 39.7151 | +0.2179 | 0.4680 | within floor |
| `THETA_INTERSECTION` | on | None (repeat) | 164.3448 to 165.6758 | +1.3309 | 0.3382 | 163.9838 to 165.7840 | +1.8002 | 0.2794 | slower |
| `ELT` | off | None (repeat) | 7.5300 to 7.6162 | +0.0862 | 0.0332 | 7.5207 to 7.6120 | +0.0913 | 0.0098 | slower |
| `ELT` | on | None (repeat) | 7.5203 to 7.6242 | +0.0619 | 0.0559 | 7.5204 to 7.6423 | +0.0912 | 0.0612 | slower |
| `QUOTE` | off | None (repeat) | 25.5415 to 25.8032 | +0.2617 | 0.1866 | 26.4625 to 26.4246 | -0.2195 | 0.3123 | within floor |
| `QUOTE` | on | None (repeat) | 25.7895 to 25.9750 | +0.3269 | 0.2591 | 26.0720 to 26.3112 | +0.2392 | 0.3680 | within floor |
| `SOUNDEX` | on | None (repeat) | 22.0177 to 22.1102 | +0.0700 | 0.0614 | 22.1526 to 22.0652 | -0.0848 | 0.1477 | within floor |
| `TRY_VALIDATE_UTF8` | on | None (repeat) | -0.0194 to -0.0124 | +0.0148 | 0.0080 | -0.0231 to -0.0141 | +0.0085 | 0.0059 | slower |
| `VALIDATE_UTF8` | on | None (repeat) | -0.0154 to -0.0070 | +0.0087 | 0.0044 | -0.0021 to -0.0144 | -0.0152 | 0.0087 | faster |
| `TRY_URL_DECODE` | off | None (repeat) | 44.6142 to 45.1972 | +0.5831 | 0.1744 | 45.4446 to 45.1642 | +0.0195 | 0.2889 | within floor |
| `TRY_URL_DECODE` | on | None (repeat) | 44.6101 to 45.1126 | +0.5935 | 0.2817 | 45.4187 to 45.1729 | -0.1594 | 0.3189 | within floor |
| `URL_DECODE` | off | None (repeat) | 44.4151 to 44.8329 | +0.4179 | 0.2051 | 44.6673 to 44.7482 | +0.0782 | 0.2165 | within floor |
| `URL_DECODE` | on | None (repeat) | 44.4443 to 44.6407 | +0.2973 | 0.1651 | 44.5119 to 45.0726 | +0.5655 | 0.2299 | slower |
| `TRY_PARSE_JSON` | on | None (repeat) | 525.0972 to 530.3656 | +5.2684 | 3.3155 | 537.5997 to 530.0101 | -7.0228 | 4.4723 | faster |

## Literal-end control

| Statement | ANSI | Without to with edit | Paired change | Floor | Result |
| --- | --- | ---: | ---: | ---: | --- |
| `TIMEDIFF` | off | 41.6959 to 27.1077 | -14.6387 | 0.4441 | faster |
| `TIMEDIFF` | on | 41.8666 to 27.1270 | -14.7056 | 0.1580 | faster |

The follow-up in #1515 is confined to the shared time-difference
implementation. The repeat records the whole-build shifts for the
maintainer.
