# Spark-only function measurements (#1414)

The source review covers 113 statement forms without a native reference.
It found batch-constant work in 30 forms. The retained changes remove
that work in 18 forms. Fifteen are faster in both ANSI modes under the
paired rule. THETA_UNION, HLL_UNION and MONTHS_BETWEEN remove the named
work with timings within the comparison floor. The source table records
what each function does per row. [Source inventory](functions-1414-inventory.md)

## Reading the comparisons

Values are median [minimum, maximum] across three process medians, in ns
per input row above the statement's plain projection. Each before and
retained pair comes from the same Linux release session. The 28 individual
trial rows link to their own timing evidence. Where a trial was rejected
or withdrawn, the retained number repeats the observed staging reference
from that trial because its original evaluation remains in use. The trial
report preserves the candidate's measured result.

The final session compares the initial #1414 product with the last staged
product across the remaining 85 frozen shapes. It verifies complete rows,
executed types and source guards before each case can enter timing.
[Final comparison protocol](../spark_only_functions_1414/final-coverage-protocol.md)

Eighty-four cases completed all pairs in the final session, with 1,008
processes and 3,024 retained observations. The unseeded `SHUFFLE(arr)`
statement produced different array orders across its two ANSI preflights
on the original binary. Both executions returned one million rows of
the same types. The case was excluded before final-build preflight and
clocks. Together with the individual trials this gives 112 paired rows
and one explicit exclusion. The final session took 31 minutes and 12
seconds. Raw records are available locally under
`costs-168-2026-11-20/1414/final-coverage`.

## All 113 statement forms

| Function | ANSI off before / retained | ANSI on before / retained | Evidence |
| --- | ---: | ---: | --- |
| `from_xml` (scalar) | 1442.316 [1440.211, 1468.687] / 1442.316 [1440.211, 1468.687] | 1439.143 [1431.424, 1439.882] / 1439.143 [1431.424, 1439.882] | [Trial rejected](functions-1414-from-xml.md) |
| `schema_of_variant` (scalar) | 959.594 [957.944, 971.061] / 953.161 [950.413, 954.743] | 955.228 [954.385, 1005.676] / 959.628 [954.974, 984.617] | Final comparison |
| `variant_explode` (generator) | 878.165 [876.411, 891.678] / 863.999 [859.097, 871.440] | 870.841 [870.538, 873.969] / 866.583 [857.710, 869.503] | Final comparison |
| `variant_explode_outer` (generator) | 877.286 [875.398, 882.431] / 863.866 [858.419, 868.423] | 874.022 [871.708, 885.766] / 864.737 [859.477, 865.226] | Final comparison |
| `variant_to_json` (scalar) | 868.907 [862.403, 878.532] / 865.924 [863.825, 866.436] | 864.142 [863.116, 872.405] / 862.375 [858.487, 863.756] | Final comparison |
| `schema_of_variant_agg` (aggregate) | 769.886 [765.421, 778.910] / 769.600 [754.264, 773.701] | 775.880 [767.202, 787.853] / 759.812 [759.354, 761.057] | Final comparison |
| `variant_get` (scalar) | 665.222 [662.512, 670.467] / 658.555 [653.807, 678.836] | 674.786 [669.608, 677.295] / 656.595 [652.623, 659.000] | Final comparison |
| `try_variant_get` (scalar) | 664.000 [663.088, 678.182] / 654.736 [648.025, 655.662] | 663.600 [663.111, 664.225] / 651.636 [647.654, 658.767] | Final comparison |
| `is_variant_null` (scalar) | 603.758 [602.373, 605.123] / 598.707 [588.025, 601.570] | 599.906 [598.162, 607.630] / 589.525 [588.372, 591.032] | Final comparison |
| `parse_json` (scalar) | 550.166 [535.543, 551.451] / 537.965 [520.147, 545.146] | 538.971 [537.612, 542.042] / 525.172 [523.334, 543.994] | Final comparison |
| `try_to_number` (scalar) | 508.887 [508.796, 509.072] / 508.887 [508.796, 509.072] | 512.760 [509.252, 559.789] / 512.760 [509.252, 559.789] | [Withdrawn after parity check](functions-1414-string-offset-boundary.md) |
| `try_parse_json` (scalar) | 526.450 [521.674, 526.579] / 531.154 [527.713, 538.743] | 525.097 [524.078, 530.709] / 530.366 [530.068, 532.301] | Final comparison |
| `to_number` (scalar) | 508.989 [508.605, 509.017] / 508.989 [508.605, 509.017] | 509.852 [508.713, 510.158] / 509.852 [508.713, 510.158] | [Withdrawn after parity check](functions-1414-string-offset-boundary.md) |
| `from_csv` (scalar) | 512.061 [507.952, 513.148] / 512.061 [507.952, 513.148] | 509.165 [509.090, 513.640] / 509.165 [509.090, 513.640] | [Withdrawn after parity check](functions-1414-string-offset-boundary.md) |
| `str_to_map` (scalar) | 319.819 [316.331, 324.596] / 201.733 [201.680, 202.497] | 314.163 [312.769, 314.654] / 202.133 [201.335, 203.112] | [Retained](functions-1414-str-to-map.md) |
| `map_concat` (scalar) | 375.129 [371.832, 377.671] / 376.262 [375.708, 378.627] | 374.829 [374.504, 377.309] / 380.394 [376.242, 385.921] | Final comparison |
| `json_tuple` (scalar) | 245.947 [241.773, 246.875] / 241.704 [241.075, 242.614] | 246.162 [242.632, 247.667] / 241.471 [241.146, 241.471] | Final comparison |
| `from_json` (scalar) | 236.320 [236.119, 238.110] / 236.320 [236.119, 238.110] | 236.381 [236.256, 250.954] / 236.381 [236.256, 250.954] | [Withdrawn after parity check](functions-1414-string-offset-boundary.md) |
| `theta_intersection_agg` (aggregate) | 226.716 [226.063, 227.199] / 229.265 [228.584, 235.951] | 228.565 [227.915, 229.339] / 228.816 [228.148, 229.618] | Final comparison |
| `try_parse_url` (scalar) | 180.956 [180.478, 181.403] / 180.956 [180.478, 181.403] | 181.817 [180.240, 188.358] / 181.817 [180.240, 188.358] | [Withdrawn after parity check](functions-1414-string-offset-boundary.md) |
| `to_variant_object` (scalar) | 175.671 [174.661, 178.545] / 178.416 [174.709, 218.199] | 176.110 [175.142, 182.202] / 176.377 [175.120, 186.176] | Final comparison |
| `parse_url` (scalar) | 177.761 [176.703, 179.117] / 177.761 [176.703, 179.117] | 177.703 [177.671, 178.602] / 177.703 [177.671, 178.602] | [Withdrawn after parity check](functions-1414-string-offset-boundary.md) |
| `get_json_object` (scalar) | 174.312 [172.530, 189.852] / 172.183 [171.813, 172.317] | 173.247 [172.567, 191.059] / 173.527 [172.259, 175.903] | Final comparison |
| `theta_intersection` (scalar) | 165.436 [163.342, 167.813] / 164.817 [164.781, 166.034] | 164.345 [164.098, 164.775] / 165.676 [165.478, 165.858] | Final comparison |
| `to_char` (scalar) | 153.072 [152.192, 153.984] / 153.072 [152.192, 153.984] | 154.161 [152.916, 161.642] / 154.161 [152.916, 161.642] | [Trial rejected](functions-1414-to-char.md) |
| `to_varchar` (scalar) | 153.923 [153.115, 155.384] / 153.923 [153.115, 155.384] | 153.543 [153.110, 155.416] / 153.543 [153.110, 155.416] | [Trial rejected](functions-1414-to-char.md) |
| `regexp_extract_all` (scalar) | 143.731 [143.367, 144.165] / 143.731 [143.367, 144.165] | 145.090 [142.965, 147.070] / 145.090 [142.965, 147.070] | [Trial rejected](functions-1414-regexp-extract-all.md) |
| `sentences` (scalar) | 136.543 [133.565, 141.746] / 130.055 [129.047, 130.349] | 133.460 [133.246, 133.828] / 130.446 [130.215, 130.701] | Final comparison |
| `theta_difference` (scalar) | 139.391 [138.180, 139.895] / 138.303 [137.930, 138.670] | 138.408 [138.390, 139.069] / 138.780 [138.513, 143.774] | Final comparison |
| `hll_union_agg` (aggregate) | 134.326 [133.979, 134.539] / 132.592 [131.366, 136.010] | 132.800 [132.602, 134.682] / 131.686 [131.332, 132.200] | Final comparison |
| `theta_union` (scalar) | 134.761 [134.725, 135.595] / 137.271 [135.948, 144.829] | 136.600 [135.528, 141.302] / 136.059 [135.588, 137.353] | [Retained](functions-1414-theta-union.md) |
| `map_from_entries` (scalar) | 137.159 [136.881, 144.486] / 136.462 [135.843, 137.641] | 136.302 [135.819, 136.864] / 137.327 [137.217, 137.774] | Final comparison |
| `theta_union_agg` (aggregate) | 124.310 [123.342, 124.483] / 123.581 [123.567, 124.702] | 124.327 [123.735, 124.823] / 124.983 [124.681, 125.901] | Final comparison |
| `format_string` (scalar) | 118.475 [113.789, 119.744] / 114.969 [114.858, 115.802] | 118.456 [112.879, 118.829] / 115.746 [115.086, 116.400] | Final comparison |
| `printf` (scalar) | 118.442 [115.349, 118.781] / 115.198 [114.563, 115.249] | 118.021 [113.691, 119.037] / 115.606 [115.195, 124.139] | Final comparison |
| `to_json` (scalar) | 121.364 [120.742, 123.901] / 113.001 [110.430, 117.601] | 121.692 [120.163, 122.198] / 110.612 [110.100, 114.202] | Final comparison |
| `hll_union` (scalar) | 102.621 [102.492, 102.727] / 102.100 [101.154, 102.675] | 103.710 [103.527, 105.031] / 102.526 [102.279, 107.875] | [Retained](functions-1414-hll-union.md) |
| `to_xml` (scalar) | 97.695 [96.662, 98.543] / 97.465 [97.363, 98.846] | 98.055 [97.503, 101.110] / 97.459 [97.256, 98.142] | Final comparison |
| `json_object_keys` (scalar) | 92.107 [91.890, 93.030] / 100.541 [100.404, 100.939] | 92.873 [91.500, 93.762] / 100.201 [100.128, 100.918] | Final comparison |
| `format_number` (scalar) | 91.138 [90.574, 93.676] / 90.397 [90.250, 90.481] | 90.456 [90.298, 90.733] / 90.307 [90.283, 90.375] | Final comparison |
| `sha` (scalar) | 88.534 [87.145, 88.723] / 88.469 [88.296, 88.575] | 87.514 [87.334, 89.727] / 88.421 [87.090, 94.560] | Final comparison |
| `sha1` (scalar) | 87.647 [87.159, 88.462] / 87.134 [87.129, 88.744] | 88.598 [87.286, 89.085] / 87.247 [87.076, 87.289] | Final comparison |
| `posexplode_outer` (generator) | 86.565 [86.474, 87.340] / 86.750 [86.445, 86.794] | 86.609 [86.512, 87.214] / 89.804 [87.793, 93.315] | Final comparison |
| `explode_outer` (generator) | 76.377 [75.625, 76.768] / 76.149 [76.042, 77.054] | 76.062 [75.683, 76.819] / 76.665 [76.516, 77.147] | Final comparison |
| `timestampdiff` (scalar) | 71.459 [71.296, 71.515] / 77.225 [77.069, 79.160] | 71.451 [71.316, 71.475] / 77.199 [77.143, 77.277] | Final comparison |
| `timestampadd` (scalar) | 66.108 [65.858, 66.303] / 66.166 [65.877, 66.383] | 66.478 [66.030, 66.611] / 66.805 [66.012, 68.214] | Final comparison |
| `to_csv` (scalar) | 61.020 [60.731, 63.034] / 61.261 [61.149, 61.708] | 62.399 [62.278, 62.584] / 61.289 [61.009, 61.724] | Final comparison |
| `make_timestamp_ltz` (scalar) | 59.394 [58.993, 59.441] / 43.332 [43.224, 43.430] | 59.380 [59.092, 59.558] / 43.233 [43.226, 43.353] | [Retained](functions-1414-make-timestamp.md) |
| `try_make_timestamp_ltz` (scalar) | 59.203 [59.189, 59.335] / 43.383 [43.305, 43.438] | 59.292 [59.250, 59.472] / 43.241 [43.209, 43.275] | [Retained](functions-1414-make-timestamp.md) |
| `make_timestamp` (scalar) | 59.273 [59.078, 59.692] / 43.318 [43.198, 44.297] | 59.053 [59.043, 59.137] / 43.439 [43.310, 43.696] | [Retained](functions-1414-make-timestamp.md) |
| `try_make_timestamp` (scalar) | 59.448 [59.246, 60.063] / 43.329 [43.217, 43.455] | 59.657 [59.281, 60.099] / 43.406 [43.195, 43.526] | [Retained](functions-1414-make-timestamp.md) |
| `shuffle` (scalar) | Excluded | Excluded | spark-on: result rows differ from reference |
| `months_between` (scalar) | 55.233 [54.986, 57.387] / 54.595 [54.594, 56.546] | 54.723 [54.629, 56.995] / 54.481 [54.237, 57.315] | [Retained](functions-1414-months-between.md) |
| `reduce` (scalar) | 51.877 [49.729, 53.780] / 51.587 [50.871, 52.752] | 54.119 [54.056, 54.827] / 53.747 [49.447, 54.087] | Final comparison |
| `aggregate` (scalar) | 52.207 [50.691, 53.488] / 50.548 [48.817, 51.347] | 51.238 [50.930, 54.075] / 54.459 [50.112, 54.750] | Final comparison |
| `try_url_decode` (scalar) | 44.614 [44.429, 44.777] / 45.197 [45.095, 45.391] | 44.610 [44.519, 45.083] / 45.113 [45.100, 45.295] | Final comparison |
| `url_decode` (scalar) | 44.415 [44.408, 44.544] / 44.833 [44.638, 45.048] | 44.444 [44.343, 44.674] / 44.641 [44.578, 44.746] | Final comparison |
| `timediff` (scalar) | 42.142 [42.118, 42.212] / 27.119 [27.072, 27.202] | 41.995 [41.813, 42.162] / 27.138 [27.077, 27.159] | [Retained](functions-1414-timediff.md) |
| `posexplode` (generator) | 38.456 [26.418, 40.142] / 38.956 [37.824, 39.284] | 38.727 [38.581, 38.731] / 39.474 [36.899, 39.476] | Final comparison |
| `hll_sketch_estimate` (scalar) | 40.191 [39.835, 41.486] / 39.707 [39.596, 39.845] | 39.652 [39.528, 39.715] / 39.882 [39.649, 39.916] | Final comparison |
| `conv` (scalar) | 37.948 [37.849, 37.996] / 37.841 [37.792, 37.951] | 38.228 [37.988, 40.328] / 37.924 [37.776, 38.005] | Final comparison |
| `json_array_length` (scalar) | 36.907 [36.683, 36.977] / 38.448 [38.404, 38.621] | 37.023 [36.938, 37.266] / 38.636 [38.252, 38.641] | Final comparison |
| `url_encode` (scalar) | 36.809 [36.730, 36.869] / 36.884 [36.856, 36.896] | 36.819 [36.669, 36.941] / 37.117 [37.050, 37.937] | Final comparison |
| `mask` (scalar) | 36.862 [36.793, 36.961] / 35.691 [35.675, 35.943] | 36.736 [36.603, 36.890] / 35.658 [35.456, 35.711] | Final comparison |
| `bin` (scalar) | 33.981 [33.951, 34.114] / 34.547 [34.171, 34.758] | 34.148 [33.856, 34.148] / 34.426 [34.331, 34.699] | Final comparison |
| `theta_sketch_estimate` (scalar) | 31.705 [31.522, 31.716] / 31.535 [31.495, 31.599] | 31.672 [31.519, 31.728] / 31.598 [31.508, 31.671] | Final comparison |
| `crc32` (scalar) | 30.941 [30.916, 31.049] / 31.031 [31.004, 31.242] | 30.948 [30.936, 30.967] / 30.985 [30.975, 31.288] | Final comparison |
| `next_day` (scalar) | 27.050 [27.023, 27.125] / 27.051 [26.980, 27.148] | 27.131 [26.988, 27.151] / 27.151 [27.080, 27.156] | Final comparison |
| `bitmap_and_agg` (aggregate) | 26.307 [24.619, 32.868] / 26.357 [26.150, 26.382] | 26.130 [24.673, 26.285] / 26.155 [26.106, 32.146] | Final comparison |
| `quote` (scalar) | 25.542 [25.465, 25.603] / 25.803 [25.675, 26.049] | 25.790 [25.648, 25.796] / 25.975 [25.636, 26.155] | Final comparison |
| `histogram_numeric` (aggregate) | 25.033 [24.574, 25.320] / 24.845 [24.522, 24.898] | 24.838 [24.377, 24.843] / 24.574 [24.569, 24.893] | Final comparison |
| `try_make_interval` (scalar) | 22.536 [22.517, 22.589] / 21.481 [21.320, 21.545] | 22.606 [22.398, 22.772] / 21.535 [21.440, 21.561] | [Retained](functions-1414-make-interval.md) |
| `make_interval` (scalar) | 22.619 [22.421, 22.697] / 21.537 [21.532, 21.631] | 22.522 [22.450, 22.549] / 21.516 [21.236, 21.554] | [Retained](functions-1414-make-interval.md) |
| `make_timestamp_ntz` (scalar) | 23.109 [22.885, 23.182] / 7.954 [4.579, 8.571] | 22.914 [22.903, 26.448] / 8.107 [7.877, 8.446] | [Retained](functions-1414-make-timestamp.md) |
| `try_make_timestamp_ntz` (scalar) | 22.892 [22.823, 22.951] / 8.360 [8.286, 8.448] | 22.958 [22.847, 22.998] / 7.827 [4.551, 8.362] | [Retained](functions-1414-make-timestamp.md) |
| `soundex` (scalar) | 22.031 [21.995, 22.130] / 22.054 [22.003, 22.072] | 22.018 [21.982, 22.105] / 22.110 [22.052, 22.144] | Final comparison |
| `st_geogfromwkb` (scalar) | 20.650 [20.619, 21.607] / 20.621 [20.424, 20.676] | 20.581 [20.543, 20.736] / 20.558 [20.439, 20.867] | Final comparison |
| `window` (scalar) | 20.389 [18.484, 20.830] / 20.632 [19.690, 21.252] | 19.498 [18.973, 20.003] / 20.044 [19.564, 20.624] | Final comparison |
| `st_geomfromwkb` (scalar) | 20.295 [20.005, 20.480] / 20.092 [19.896, 20.204] | 20.313 [20.283, 20.380] / 20.095 [19.852, 20.433] | Final comparison |
| `window_time` (scalar) | 17.657 [16.982, 17.750] / 14.745 [14.473, 15.745] | 16.223 [15.329, 17.652] / 16.209 [15.128, 18.210] | Final comparison |
| `width_bucket` (scalar) | 14.917 [14.797, 15.075] / 15.108 [15.009, 15.131] | 14.765 [14.734, 15.009] / 15.093 [15.083, 15.188] | Final comparison |
| `transform` (scalar) | 9.498 [9.433, 9.529] / 9.477 [9.457, 9.508] | 12.316 [11.439, 14.759] / 11.680 [10.985, 19.521] | Final comparison |
| `hll_sketch_agg` (aggregate) | 12.881 [12.846, 12.885] / 13.053 [13.013, 13.115] | 12.905 [12.823, 13.011] / 13.088 [13.052, 13.099] | Final comparison |
| `forall` (scalar) | 12.790 [12.730, 12.815] / 13.116 [13.069, 13.554] | 12.779 [12.756, 12.784] / 13.155 [13.107, 13.468] | Final comparison |
| `filter` (scalar) | 12.289 [9.619, 14.640] / 14.472 [11.693, 14.538] | 13.120 [11.810, 14.307] / 13.882 [13.309, 14.398] | Final comparison |
| `theta_sketch_agg` (aggregate) | 10.740 [10.731, 10.765] / 10.921 [10.877, 11.066] | 10.759 [10.742, 10.772] / 10.930 [10.929, 10.976] | Final comparison |
| `exists` (scalar) | 9.817 [9.658, 9.840] / 10.071 [10.023, 10.208] | 9.868 [9.745, 9.991] / 10.062 [10.060, 10.216] | Final comparison |
| `bitmap_count` (scalar) | 9.645 [9.618, 9.657] / 9.445 [9.426, 9.488] | 9.609 [9.596, 9.847] / 9.437 [9.215, 9.472] | Final comparison |
| `make_dt_interval` (scalar) | 8.960 [8.944, 8.990] / 8.975 [8.958, 9.012] | 8.943 [8.932, 9.013] / 8.979 [8.953, 8.999] | Final comparison |
| `xxhash64` (scalar) | 8.432 [8.406, 8.475] / 8.351 [8.311, 8.374] | 8.444 [8.410, 8.460] / 8.333 [8.332, 8.353] | Final comparison |
| `hash` (scalar) | 7.787 [7.772, 7.820] / 7.776 [7.774, 7.898] | 7.776 [7.771, 7.792] / 7.777 [7.771, 7.788] | Final comparison |
| `elt` (scalar) | 7.530 [7.477, 7.544] / 7.616 [7.593, 7.620] | 7.520 [7.486, 7.577] / 7.624 [7.548, 7.659] | Final comparison |
| `bround` (scalar) | 6.760 [6.737, 6.857] / 5.792 [5.791, 5.843] | 6.824 [6.774, 6.950] / 5.824 [5.811, 5.869] | [Retained](functions-1414-bround.md) |
| `time_trunc` (scalar) | 5.891 [5.862, 5.893] / 5.906 [5.905, 5.909] | 5.919 [5.880, 5.939] / 5.839 [5.829, 5.874] | Final comparison |
| `count_min_sketch` (aggregate) | 5.115 [5.098, 5.120] / 5.202 [5.194, 5.218] | 5.095 [5.094, 5.344] / 5.173 [5.158, 5.187] | Final comparison |
| `luhn_check` (scalar) | 4.982 [4.972, 5.092] / 4.967 [4.947, 4.968] | 4.989 [4.967, 4.990] / 4.970 [4.963, 4.976] | Final comparison |
| `kurtosis` (aggregate) | 4.118 [4.115, 4.126] / 4.128 [4.121, 4.137] | 4.127 [4.115, 4.142] / 4.122 [4.119, 4.122] | Final comparison |
| `time_diff` (scalar) | 3.924 [3.919, 4.060] / 3.726 [3.697, 3.741] | 3.902 [3.876, 3.906] / 3.709 [3.686, 3.723] | [Retained](functions-1414-time-diff.md) |
| `skewness` (aggregate) | 3.764 [3.761, 3.774] / 3.757 [3.756, 3.764] | 3.767 [3.767, 3.771] / 3.772 [3.760, 3.786] | Final comparison |
| `mode` (aggregate) | 3.160 [3.135, 3.182] / 3.413 [3.104, 3.718] | 3.150 [3.149, 3.454] / 3.431 [3.129, 3.688] | Final comparison |
| `percentile_disc` (aggregate) | 2.800 [2.781, 2.802] / 2.803 [2.793, 2.804] | 2.802 [2.794, 2.803] / 2.809 [2.808, 2.907] | Final comparison |
| `make_ym_interval` (scalar) | 2.336 [2.310, 2.398] / 1.911 [1.868, 1.916] | 2.312 [2.304, 2.313] / 1.904 [1.861, 1.922] | [Retained](functions-1414-make-ym-interval.md) |
| `bitmap_or_agg` (aggregate) | 1.996 [1.984, 2.003] / 1.993 [1.984, 2.023] | 1.988 [1.986, 2.001] / 2.002 [1.994, 2.004] | Final comparison |
| `try_aes_decrypt` (scalar) | 155.857 [154.212, 156.469] / 63.184 [62.848, 63.758] | 155.010 [153.021, 157.829] / 63.693 [63.079, 64.047] | [Retained](functions-1414-try-aes.md) |
| `bit_count` (scalar) | 1.787 [1.775, 1.809] / 1.761 [1.756, 1.774] | 1.789 [1.766, 1.797] / 1.763 [1.748, 1.770] | Final comparison |
| `try_aes_encrypt` (scalar) | 179.888 [178.118, 180.547] / 86.711 [84.301, 88.541] | 178.346 [177.180, 179.637] / 85.867 [85.739, 93.378] | [Retained](functions-1414-try-aes.md) |
| `bitmap_construct_agg` (aggregate) | 1.468 [1.451, 1.470] / 1.478 [1.470, 1.485] | 1.470 [1.465, 1.471] / 1.469 [1.467, 1.517] | Final comparison |
| `product` (aggregate) | 1.040 [1.033, 1.077] / 1.034 [1.032, 1.038] | 1.030 [1.025, 1.032] / 1.025 [1.020, 1.045] | Final comparison |
| `make_valid_utf8` (scalar) | 0.005 [0.005, 0.007] / 0.006 [0.006, 0.013] | 0.005 [0.005, 0.012] / 0.005 [0.004, 0.012] | Final comparison |
| `st_asbinary` (scalar) | -0.004 [-0.004, -0.003] / -0.002 [-0.002, -0.002] | -0.004 [-0.004, 0.002] / -0.002 [-0.003, 0.002] | Final comparison |
| `try_validate_utf8` (scalar) | -0.014 [-0.018, -0.007] / -0.015 [-0.016, -0.014] | -0.019 [-0.028, -0.012] / -0.012 [-0.013, -0.005] | Final comparison |
| `validate_utf8` (scalar) | -0.011 [-0.015, -0.006] / -0.016 [-0.017, -0.012] | -0.015 [-0.017, -0.015] / -0.007 [-0.015, -0.006] | Final comparison |
| `is_valid_utf8` (scalar) | -0.016 [-0.026, -0.011] / -0.020 [-0.025, -0.020] | -0.021 [-0.023, -0.021] / -0.023 [-0.029, -0.015] | Final comparison |

## Changes across the two staged builds

In the final 84-case session, 41 mode comparisons exceeded the paired
floor in the slower direction and 39 exceeded it in the faster direction.
The other 88 fall within the floor. The largest increases were
JSON_OBJECT_KEYS at 8.30 and 8.05 ns per input row, and TIMESTAMPDIFF at
5.93 and 5.80 ns per input row. The TIMESTAMPDIFF column/column statement
uses the calculation shared with the TIMEDIFF literal-end change.
This whole-build comparison records the shifts across all retained
changes. It cannot attribute them to an individual change.

## Batch-constant work still present

- FROM_XML expands literal schema and options. The singleton trial was
  slower in both ANSI modes. [Trial](functions-1414-from-xml.md)
- Numeric TO_CHAR and TO_VARCHAR expand a literal format. The guarded
  trial regressed with ANSI off for both names. [Trial](functions-1414-to-char.md)
- REGEXP_EXTRACT_ALL repeats capture-index checks. The trial that moved
  those checks out of the row loop was slower in both ANSI modes.
  [Trial](functions-1414-regexp-extract-all.md)
- TO_NUMBER, TRY_TO_NUMBER, FROM_CSV, FROM_JSON, PARSE_URL and
  TRY_PARSE_URL keep their original broadcast adapters. The four
  singleton-setting trials changed execution behavior when an expanded
  Utf8 literal exceeded the Arrow offset range. The exact original
  adapters were restored after the failure comparison. [Boundary evidence](functions-1414-string-offset-boundary.md)
- JSON_TUPLE expands literal field names inside the locked
  `datafusion-spark` 54.1.0 implementation. Its public UDF calls a private
  array kernel. Eliminating the broadcast would require taking ownership
  of that kernel or changing the dependency.
- WIDTH_BUCKET expands fixed bounds and bucket count, then checks the
  fixed values in the private row kernel. The locked `datafusion-spark`
  54.1.0 UDF exposes no scalar-aware equivalent. A change would also need
  a separate dependency decision.

The time differences for the slower trials are recorded in their
individual reports. The paired samples establish the changes in timing.
They do not identify the cause of those increases.

## Input-dependent costs

The source review also found rows whose work depends on their input.
Examples include building schemas from VARIANT values, decoding and
merging sketch pairs, and parsing JSON and XML documents. The table
records their paired costs. Possible changes would reuse decoded
values across rows or change the parser or output builder. Those paths
are outside the fixed-argument trials.
