# Floating-string CAST grammar

[Issue 238](https://github.com/mag1cfrog/delta-arrow-reader/issues/238) adds
Spark's explicit STRING-to-FLOAT/DOUBLE grammar to the selected Rust runtime.
`CAST('1.25f' AS DOUBLE)` returns 1.25 and `CAST('0x1.8p2' AS FLOAT)` returns
6.0. ANSI CAST raises invalid-input errors; legacy CAST and TRY_CAST return
NULL for invalid text. Query-local modes and Float32/Float64 result types
are preserved.

The [Sail patch](sail-floating-string-cast.patch) adds one native scalar
function and routes explicit floating string casts through it. It reuses
the existing `lexical-core` dependency, enabling its `format` and
`power-of-two` features. Whitespace and suffix removal borrow string slices;
the function parses each value directly into its target precision and
builds the output primitive array. No intermediate trimmed string array or
Float64-to-Float32 conversion is introduced.

Accepted grammar includes signed decimal/exponent text, Java f/F/d/D
suffixes, hexadecimal fractions with a required binary exponent, and
Spark's special NaN/Infinity forms. Trimming covers ASCII bytes 0-32;
DEL and Unicode spaces remain invalid. Special values are checked before
suffix removal, so `NaNf` and `Infinityd` stay invalid. Negative zero and
underflow/overflow boundaries retain their IEEE values.

The reference is Spark 4.2.0's
[Cast implementation](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala),
which uses Java Float/Double parsing and Spark's special-literal fallback.
Its captured source hash and the pinned JRE are retained with the evidence.

This layers on integration `bb1454385b2818ef3fb6d3dd6fde6fad44e109f2` and the
accepted [strict integer CAST repair](STRICT_INTEGER_CAST.md). Default vendor
sources, earlier corpora and implicit arithmetic conversions are unchanged.
Planning and execution remain Rust; Python captures independent Spark
references and verifies evidence.

## Parser dependency and error folding

Unpatched `lexical-parse-float` 1.0.6 does not correctly handle all of this
hexadecimal grammar. The first audit recorded 78 bit differences, including
`0x1.8p2` becoming 0.09375 rather than 6.0. Its fast path assumes equal
mantissa and exponent bases; hexadecimal floating literals use 16 and 2.

The [lexical patch](lexical-hex-float.patch) adopts the guards from the
upstream [PR 172](https://github.com/Alexhuszagh/rust-lexical/pull/172), captured
at head `62e40dc16560833f11e360ae6371875c513e2038` while still open. It also
adds two local corrections: zero remains zero with an extreme exponent,
and rounding handles exactly 64 discarded mantissa bits near half the
minimum subnormal. Applying only the upstream guards and zero correction
left 12 mismatching strings. These intermediate results remain archived;
the additional fixes are not attributed to the upstream PR.

The fork uses the unchanged 1.0.6 dependency version and MIT/Apache-2.0
licensing. A scratch Cargo override selects it only for this optional
runtime. Registry files are not edited. The archive records the original
crate file hashes, two-file patch, Cargo manifests and exact build sources.

The first integrated candidate also lost 10 previously matching historical
observations and 12 owned controls. DataFusion preserves failed native
literal CASTs during constant folding, but defers ordinary scalar-function
errors. Later NULL/empty-result rewrites could therefore discard the new
function's error. Its `simplify` hook now uses the same parser and invalid
input handling as execution and preserves the prior literal-error policy.
Broader unreachable-expression policy remains separately owned.

## Correctness evidence

All 34 assigned floating-grammar observations now match Spark. The new
[244-query corpus](floating-string-cast.jsonl) improves from 394/488 to
488/488 across both ANSI modes. It covers valid/invalid literals, ordered
columns, NULL/empty input, runtime expressions and batches of 1, 2 and 64.
The native Rust matrix adds Utf8, LargeUtf8 and Utf8View and switches ANSI
on/off/on within one session.

The prior CAST corpus improves from 528/562 to 562/562. The preceding
strict-integer corpus retains 496/496. The original owned/control set
improves from 89/126 to 90/126: `owned_52_true`, a dead constant CASE branch,
now returns Spark's 7. The other 36 owned/control differences remain open;
no previous agreement is lost.

The standalone parser matrix contains 2,082 distinct strings and 4,164 IEEE
observations. It combines the focused grammar, fixed-seed decimal and hex
inputs, malformed strings, very long mantissas and extreme exponents.
Spark receives strings through a typed DataFrame, independently of SQL
literal escaping. Both debug and release builds agree on all bits, including
direct Float32 rounding, signed zero and nonfinite values. Seed and every
input, reference and result are retained.

The 37 historical groups retain 6,508/6,784 agreements with no changed
value/type/status outcomes or lost earlier agreements. Integer-overflow
controls retain 616/616 and all 131 complete error payloads. All 12
dedicated comparators return the same case results as the accepted baseline:
arithmetic 575/578, string division 472/480, BIGINT DIV 232/234, modulo
427/442, extreme Decimal ROUND 1,194/1,322, Float ROUND 398/398 targets and
16/18 controls, ROUND arguments 388/388, integer ROUND 1,254/1,254, legacy
Decimal ROUND 234/234, Float BROUND boundaries 246/246 plus 8/8 controls,
Float BROUND 80/80 plus 8/8 controls and 10/10 boundaries, and Decimal
BROUND 506/506. Their remaining mismatches are retained.

Rust tests pass: 314 function, 47 planner and 28 runner tests. The comparator
self-check passes. The real-Delta capture retains all 116 earlier outcomes
and passes 18 adapter checks. Its frozen Spark comparison remains 47
matches, 58 differences and 11 pending adapters; preservation of the earlier
runtime is separate from full Delta/Spark acceptance.

Nullable schema differences:

| Corpus | Logical before / after | Physical before / after |
| --- | --- | --- |
| focused | 8 / 16 | 50 / 66 |
| strict | 0 / 0 | 12 / 12 |
| legacy | 40 / 54 | 188 / 210 |
| owned | 0 / 0 | 3 / 4 |

Harness phase-label differences change from 22 to 0 in the new corpus,
remain 0 in strict integers, change from 16 to 0 in prior CASTs and from
37 to 36 in the owned/control set. A harness `collect()` label does not
identify Spark's internal optimization phase.

Retained error texts change in 88 new-corpus observations, 16 prior CASTs
and 41 owned controls; strict-integer errors are unchanged. Historical
replay retains all 908 error outcomes: 223 complete payloads change,
including 24 text changes. Of those 24, 22 replace the old Arrow CAST
diagnostic with the new CAST_INVALID_INPUT diagnostic, and two report a
different first invalid row in existing multi-error Decimal queries. The
archive keeps the exact IDs, before/after text and plans. These changes
are not normalized away. Schema and precise error acceptance remain with
[issue 149](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).

The first focused capture exposed an independent SQL-literal discrepancy:
Sail parses `'1'''` as `1` and `'a''b'` as `ab`, while Spark preserves the
embedded quote. The floating corpus now uses backslash escaping to deliver
the intended quote byte to both numeric parsers. All 488 Spark outcomes
remain identical after this encoding change. The original corpus, 14
affected floating observations and reduced raw-string captures remain in
the archive. The four-query lexical matrix retains six mismatches and two
matching backslash controls across both modes. This is owned by the existing
[lexical acceptance leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/137),
and is not counted as fixed by the floating parser.

Agreement here compares represented values, complete numeric types, NULLs
and classified error causes. Schema nullability, exact error payloads and
harness phase labels are retained separately. This finite corpus does not
establish complete Spark SQL compatibility.

## Bounded cost check

The fixed eight-process schedule is before, after, after, before, after,
before, before, after. Each process uses 1,048,576 rows, batches of 8,192,
one partition, CPU 2, eight execution warmups and 41 samples per phase.
Planning covers parsing through physical-plan creation. Execution consumes
an existing physical plan; input creation and row-by-row verification are
outside the timed region. Tables report the median of four process medians.

There are 50 configurations: FLOAT/DOUBLE in ANSI, legacy and TRY modes,
suffix/hexadecimal/padded/invalid/long inputs, native floating controls,
and unchanged integer, Decimal, arithmetic and NULL-DIV controls. Each runs
with and without NULLs. Every timed output is verified against independent
Spark vectors, including exact floating bits and NULLs.

Input pattern `i = row % 10000` gives `a = i % 199 - 99` and `f = str(a) +
'.25'`. Suffix input adds F; padded input adds SPACE/TAB on the left and LF
on the right; hexadecimal input is the signed absolute integer in hex plus
`.8p0`; long input appends 30 zeroes and 1 to `f`. Invalid input replaces
every pattern with `i % 17 == 16` by `bad`. The NULL configuration masks all
columns when `i % 10 == 9`. The benchmark source records the complete schema
and unused inherited fixture fields as well as all SQL and physical plans.

The 38 comparable pairs have identical output types and digests. Twelve
configurations have candidate-only timings: four baseline padded ANSI cases
raise errors and eight suffix/hex cases return incorrect NULLs. No ratio
compares these incorrect old outcomes against successful new work.

Execution time:

| Configuration | Before (ms) | After (ms) | After / before |
| --- | --- | --- | --- |
| ansi_float_nullsfalse | 8.875690 | 16.893802 | 1.9034x (+90.34%) |
| legacy_float_nullsfalse | 19.555048 | 16.906841 | 0.8646x (-13.54%) |
| try_float_nullsfalse | 7.711773 | 16.862192 | 2.1866x (+118.66%) |
| suffix_float_nullsfalse | incorrect NULLs | 16.865313 | n/a |
| hex_float_nullsfalse | incorrect NULLs | 20.582231 | n/a |
| space_ansi_float_nullsfalse | error | 17.991876 | n/a |
| invalid_legacy_float_nullsfalse | 19.359305 | 18.118856 | 0.9359x (-6.41%) |
| invalid_try_float_nullsfalse | 7.500526 | 18.196652 | 2.4261x (+142.61%) |
| long_float_nullsfalse | 28.679280 | 36.338876 | 1.2671x (+26.71%) |
| ansi_double_nullsfalse | 8.773986 | 16.772876 | 1.9117x (+91.17%) |
| legacy_double_nullsfalse | 19.352283 | 16.762953 | 0.8662x (-13.38%) |
| try_double_nullsfalse | 7.709860 | 16.796655 | 2.1786x (+117.86%) |
| suffix_double_nullsfalse | incorrect NULLs | 16.837392 | n/a |
| hex_double_nullsfalse | incorrect NULLs | 20.409140 | n/a |
| space_ansi_double_nullsfalse | error | 17.895927 | n/a |
| invalid_legacy_double_nullsfalse | 19.276352 | 18.123660 | 0.9402x (-5.98%) |
| invalid_try_double_nullsfalse | 7.497885 | 17.897210 | 2.3870x (+138.70%) |
| long_double_nullsfalse | 28.993900 | 36.081584 | 1.2445x (+24.45%) |
| native_float_nullsfalse | 8.878324 | 9.664424 | 1.0885x (+8.85%) |
| native_double_nullsfalse | 8.769963 | 9.892668 | 1.1280x (+12.80%) |
| legacy_long_nullsfalse | 6.037142 | 6.288484 | 1.0416x (+4.16%) |
| ansi_long_nullsfalse | 16.783175 | 16.867017 | 1.0050x (+0.50%) |
| decimal_nullsfalse | 72.962696 | 72.752316 | 0.9971x (-0.29%) |
| numeric_nullsfalse | 0.450236 | 0.615382 | 1.3668x (+36.68%) |
| null_div_nullsfalse | 0.144143 | 0.150585 | 1.0447x (+4.47%) |
| ansi_float_nullstrue | 8.376357 | 17.031166 | 2.0332x (+103.32%) |
| legacy_float_nullstrue | 18.840927 | 17.028797 | 0.9038x (-9.62%) |
| try_float_nullstrue | 7.278042 | 17.024905 | 2.3392x (+133.92%) |
| suffix_float_nullstrue | incorrect NULLs | 17.010748 | n/a |
| hex_float_nullstrue | incorrect NULLs | 20.579276 | n/a |
| space_ansi_float_nullstrue | error | 18.180196 | n/a |
| invalid_legacy_float_nullstrue | 18.889549 | 17.198292 | 0.9105x (-8.95%) |
| invalid_try_float_nullstrue | 7.127899 | 17.171467 | 2.4091x (+140.91%) |
| long_float_nullstrue | 26.278140 | 34.432074 | 1.3103x (+31.03%) |
| ansi_double_nullstrue | 8.333222 | 16.911714 | 2.0294x (+102.94%) |
| legacy_double_nullstrue | 18.798229 | 16.886854 | 0.8983x (-10.17%) |
| try_double_nullstrue | 7.237832 | 16.886588 | 2.3331x (+133.31%) |
| suffix_double_nullstrue | incorrect NULLs | 16.927298 | n/a |
| hex_double_nullstrue | incorrect NULLs | 20.255581 | n/a |
| space_ansi_double_nullstrue | error | 17.944784 | n/a |
| invalid_legacy_double_nullstrue | 18.827638 | 17.015788 | 0.9038x (-9.62%) |
| invalid_try_double_nullstrue | 7.131590 | 17.003285 | 2.3842x (+138.42%) |
| long_double_nullstrue | 26.621835 | 34.507750 | 1.2962x (+29.62%) |
| native_float_nullstrue | 8.398814 | 9.202621 | 1.0957x (+9.57%) |
| native_double_nullstrue | 8.353260 | 9.365278 | 1.1212x (+12.12%) |
| legacy_long_nullstrue | 7.852585 | 7.686415 | 0.9788x (-2.12%) |
| ansi_long_nullstrue | 16.806784 | 16.608677 | 0.9882x (-1.18%) |
| decimal_nullstrue | 67.100364 | 66.502653 | 0.9911x (-0.89%) |
| numeric_nullstrue | 0.799651 | 0.803868 | 1.0053x (+0.53%) |
| null_div_nullstrue | 0.147985 | 0.144198 | 0.9744x (-2.56%) |

Planning time:

| Configuration | Before (ms) | After (ms) | After / before |
| --- | --- | --- | --- |
| ansi_float_nullsfalse | 0.309179 | 0.321226 | 1.0390x (+3.90%) |
| legacy_float_nullsfalse | 0.359077 | 0.321688 | 0.8959x (-10.41%) |
| try_float_nullsfalse | 0.309260 | 0.321137 | 1.0384x (+3.84%) |
| suffix_float_nullsfalse | incorrect NULLs | 0.320069 | n/a |
| hex_float_nullsfalse | incorrect NULLs | 0.320545 | n/a |
| space_ansi_float_nullsfalse | error | 0.322128 | n/a |
| invalid_legacy_float_nullsfalse | 0.358121 | 0.322032 | 0.8992x (-10.08%) |
| invalid_try_float_nullsfalse | 0.310447 | 0.322929 | 1.0402x (+4.02%) |
| long_float_nullsfalse | 0.307716 | 0.318101 | 1.0337x (+3.37%) |
| ansi_double_nullsfalse | 0.311428 | 0.321647 | 1.0328x (+3.28%) |
| legacy_double_nullsfalse | 0.359422 | 0.320284 | 0.8911x (-10.89%) |
| try_double_nullsfalse | 0.308322 | 0.320476 | 1.0394x (+3.94%) |
| suffix_double_nullsfalse | incorrect NULLs | 0.322443 | n/a |
| hex_double_nullsfalse | incorrect NULLs | 0.322183 | n/a |
| space_ansi_double_nullsfalse | error | 0.322253 | n/a |
| invalid_legacy_double_nullsfalse | 0.358807 | 0.320716 | 0.8938x (-10.62%) |
| invalid_try_double_nullsfalse | 0.308878 | 0.322318 | 1.0435x (+4.35%) |
| long_double_nullsfalse | 0.306529 | 0.318025 | 1.0375x (+3.75%) |
| native_float_nullsfalse | 0.172897 | 0.170642 | 0.9870x (-1.30%) |
| native_double_nullsfalse | 0.172696 | 0.170372 | 0.9865x (-1.35%) |
| legacy_long_nullsfalse | 0.318160 | 0.318030 | 0.9996x (-0.04%) |
| ansi_long_nullsfalse | 0.356232 | 0.356387 | 1.0004x (+0.04%) |
| decimal_nullsfalse | 0.325875 | 0.324693 | 0.9964x (-0.36%) |
| numeric_nullsfalse | 0.362564 | 0.361416 | 0.9968x (-0.32%) |
| null_div_nullsfalse | 0.370007 | 0.370634 | 1.0017x (+0.17%) |
| ansi_float_nullstrue | 0.294497 | 0.304250 | 1.0331x (+3.31%) |
| legacy_float_nullstrue | 0.345407 | 0.305547 | 0.8846x (-11.54%) |
| try_float_nullstrue | 0.294397 | 0.308578 | 1.0482x (+4.82%) |
| suffix_float_nullstrue | incorrect NULLs | 0.305728 | n/a |
| hex_float_nullstrue | incorrect NULLs | 0.308713 | n/a |
| space_ansi_float_nullstrue | error | 0.306524 | n/a |
| invalid_legacy_float_nullstrue | 0.341219 | 0.319037 | 0.9350x (-6.50%) |
| invalid_try_float_nullstrue | 0.292879 | 0.325565 | 1.1116x (+11.16%) |
| long_float_nullstrue | 0.294297 | 0.323842 | 1.1004x (+10.04%) |
| ansi_double_nullstrue | 0.308904 | 0.324348 | 1.0500x (+5.00%) |
| legacy_double_nullstrue | 0.362548 | 0.326993 | 0.9019x (-9.81%) |
| try_double_nullstrue | 0.312144 | 0.325144 | 1.0416x (+4.16%) |
| suffix_double_nullstrue | incorrect NULLs | 0.324924 | n/a |
| hex_double_nullstrue | incorrect NULLs | 0.325219 | n/a |
| space_ansi_double_nullstrue | error | 0.324012 | n/a |
| invalid_legacy_double_nullstrue | 0.362148 | 0.322344 | 0.8901x (-10.99%) |
| invalid_try_double_nullstrue | 0.310056 | 0.323356 | 1.0429x (+4.29%) |
| long_double_nullstrue | 0.309475 | 0.322474 | 1.0420x (+4.20%) |
| native_float_nullstrue | 0.175020 | 0.172330 | 0.9846x (-1.54%) |
| native_double_nullstrue | 0.174199 | 0.173086 | 0.9936x (-0.64%) |
| legacy_long_nullstrue | 0.319363 | 0.319763 | 1.0013x (+0.13%) |
| ansi_long_nullstrue | 0.357374 | 0.355039 | 0.9935x (-0.65%) |
| decimal_nullstrue | 0.326702 | 0.326993 | 1.0009x (+0.09%) |
| numeric_nullstrue | 0.364216 | 0.361511 | 0.9926x (-0.74%) |
| null_div_nullstrue | 0.367262 | 0.366841 | 0.9989x (-0.11%) |

Canonical ANSI/TRY FLOAT/DOUBLE conversion takes 1.9034-2.3392 times the
previous execution time. ANSI FLOAT without NULLs rises from 8.875690 to
16.893802 ms; TRY FLOAT with NULLs rises from 7.278042 to 17.024905 ms.
Invalid-string TRY configurations take 2.3842-2.4261 times the previous time.
The long-decimal configurations take 1.2445-1.3103 times the previous time.
These are measured compatibility costs, not accepted performance results.

Legacy canonical conversions improve by 9.62%-13.54%, and legacy conversions
with invalid strings improve by 5.98%-9.62%. Those old plans performed BTRIM
before conversion; the new parser consumes borrowed slices in one pass.
The plan/source comparison explains the removed intermediate operation,
but this run does not isolate allocation cost from parsing and dispatch.
Newly supported suffix, hex and padded ANSI cases take about 16.8-20.6 ms
per 1,048,576 rows; their incorrect old outcomes have no throughput ratio.

The native DataFusion floating controls also slow by 8.85%-12.80%.
For example, native DOUBLE without NULLs rises from 8.769963 to 9.892668 ms.
All four process medians are separated: 8.705649-8.826313 ms before and
9.815941-9.922048 ms after. These controls bypass the new Sail function;
the shared lexical features/source and resulting binary changes need
separate attribution before claiming that costs affect only the new CAST.

The unchanged numeric `a + 1` control without NULLs rises from 0.450236 to
0.615382 ms, +36.68% or +0.165146 ms. Its four process medians are also
separated: 0.448503-0.455566 ms before and 0.611466-0.620112 ms after.
The NULL variant changes by +0.53%. This persistent configuration-specific
flag is retained, not dismissed as random timing noise. Legacy BIGINT
without NULLs rises from 6.037142 to 6.288484 ms (+4.16%); NULL-DIV without
NULLs rises from 0.144143 to 0.150585 ms (+4.47%). Their NULL variants
improve by 2.12% and 2.56%. ANSI BIGINT and Decimal execution ratios span
0.9882-1.0050. These controls do not establish a universal runtime slowdown
or prove that unrelated paths have no regression.

Canonical ANSI/TRY planning rises by 3.28%-5.00%, while legacy canonical
planning improves by 9.81%-11.54%. The largest planning flags are invalid
TRY FLOAT with NULLs, 0.292879 to 0.325565 ms (+11.16%), and long FLOAT with
NULLs, 0.294297 to 0.323842 ms (+10.04%). All process medians and samples
remain visible, including the smaller controls.

Follow-up attribution belongs to the existing CAST performance leaf:
separate the parser features/backport from the new conversion function,
calibrate the native floating and unchanged numeric controls, then evaluate
ways to reduce ordinary decimal-path work while preserving the full grammar
and IEEE matrix. Earlier strict-integer trimming costs remain open.

Host: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics; `Linux-6.19.14-200.fc43.x86_64-x86_64-with-glibc2.42`; `rustc 1.98.1 (48a229cea 2026-09-01)`. The guard recorded 0 waits for compiler/JVM/benchmark processes between runs. The host was not isolated or reserved, and the guard cannot exclude
unrelated activity starting during a run. No timing process was discarded
or repeated. Same-binary calibration, instruction counts, allocations and
memory were not measured; small differences remain unattributed.

All samples and process medians remain available. No follow-up optimization
is selected here. Outstanding CAST costs, including earlier integer-trim
costs, remain with [issue 159](https://github.com/mag1cfrog/delta-arrow-reader/issues/159).

## Build and reproduction

There are 95 selected source paths: the accepted 92 plus the new floating
function and two lexical sources. Seven files differ from the extended
baseline, including the dependency manifest and lockfile. Both variants
have 414 named libraries and 505 exact link artifacts, verified and copied
to separate directories before linking. 54 named library hashes change
because of the lexical feature/source changes and downstream rebuilding.
Versions are unchanged. This is not a sail_plan-only binary comparison.
Shared source files, executable slots and Delta inputs were restored.

| Executable | SHA-256 |
| --- | --- |
| before probe | `607ef3f7636b391615d743166ddf588a1b9a0b553a7c141dadd13b2ae532261d` |
| before runner | `a8d9ce69bf41804c8263f858919ee71beebc6aee2fd6fc5769874925f43ad376` |
| after probe | `cf9b986c3c154324807ac9ea5d337a08005cc7199f605f87e57ecbaa0b23092f` |
| after runner | `fb7cfb17a0b9323ed7dd7cbb0355e8635df1499118f68be0e464f1621f920f5a` |

| Benchmark | SHA-256 |
| --- | --- |
| before | `0a7cf30ed5112bd2b0a70061e29fc865ccdf240f97309b53d332fe34ad3031d1` |
| after | `db2fc7fb68c2aeeb3bc7786b785216bd3fa3e90920f2339166a0be71c345fec6` |

The [result record](floating-string-cast-results.json) and
[raw archive](floating-string-cast-runs.json.gz) contain source snapshots,
patches, rejected candidates, compiler/test logs, Spark/native captures,
the fixed benchmark and every timing sample. Frozen previous references
are supplied by their hashed accepted archives.

From the repository root, verify without Rust or Spark:

```sh
python - <<'PY'
import gzip, hashlib, json
from pathlib import Path
root = Path('experiments/spark-sql')
record = json.loads((root / 'floating-string-cast-results.json').read_text())
packed = (root / record['archive']['path']).read_bytes()
assert hashlib.sha256(packed).hexdigest() == record['archive']['sha256']
archive = json.loads(gzip.decompress(packed))
exec(compile(archive['files']['check-archive.py'], 'check-archive.py', 'exec'))
PY
python -m unittest discover -s experiments/spark-sql -p test_division_cast_classification.py
```

To repeat the focused capture, use the pinned Spark environment with
`floating_string_cast.py spark OUT`, the selected probe with
`floating-string-cast.jsonl OUT --physical-plans`, then
`floating_string_cast.py compare SPARK NATIVE REPORT`.

Archived `prepare.py`, `build.py`, `override.toml`, `link-bench.py`,
`bench-reference.py` and `measure.py` record the selected runtime and commands.
The lexical source copy starts from registry `lexical-parse-float` 1.0.6;
`build.py` installs the two candidate sources into that copy. Parser-only
debug/release checks use the archived `parser-probe` Cargo project and
`check-parser.py`. Paths describe the measured environment. Portable
clean-checkout and CI adoption remain with the
[runtime-build leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/165).
